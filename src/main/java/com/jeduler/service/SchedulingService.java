package com.jeduler.service;

import com.jeduler.config.SchedulerProperties;
import com.jeduler.model.entity.Job;
import com.jeduler.model.entity.JobConfig;
import com.jeduler.model.enums.JobStatus;
import com.jeduler.repository.JobRepository;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

@Service
public class SchedulingService {

    private static final Logger log = LoggerFactory.getLogger(SchedulingService.class);

    private final JobRepository jobRepository;
    private final JobConfigService jobConfigService;
    private final ConcurrencyService concurrencyService;
    private final KafkaPublisherService kafkaPublisherService;
    private final MetricsService metricsService;
    private final SchedulerProperties properties;
    private final Executor schedulerExecutor;

    // Deduplication: tracks which jobNames have pending scheduling
    private final Set<String> pendingSet = ConcurrentHashMap.newKeySet();
    private final Map<String, Instant> lastAttempt = new ConcurrentHashMap<>();

    // Pause state
    private final Set<Integer> pausedTenants = ConcurrentHashMap.newKeySet();
    private final Set<String> pausedJobNames = ConcurrentHashMap.newKeySet();

    public SchedulingService(
            JobRepository jobRepository,
            JobConfigService jobConfigService,
            ConcurrencyService concurrencyService,
            KafkaPublisherService kafkaPublisherService,
            MetricsService metricsService,
            SchedulerProperties properties,
            @Qualifier("schedulerExecutor") Executor schedulerExecutor) {
        this.jobRepository = jobRepository;
        this.jobConfigService = jobConfigService;
        this.concurrencyService = concurrencyService;
        this.kafkaPublisherService = kafkaPublisherService;
        this.metricsService = metricsService;
        this.properties = properties;
        this.schedulerExecutor = schedulerExecutor;
    }

    /**
     * Trigger scheduling for a specific job name (with deduplication).
     */
    public boolean triggerScheduling(String jobName) {
        if (!properties.isEnabled()) return false;
        if (pausedJobNames.contains(jobName)) return false;

        if (pendingSet.add(jobName)) {
            schedulerExecutor.execute(() -> {
                try {
                    executeScheduling(jobName);
                } catch (Exception e) {
                    log.error("Scheduling failed for jobName={}", jobName, e);
                } finally {
                    lastAttempt.put(jobName, Instant.now());
                    pendingSet.remove(jobName);
                }
            });
            return true;
        }
        return false; // Already pending
    }

    /**
     * Execute the concurrency maximization algorithm for a job type.
     */
    @Transactional
    public void executeScheduling(String jobName) {
        Timer.Sample timer = metricsService.startSchedulingTimer();

        Optional<JobConfig> configOpt = jobConfigService.getConfig(jobName);
        if (configOpt.isEmpty()) {
            log.warn("No configuration found for jobName={}", jobName);
            return;
        }

        JobConfig config = configOpt.get();
        Map<String, Integer> rules = config.getConcurrencyRule();
        Integer globalLimit = rules.getOrDefault("_global", Integer.MAX_VALUE);

        // Check global capacity
        long globalCount = concurrencyService.getGlobalCount(jobName);
        int availableSlots = (int) (globalLimit - globalCount);

        if (availableSlots <= 0) {
            log.debug("No available slots for jobName={} (global: {}/{})", jobName, globalCount, globalLimit);
            metricsService.stopSchedulingTimer(timer, jobName);
            return;
        }

        // Iterative greedy fill algorithm
        List<Job> scheduledJobs = fillVacantSpots(jobName, config, availableSlots);

        if (!scheduledJobs.isEmpty()) {
            publishScheduledJobs(scheduledJobs, config);
            log.info("Scheduled {} jobs for jobName={}", scheduledJobs.size(), jobName);
        }

        metricsService.stopSchedulingTimer(timer, jobName);
    }

    /**
     * The concurrency maximization algorithm - iterative greedy fill.
     */
    private List<Job> fillVacantSpots(String jobName, JobConfig config, int availableSlots) {
        Map<String, Integer> rules = config.getConcurrencyRule();
        List<Job> scheduled = new ArrayList<>();
        Map<String, Long> sessionCounts = new HashMap<>();
        Set<String> exhaustedCombinations = new HashSet<>();
        int remainingSlots = availableSlots;

        int maxIterations = 10; // Safety cap
        for (int iteration = 0; iteration < maxIterations && remainingSlots > 0; iteration++) {
            int exhaustedBefore = exhaustedCombinations.size();

            // Query candidates from DB
            List<Job> candidates;
            if (exhaustedCombinations.isEmpty()) {
                candidates = jobRepository.findSchedulableCandidates(jobName, remainingSlots * 3);
            } else {
                // Build exclusion patterns for JSONB
                List<String> excludePatterns = new ArrayList<>(exhaustedCombinations);
                candidates = jobRepository.findSchedulableCandidatesExcluding(jobName, excludePatterns, remainingSlots * 3);
            }

            if (candidates.isEmpty()) break;

            for (Job candidate : candidates) {
                if (remainingSlots <= 0) break;

                // Skip paused tenants
                if (candidate.getTenant() != null && pausedTenants.contains(candidate.getTenant())) {
                    continue;
                }

                // Check all concurrency rules
                String blockedRule = concurrencyService.checkConcurrencyRules(candidate, config, sessionCounts);

                if (blockedRule == null) {
                    // All rules pass - schedule this job
                    scheduled.add(candidate);
                    remainingSlots--;

                    // Update session counters
                    String globalSessionKey = jobName + ":_global";
                    sessionCounts.merge(globalSessionKey, 1L, Long::sum);

                    Map<String, String> control = candidate.getConcurrencyControl();
                    if (control != null) {
                        for (String ruleKey : rules.keySet()) {
                            if ("_global".equals(ruleKey)) continue;
                            String value = control.get(ruleKey);
                            if (value != null) {
                                String sessionKey = jobName + ":" + ruleKey + ":" + value;
                                sessionCounts.merge(sessionKey, 1L, Long::sum);
                            }
                        }
                    }
                } else {
                    // Mark the blocking combination as exhausted
                    Map<String, String> control = candidate.getConcurrencyControl();
                    if (control != null && !"_global".equals(blockedRule)) {
                        String value = control.get(blockedRule);
                        if (value != null) {
                            // Build JSONB pattern for exclusion
                            String pattern = "{\"" + blockedRule + "\":\"" + value + "\"}";
                            exhaustedCombinations.add(pattern);
                        }
                    } else if ("_global".equals(blockedRule)) {
                        // Global is exhausted - no point continuing
                        break;
                    }
                }
            }

            // Termination: no new exhausted combinations found (no progress)
            if (exhaustedCombinations.size() == exhaustedBefore) break;
        }

        return scheduled;
    }

    /**
     * Publish scheduled jobs: update DB status, increment Redis counters, send to Kafka.
     */
    private void publishScheduledJobs(List<Job> jobs, JobConfig config) {
        Map<String, Integer> rules = config.getConcurrencyRule();

        for (Job job : jobs) {
            // Update status in DB
            JobStatus previousStatus = job.getStatus();
            job.setStatus(JobStatus.PUBLISHED);
            jobRepository.save(job);

            // Increment Redis concurrency counters
            concurrencyService.incrementCounters(job, rules);

            // Record metrics
            metricsService.incrementStateTransition(previousStatus.name(), "PUBLISHED", job.getJobName());

            // Publish to Kafka
            Timer.Sample kafkaTimer = metricsService.startKafkaPublishTimer();
            kafkaPublisherService.publishJob(job, config.getDestination(), config.getMaxRetries())
                .whenComplete((result, ex) -> {
                    metricsService.stopKafkaPublishTimer(kafkaTimer);
                    if (ex != null) {
                        log.error("Failed to publish job {} to Kafka", job.getId(), ex);
                    }
                });
        }
    }

    // Pause/Resume methods
    public void pauseTenant(int tenantId) { pausedTenants.add(tenantId); }
    public void resumeTenant(int tenantId) { pausedTenants.remove(tenantId); }
    public void pauseJobName(String jobName) { pausedJobNames.add(jobName); }
    public void resumeJobName(String jobName) { pausedJobNames.remove(jobName); }
    public Set<Integer> getPausedTenants() { return Set.copyOf(pausedTenants); }
    public Set<String> getPausedJobNames() { return Set.copyOf(pausedJobNames); }
    public Map<String, Instant> getLastAttempts() { return Map.copyOf(lastAttempt); }
}
