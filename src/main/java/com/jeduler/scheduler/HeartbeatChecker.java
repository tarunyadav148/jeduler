package com.jeduler.scheduler;

import com.jeduler.config.SchedulerProperties;
import com.jeduler.model.entity.Job;
import com.jeduler.model.entity.JobConfig;
import com.jeduler.model.enums.JobStatus;
import com.jeduler.repository.JobRepository;
import com.jeduler.service.ConcurrencyService;
import com.jeduler.service.JobConfigService;
import com.jeduler.service.MetricsService;
import com.jeduler.service.SchedulingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class HeartbeatChecker {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatChecker.class);

    private final JobRepository jobRepository;
    private final ConcurrencyService concurrencyService;
    private final JobConfigService jobConfigService;
    private final SchedulingService schedulingService;
    private final MetricsService metricsService;
    private final SchedulerProperties properties;

    public HeartbeatChecker(
            JobRepository jobRepository,
            ConcurrencyService concurrencyService,
            JobConfigService jobConfigService,
            SchedulingService schedulingService,
            MetricsService metricsService,
            SchedulerProperties properties) {
        this.jobRepository = jobRepository;
        this.concurrencyService = concurrencyService;
        this.jobConfigService = jobConfigService;
        this.schedulingService = schedulingService;
        this.metricsService = metricsService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${job.scheduler.heartbeat.check-interval-ms:30000}")
    @Transactional
    public void checkHeartbeats() {
        if (!properties.isEnabled()) return;

        List<Job> processingJobs = jobRepository.findByStatus(JobStatus.PROCESSING);
        int failedCount = 0;

        for (Job job : processingJobs) {
            if (!concurrencyService.hasHeartbeat(job.getId())) {
                // Re-read to avoid stale data
                Job freshJob = jobRepository.findById(job.getId()).orElse(null);
                if (freshJob == null || freshJob.getStatus() != JobStatus.PROCESSING) continue;

                log.warn("Heartbeat timeout for job {} (jobName={})", freshJob.getId(), freshJob.getJobName());

                // Mark as failed by scheduler
                freshJob.setStatus(JobStatus.FAILED_BY_SCHEDULER);
                freshJob.setLastFailureReason("Heartbeat timeout - no heartbeat received for >" + properties.getHeartbeat().getTimeoutSeconds() + "s");
                freshJob.setExecutionCount(freshJob.getExecutionCount() + 1);

                // Check if eligible for retry
                var config = jobConfigService.getConfig(freshJob.getJobName());
                if (config.isPresent() && freshJob.getExecutionCount() < config.get().getMaxRetries()) {
                    freshJob.setStatus(JobStatus.RETRY);
                }

                jobRepository.save(freshJob);

                // Decrement concurrency counters
                config.ifPresent(c -> concurrencyService.decrementCounters(freshJob, c.getConcurrencyRule()));

                // Delete heartbeat key
                concurrencyService.deleteHeartbeat(freshJob.getId());

                // Trigger re-scheduling
                schedulingService.triggerScheduling(freshJob.getJobName());

                metricsService.incrementHeartbeatFailures(freshJob.getJobName());
                failedCount++;
            }
        }

        if (failedCount > 0) {
            log.info("Heartbeat check complete: {} jobs timed out", failedCount);
        }
    }
}
