package com.jeduler.service;

import com.jeduler.model.entity.Job;
import com.jeduler.model.entity.JobConfig;
import com.jeduler.repository.JobRepository;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConcurrencyService {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyService.class);
    private static final String CONCURRENCY_PREFIX = "concurrency:";
    private static final String HEARTBEAT_PREFIX = "heartbeat:job:";
    private static final String GLOBAL_KEY = "_global";

    private final RedissonClient redissonClient;
    private final JobRepository jobRepository;

    public ConcurrencyService(RedissonClient redissonClient, JobRepository jobRepository) {
        this.redissonClient = redissonClient;
        this.jobRepository = jobRepository;
    }

    /**
     * Get the current global concurrency count for a job type.
     */
    public long getGlobalCount(String jobName) {
        RAtomicLong counter = redissonClient.getAtomicLong(buildKey(jobName, GLOBAL_KEY, null));
        return counter.get();
    }

    /**
     * Get the current count for a specific rule key/value pair.
     */
    public long getRuleCount(String jobName, String ruleKey, String ruleValue) {
        RAtomicLong counter = redissonClient.getAtomicLong(buildKey(jobName, ruleKey, ruleValue));
        return counter.get();
    }

    /**
     * Check if a job can be scheduled given current concurrency state and session counts.
     * Returns the blocking rule key or null if all rules pass.
     */
    public String checkConcurrencyRules(Job job, JobConfig config, Map<String, Long> sessionCounts) {
        Map<String, Integer> rules = config.getConcurrencyRule();
        Map<String, String> jobControl = job.getConcurrencyControl();

        for (Map.Entry<String, Integer> rule : rules.entrySet()) {
            String ruleKey = rule.getKey();
            int limit = rule.getValue();

            String redisKey;
            String sessionKey;
            if (GLOBAL_KEY.equals(ruleKey)) {
                redisKey = buildKey(job.getJobName(), GLOBAL_KEY, null);
                sessionKey = job.getJobName() + ":" + GLOBAL_KEY;
            } else {
                String value = jobControl != null ? jobControl.get(ruleKey) : null;
                if (value == null) continue; // Rule doesn't apply to this job
                redisKey = buildKey(job.getJobName(), ruleKey, value);
                sessionKey = job.getJobName() + ":" + ruleKey + ":" + value;
            }

            long currentCount = redissonClient.getAtomicLong(redisKey).get();
            long sessionCount = sessionCounts.getOrDefault(sessionKey, 0L);

            if (currentCount + sessionCount >= limit) {
                return ruleKey; // This rule blocks the job
            }
        }
        return null; // All rules pass
    }

    /**
     * Increment all concurrency counters for a job being scheduled.
     */
    public void incrementCounters(Job job, Map<String, Integer> concurrencyRule) {
        String jobName = job.getJobName();
        Map<String, String> control = job.getConcurrencyControl();

        // Always increment global
        if (concurrencyRule.containsKey(GLOBAL_KEY)) {
            redissonClient.getAtomicLong(buildKey(jobName, GLOBAL_KEY, null)).incrementAndGet();
        }

        // Increment per-rule counters
        if (control != null) {
            for (Map.Entry<String, Integer> rule : concurrencyRule.entrySet()) {
                if (GLOBAL_KEY.equals(rule.getKey())) continue;
                String value = control.get(rule.getKey());
                if (value != null) {
                    redissonClient.getAtomicLong(buildKey(jobName, rule.getKey(), value)).incrementAndGet();
                }
            }
        }
    }

    /**
     * Decrement all concurrency counters for a job that completed/failed/cancelled.
     */
    public void decrementCounters(Job job, Map<String, Integer> concurrencyRule) {
        String jobName = job.getJobName();
        Map<String, String> control = job.getConcurrencyControl();

        // Always decrement global (floor at 0)
        if (concurrencyRule.containsKey(GLOBAL_KEY)) {
            RAtomicLong counter = redissonClient.getAtomicLong(buildKey(jobName, GLOBAL_KEY, null));
            long val = counter.decrementAndGet();
            if (val < 0) counter.set(0);
        }

        // Decrement per-rule counters (floor at 0)
        if (control != null) {
            for (Map.Entry<String, Integer> rule : concurrencyRule.entrySet()) {
                if (GLOBAL_KEY.equals(rule.getKey())) continue;
                String value = control.get(rule.getKey());
                if (value != null) {
                    RAtomicLong counter = redissonClient.getAtomicLong(buildKey(jobName, rule.getKey(), value));
                    long val = counter.decrementAndGet();
                    if (val < 0) counter.set(0);
                }
            }
        }
    }

    /**
     * Set heartbeat for a job with TTL.
     */
    public void setHeartbeat(Long jobId, int ttlSeconds) {
        RBucket<String> bucket = redissonClient.getBucket(HEARTBEAT_PREFIX + jobId);
        bucket.set(Instant.now().toString(), Duration.ofSeconds(ttlSeconds));
    }

    /**
     * Check if a heartbeat exists for a job.
     */
    public boolean hasHeartbeat(Long jobId) {
        RBucket<String> bucket = redissonClient.getBucket(HEARTBEAT_PREFIX + jobId);
        return bucket.isExists();
    }

    /**
     * Delete heartbeat for a job.
     */
    public void deleteHeartbeat(Long jobId) {
        redissonClient.getBucket(HEARTBEAT_PREFIX + jobId).delete();
    }

    /**
     * Refresh concurrency counters from database (reconciliation).
     */
    public void refreshCounters(String jobName, Map<String, Integer> concurrencyRule) {
        log.info("Refreshing concurrency counters for job: {}", jobName);

        // Set global counter
        long globalCount = jobRepository.countRunningGlobal(jobName);
        redissonClient.getAtomicLong(buildKey(jobName, GLOBAL_KEY, null)).set(globalCount);

        // For rule-based counters, we need to count per distinct value
        // This is done by querying the DB for all running jobs and their concurrency control values
        var runningJobs = jobRepository.findByStatusAndJobName(
            com.jeduler.model.enums.JobStatus.PROCESSING, jobName);
        var publishedJobs = jobRepository.findByStatusAndJobName(
            com.jeduler.model.enums.JobStatus.PUBLISHED, jobName);

        // Combine running + published
        Map<String, Long> ruleCounts = new HashMap<>();
        for (var job : runningJobs) {
            accumulateRuleCounts(job, concurrencyRule, ruleCounts);
        }
        for (var job : publishedJobs) {
            accumulateRuleCounts(job, concurrencyRule, ruleCounts);
        }

        // Set each counter in Redis
        for (Map.Entry<String, Long> entry : ruleCounts.entrySet()) {
            redissonClient.getAtomicLong(entry.getKey()).set(entry.getValue());
        }

        log.info("Refreshed counters for {}: global={}, rules={}", jobName, globalCount, ruleCounts.size());
    }

    /**
     * Get concurrency status for monitoring endpoint.
     */
    public Map<String, Map<String, Long>> getConcurrencyStatus(String jobName, Map<String, Integer> concurrencyRule) {
        Map<String, Map<String, Long>> status = new HashMap<>();

        // Global
        long globalCount = redissonClient.getAtomicLong(buildKey(jobName, GLOBAL_KEY, null)).get();
        Integer globalLimit = concurrencyRule.get(GLOBAL_KEY);
        if (globalLimit != null) {
            Map<String, Long> globalStatus = new HashMap<>();
            globalStatus.put("current", globalCount);
            globalStatus.put("limit", (long) globalLimit);
            status.put(GLOBAL_KEY, globalStatus);
        }

        return status;
    }

    private void accumulateRuleCounts(Job job, Map<String, Integer> concurrencyRule, Map<String, Long> ruleCounts) {
        Map<String, String> control = job.getConcurrencyControl();
        if (control == null) return;

        for (Map.Entry<String, Integer> rule : concurrencyRule.entrySet()) {
            if (GLOBAL_KEY.equals(rule.getKey())) continue;
            String value = control.get(rule.getKey());
            if (value != null) {
                String key = buildKey(job.getJobName(), rule.getKey(), value);
                ruleCounts.merge(key, 1L, Long::sum);
            }
        }
    }

    private String buildKey(String jobName, String ruleKey, String ruleValue) {
        if (GLOBAL_KEY.equals(ruleKey)) {
            return CONCURRENCY_PREFIX + jobName + ":" + GLOBAL_KEY;
        }
        return CONCURRENCY_PREFIX + jobName + ":" + ruleKey + ":" + ruleValue;
    }
}
