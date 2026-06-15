package com.jeduler.service;

import com.jeduler.config.SchedulerProperties;
import com.jeduler.model.dto.*;
import com.jeduler.model.entity.Job;
import com.jeduler.model.entity.JobConfig;
import com.jeduler.model.enums.JobStatus;
import com.jeduler.repository.JobRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.*;

@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final JobRepository jobRepository;
    private final JobConfigService jobConfigService;
    private final ConcurrencyService concurrencyService;
    private final SchedulingService schedulingService;
    private final MetricsService metricsService;
    private final SchedulerProperties properties;
    private final ObjectMapper objectMapper;

    public JobService(
            JobRepository jobRepository,
            JobConfigService jobConfigService,
            ConcurrencyService concurrencyService,
            SchedulingService schedulingService,
            MetricsService metricsService,
            SchedulerProperties properties,
            ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.jobConfigService = jobConfigService;
        this.concurrencyService = concurrencyService;
        this.schedulingService = schedulingService;
        this.metricsService = metricsService;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    /**
     * Submit a single job.
     */
    @Transactional
    public JobSubmitResponse submitJob(JobSubmitRequest request) {
        // Validate job name
        if (!jobConfigService.isValidJobName(request.jobName())) {
            throw new IllegalArgumentException("Job name '" + request.jobName() + "' is not configured");
        }

        // Validate priority
        if (request.priority() != null && request.priority() < 1) {
            throw new IllegalArgumentException("Priority must be >= 1");
        }

        // Create job entity
        Job job = new Job();
        job.setJobName(request.jobName());
        job.setTenant(request.tenant());
        job.setPriority(request.priority() != null ? request.priority() : 10);
        job.setPayload(request.payload());
        job.setConcurrencyControl(request.concurrencyControl() != null ? request.concurrencyControl() : Map.of());
        job.setStatus(JobStatus.WAITING);
        job.setExecutionCount(0);

        // Compute payload hash for deduplication
        String hash = computePayloadHash(request.payload());
        job.setPayloadHash(hash);

        // Save job
        try {
            job = jobRepository.save(job);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("idx_job_dedup")) {
                throw new DuplicateJobException("A job with this payload already exists in active state");
            }
            throw e;
        }

        // Record metrics
        metricsService.incrementJobsTotal("WAITING", job.getJobName());

        // Trigger scheduling
        schedulingService.triggerScheduling(job.getJobName());

        return new JobSubmitResponse(job.getId(), "WAITING", job.getSubmitTime());
    }

    /**
     * Submit a batch of jobs.
     */
    @Transactional
    public BatchJobSubmitResponse submitBatch(BatchJobSubmitRequest request) {
        List<JobSubmitResponse> successJobs = new ArrayList<>();
        List<BatchJobSubmitResponse.BatchError> errors = new ArrayList<>();
        Set<String> jobNamesToSchedule = new HashSet<>();

        for (int i = 0; i < request.jobs().size(); i++) {
            try {
                JobSubmitResponse response = submitJob(request.jobs().get(i));
                successJobs.add(response);
                jobNamesToSchedule.add(request.jobs().get(i).jobName());
            } catch (DuplicateJobException e) {
                errors.add(new BatchJobSubmitResponse.BatchError(i, "DUPLICATE_JOB", e.getMessage()));
            } catch (IllegalArgumentException e) {
                errors.add(new BatchJobSubmitResponse.BatchError(i, "VALIDATION_ERROR", e.getMessage()));
            } catch (Exception e) {
                errors.add(new BatchJobSubmitResponse.BatchError(i, "INTERNAL_ERROR", e.getMessage()));
            }
        }

        // Trigger scheduling for all affected job names
        jobNamesToSchedule.forEach(schedulingService::triggerScheduling);

        return new BatchJobSubmitResponse(successJobs.size(), errors.size(), successJobs, errors);
    }

    /**
     * Update job status (called by consumers).
     */
    @Transactional
    public JobStatusUpdateResponse updateJobStatus(Long jobId, JobStatusUpdateRequest request) {
        Job job = jobRepository.findById(jobId)
            .orElseThrow(() -> new NoSuchElementException("Job not found: " + jobId));

        JobStatus targetStatus = JobStatus.valueOf(request.status());
        JobStatus previousStatus = job.getStatus();

        // Validate transition
        if (!previousStatus.canTransitionTo(targetStatus)) {
            throw new InvalidTransitionException(
                "Cannot transition from " + previousStatus + " to " + targetStatus,
                previousStatus.name());
        }

        // Apply status change
        job.setStatus(targetStatus);

        // Handle specific transitions
        switch (targetStatus) {
            case PROCESSING -> {
                // Set initial heartbeat
                concurrencyService.setHeartbeat(jobId, properties.getHeartbeat().getTimeoutSeconds());
            }
            case SUCCESSFUL -> {
                handleJobCompletion(job);
            }
            case FAILED -> {
                job.setLastFailureReason(request.reason());
                job.setExecutionCount(job.getExecutionCount() + 1);
                handleJobFailure(job);
            }
            default -> {}
        }

        // Store processing metadata
        if (request.source() != null) {
            Map<String, Object> metadata = job.getJobProcessingMetadata();
            if (metadata == null) metadata = new HashMap<>();
            Map<String, Object> execInfo = new HashMap<>();
            execInfo.put("status", targetStatus.name());
            execInfo.put("source", request.source());
            execInfo.put("timestamp", OffsetDateTime.now().toString());
            if (request.result() != null) execInfo.put("result", request.result());

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> history = (List<Map<String, Object>>) metadata.computeIfAbsent("executionInfo", k -> new ArrayList<>());
            history.add(execInfo);
            job.setJobProcessingMetadata(metadata);
        }

        jobRepository.save(job);

        // Record metrics
        metricsService.incrementStateTransition(previousStatus.name(), targetStatus.name(), job.getJobName());

        return new JobStatusUpdateResponse(jobId, previousStatus.name(), targetStatus.name(), true);
    }

    /**
     * Process heartbeat from a consumer.
     */
    public HeartbeatResponse processHeartbeat(Long jobId, HeartbeatRequest request) {
        Job job = jobRepository.findById(jobId)
            .orElseThrow(() -> new NoSuchElementException("Job not found: " + jobId));

        if (job.getStatus() != JobStatus.PROCESSING) {
            throw new IllegalStateException(
                "Job " + jobId + " is not in PROCESSING state (current: " + job.getStatus() + ")");
        }

        int ttl = properties.getHeartbeat().getTimeoutSeconds();
        concurrencyService.setHeartbeat(jobId, ttl);

        return new HeartbeatResponse(true, jobId, ttl);
    }

    /**
     * Retry a single job.
     */
    @Transactional
    public void retryJob(Long jobId) {
        Job job = jobRepository.findById(jobId)
            .orElseThrow(() -> new NoSuchElementException("Job not found: " + jobId));

        if (!job.getStatus().isTerminal() && job.getStatus() != JobStatus.FAILED_BY_SCHEDULER) {
            throw new IllegalStateException("Job " + jobId + " is not in a retryable state");
        }

        job.setStatus(JobStatus.RETRY);
        jobRepository.save(job);
        schedulingService.triggerScheduling(job.getJobName());
    }

    /**
     * Bulk retry jobs.
     */
    @Transactional
    public Map<String, Object> bulkRetry(RetryRequest request) {
        int retried = 0;
        List<Long> skipped = new ArrayList<>();

        for (Long id : request.jobIds()) {
            try {
                retryJob(id);
                retried++;
            } catch (Exception e) {
                skipped.add(id);
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("requested", request.jobIds().size());
        result.put("retried", retried);
        result.put("skipped", skipped.size());
        result.put("skippedIds", skipped);
        return result;
    }

    /**
     * Cancel jobs by IDs or by criteria.
     */
    @Transactional
    public Map<String, Object> cancelJobs(CancelRequest request) {
        int cancelled = 0;

        if (request.jobIds() != null && !request.jobIds().isEmpty()) {
            cancelled = jobRepository.updateStatusForIds(
                request.jobIds(),
                JobStatus.CANCELLED,
                List.of(JobStatus.WAITING, JobStatus.RETRY, JobStatus.PUBLISHED));
        } else if (request.tenant() != null && request.jobName() != null) {
            cancelled = jobRepository.cancelByTenantAndJobName(request.tenant(), request.jobName());
        } else if (request.tenant() != null) {
            cancelled = jobRepository.cancelByTenant(request.tenant());
        }

        return Map.of("cancelled", cancelled);
    }

    /**
     * Cancel all pending jobs for a tenant.
     */
    @Transactional
    public Map<String, Object> cancelByTenant(int tenantId) {
        int cancelled = jobRepository.cancelByTenant(tenantId);
        return Map.of("cancelled", cancelled);
    }

    /**
     * Update priority for jobs.
     */
    @Transactional
    public Map<String, Object> updatePriority(PriorityUpdateRequest request) {
        if (request.priority() < 1) {
            throw new IllegalArgumentException("Priority must be >= 1");
        }
        int updated = jobRepository.updatePriorityForIds(request.jobIds(), request.priority());
        return Map.of("updated", updated);
    }

    /**
     * Get job details.
     */
    public JobDetailResponse getJobDetail(Long jobId) {
        Job job = jobRepository.findById(jobId)
            .orElseThrow(() -> new NoSuchElementException("Job not found: " + jobId));

        return new JobDetailResponse(
            job.getId(),
            job.getJobName(),
            job.getTenant(),
            job.getPriority(),
            job.getStatus().name(),
            job.getExecutionCount(),
            job.getPayload(),
            job.getConcurrencyControl(),
            job.getLastFailureReason(),
            job.getSubmitTime(),
            job.getLastUpdated()
        );
    }

    /**
     * Get job stats by jobName and status.
     */
    public Map<String, Map<String, Long>> getJobStats() {
        List<Object[]> stats = jobRepository.getJobStats();
        Map<String, Map<String, Long>> result = new HashMap<>();

        for (Object[] row : stats) {
            String jobName = (String) row[0];
            String status = (String) row[1];
            Long count = ((Number) row[2]).longValue();

            result.computeIfAbsent(jobName, k -> new HashMap<>()).put(status, count);
        }

        return result;
    }

    // Private helpers

    private void handleJobCompletion(Job job) {
        // Delete heartbeat
        concurrencyService.deleteHeartbeat(job.getId());

        // Decrement concurrency counters
        var config = jobConfigService.getConfig(job.getJobName());
        config.ifPresent(c -> concurrencyService.decrementCounters(job, c.getConcurrencyRule()));

        // Trigger scheduling (freed slot)
        schedulingService.triggerScheduling(job.getJobName());
    }

    private void handleJobFailure(Job job) {
        // Delete heartbeat
        concurrencyService.deleteHeartbeat(job.getId());

        // Decrement concurrency counters
        var config = jobConfigService.getConfig(job.getJobName());
        config.ifPresent(c -> {
            concurrencyService.decrementCounters(job, c.getConcurrencyRule());

            // Auto-retry if eligible
            if (job.getExecutionCount() < c.getMaxRetries()) {
                job.setStatus(JobStatus.RETRY);
            }
        });

        // Trigger scheduling
        schedulingService.triggerScheduling(job.getJobName());
    }

    private String computePayloadHash(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) return null;
        try {
            String json = objectMapper.writeValueAsString(new TreeMap<>(payload));
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(json.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            log.warn("Failed to compute payload hash", e);
            return null;
        }
    }

    // Custom exceptions
    public static class DuplicateJobException extends RuntimeException {
        public DuplicateJobException(String message) { super(message); }
    }

    public static class InvalidTransitionException extends RuntimeException {
        private final String currentStatus;
        public InvalidTransitionException(String message, String currentStatus) {
            super(message);
            this.currentStatus = currentStatus;
        }
        public String getCurrentStatus() { return currentStatus; }
    }
}
