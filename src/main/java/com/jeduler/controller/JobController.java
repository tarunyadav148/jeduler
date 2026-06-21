package com.jeduler.controller;

import com.jeduler.model.dto.*;
import com.jeduler.service.JobService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public ResponseEntity<JobSubmitResponse> submitJob(@RequestBody JobSubmitRequest request) {
        JobSubmitResponse response = jobService.submitJob(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PostMapping("/batch")
    public ResponseEntity<BatchJobSubmitResponse> submitBatch(@RequestBody BatchJobSubmitRequest request) {
        BatchJobSubmitResponse response = jobService.submitBatch(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PostMapping("/{jobId}/status")
    public ResponseEntity<JobStatusUpdateResponse> updateStatus(
            @PathVariable Long jobId,
            @RequestBody JobStatusUpdateRequest request) {
        JobStatusUpdateResponse response = jobService.updateJobStatus(jobId, request);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/{jobId}/heartbeat")
    public ResponseEntity<HeartbeatResponse> heartbeat(
            @PathVariable Long jobId,
            @RequestBody(required = false) HeartbeatRequest request) {
        HeartbeatResponse response = jobService.processHeartbeat(jobId, request);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/{jobId}/retry")
    public ResponseEntity<Map<String, Object>> retryJob(@PathVariable Long jobId) {
        jobService.retryJob(jobId);
        return ResponseEntity.ok(Map.of("jobId", jobId, "status", "RETRY", "message", "Job queued for retry"));
    }

    @PostMapping("/retry")
    public ResponseEntity<Map<String, Object>> bulkRetry(@RequestBody RetryRequest request) {
        return ResponseEntity.ok(jobService.bulkRetry(request));
    }

    @PostMapping("/cancel")
    public ResponseEntity<Map<String, Object>> cancelJobs(@RequestBody CancelRequest request) {
        return ResponseEntity.ok(jobService.cancelJobs(request));
    }

    @PostMapping("/cancel/{tenantId}")
    public ResponseEntity<Map<String, Object>> cancelByTenant(@PathVariable int tenantId) {
        return ResponseEntity.ok(jobService.cancelByTenant(tenantId));
    }

    @PostMapping("/priority")
    public ResponseEntity<Map<String, Object>> updatePriority(@RequestBody PriorityUpdateRequest request) {
        return ResponseEntity.ok(jobService.updatePriority(request));
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<JobDetailResponse> getJob(@PathVariable Long jobId) {
        return ResponseEntity.ok(jobService.getJobDetail(jobId));
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Map<String, Long>>> getStats() {
        return ResponseEntity.ok(jobService.getJobStats());
    }
}
