package com.jeduler.controller;

import com.jeduler.service.SchedulingService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/scheduling")
public class SchedulingController {

    private final SchedulingService schedulingService;

    public SchedulingController(SchedulingService schedulingService) {
        this.schedulingService = schedulingService;
    }

    @PostMapping("/pause/tenant/{tenantId}")
    public ResponseEntity<Map<String, Object>> pauseTenant(@PathVariable int tenantId) {
        schedulingService.pauseTenant(tenantId);
        return ResponseEntity.ok(Map.of("paused", true, "tenant", tenantId));
    }

    @PostMapping("/resume/tenant/{tenantId}")
    public ResponseEntity<Map<String, Object>> resumeTenant(@PathVariable int tenantId) {
        schedulingService.resumeTenant(tenantId);
        // Trigger scheduling for all job types
        schedulingService.getLastAttempts().keySet().forEach(schedulingService::triggerScheduling);
        return ResponseEntity.ok(Map.of("resumed", true, "tenant", tenantId));
    }

    @PostMapping("/pause/job/{jobName}")
    public ResponseEntity<Map<String, Object>> pauseJobName(@PathVariable String jobName) {
        schedulingService.pauseJobName(jobName);
        return ResponseEntity.ok(Map.of("paused", true, "jobName", jobName));
    }

    @PostMapping("/resume/job/{jobName}")
    public ResponseEntity<Map<String, Object>> resumeJobName(@PathVariable String jobName) {
        schedulingService.resumeJobName(jobName);
        schedulingService.triggerScheduling(jobName);
        return ResponseEntity.ok(Map.of("resumed", true, "jobName", jobName));
    }

    @GetMapping("/paused")
    public ResponseEntity<Map<String, Object>> getPaused() {
        return ResponseEntity.ok(Map.of(
            "pausedTenants", schedulingService.getPausedTenants(),
            "pausedJobNames", schedulingService.getPausedJobNames()
        ));
    }
}
