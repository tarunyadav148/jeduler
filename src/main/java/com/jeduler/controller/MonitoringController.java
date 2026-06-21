package com.jeduler.controller;

import com.jeduler.model.entity.JobConfig;
import com.jeduler.service.ConcurrencyService;
import com.jeduler.service.JobConfigService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class MonitoringController {

    private final ConcurrencyService concurrencyService;
    private final JobConfigService jobConfigService;

    public MonitoringController(ConcurrencyService concurrencyService, JobConfigService jobConfigService) {
        this.concurrencyService = concurrencyService;
        this.jobConfigService = jobConfigService;
    }

    @GetMapping("/concurrency/status")
    public ResponseEntity<Map<String, Map<String, Map<String, Long>>>> getConcurrencyStatus() {
        Map<String, Map<String, Map<String, Long>>> result = new HashMap<>();

        for (var entry : jobConfigService.getAllConfigs().entrySet()) {
            String jobName = entry.getKey();
            JobConfig config = entry.getValue();
            Map<String, Integer> rules = config.getConcurrencyRule();

            Map<String, Map<String, Long>> jobStatus = new HashMap<>();

            // Global counter
            if (rules.containsKey("_global")) {
                long current = concurrencyService.getGlobalCount(jobName);
                jobStatus.put("_global", Map.of("current", current, "limit", (long) rules.get("_global")));
            }

            result.put(jobName, jobStatus);
        }

        return ResponseEntity.ok(result);
    }
}
