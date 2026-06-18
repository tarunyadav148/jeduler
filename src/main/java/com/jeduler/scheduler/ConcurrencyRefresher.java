package com.jeduler.scheduler;

import com.jeduler.config.SchedulerProperties;
import com.jeduler.model.entity.JobConfig;
import com.jeduler.service.ConcurrencyService;
import com.jeduler.service.JobConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ConcurrencyRefresher {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyRefresher.class);

    private final ConcurrencyService concurrencyService;
    private final JobConfigService jobConfigService;
    private final SchedulerProperties properties;

    public ConcurrencyRefresher(
            ConcurrencyService concurrencyService,
            JobConfigService jobConfigService,
            SchedulerProperties properties) {
        this.concurrencyService = concurrencyService;
        this.jobConfigService = jobConfigService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${job.scheduler.concurrency.refresh-interval-ms:300000}")
    public void refreshConcurrencyCounters() {
        if (!properties.isEnabled()) return;

        log.info("Starting concurrency counter refresh");

        for (var entry : jobConfigService.getAllConfigs().entrySet()) {
            try {
                concurrencyService.refreshCounters(entry.getKey(), entry.getValue().getConcurrencyRule());
            } catch (Exception e) {
                log.error("Failed to refresh counters for jobName={}", entry.getKey(), e);
            }
        }

        log.info("Concurrency counter refresh complete");
    }
}
