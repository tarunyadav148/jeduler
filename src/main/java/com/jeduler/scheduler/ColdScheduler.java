package com.jeduler.scheduler;

import com.jeduler.config.SchedulerProperties;
import com.jeduler.model.enums.JobStatus;
import com.jeduler.repository.JobRepository;
import com.jeduler.service.ConcurrencyService;
import com.jeduler.service.SchedulingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Component
public class ColdScheduler {

    private static final Logger log = LoggerFactory.getLogger(ColdScheduler.class);

    private final JobRepository jobRepository;
    private final SchedulingService schedulingService;
    private final SchedulerProperties properties;

    public ColdScheduler(
            JobRepository jobRepository,
            SchedulingService schedulingService,
            SchedulerProperties properties) {
        this.jobRepository = jobRepository;
        this.schedulingService = schedulingService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${job.scheduler.cold.interval-ms:120000}")
    public void scheduleStaleJobNames() {
        if (!properties.isEnabled()) return;

        List<String> jobNamesWithWork = jobRepository.findDistinctJobNamesByStatusIn(
            List.of(JobStatus.WAITING, JobStatus.RETRY));

        Duration staleThreshold = Duration.ofMinutes(properties.getCold().getStaleThresholdMinutes());
        Map<String, Instant> lastAttempts = schedulingService.getLastAttempts();
        int triggered = 0;

        for (String jobName : jobNamesWithWork) {
            Instant last = lastAttempts.get(jobName);
            if (last != null && Instant.now().minus(staleThreshold).isBefore(last)) {
                continue; // Recently scheduled
            }

            if (schedulingService.triggerScheduling(jobName)) {
                triggered++;
            }
        }

        if (triggered > 0) {
            log.info("Cold scheduler triggered scheduling for {} job types", triggered);
        }
    }
}
