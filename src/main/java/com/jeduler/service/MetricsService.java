package com.jeduler.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class MetricsService {

    private final MeterRegistry registry;
    private final Map<String, AtomicLong> gaugeValues = new ConcurrentHashMap<>();

    public MetricsService(MeterRegistry registry) {
        this.registry = registry;
    }

    public void incrementJobsTotal(String status, String jobName) {
        Counter.builder("jeduler_jobs_total")
            .tag("status", status)
            .tag("jobName", jobName)
            .register(registry)
            .increment();
    }

    public void incrementStateTransition(String from, String to, String jobName) {
        Counter.builder("jeduler_state_transitions_total")
            .tag("from", from)
            .tag("to", to)
            .tag("jobName", jobName)
            .register(registry)
            .increment();
    }

    public Timer.Sample startSchedulingTimer() {
        return Timer.start(registry);
    }

    public void stopSchedulingTimer(Timer.Sample sample, String jobName) {
        sample.stop(Timer.builder("jeduler_scheduling_duration_seconds")
            .tag("jobName", jobName)
            .register(registry));
    }

    public void recordConcurrencyUtilization(String jobName, String rule, double current, double max) {
        String key = "concurrency_" + jobName + "_" + rule;
        AtomicLong gauge = gaugeValues.computeIfAbsent(key, k -> {
            AtomicLong val = new AtomicLong();
            Gauge.builder("jeduler_concurrency_utilization", val, AtomicLong::doubleValue)
                .tag("jobName", jobName)
                .tag("rule", rule)
                .register(registry);
            return val;
        });
        gauge.set((long) current);
    }

    public void incrementHeartbeatFailures(String jobName) {
        Counter.builder("jeduler_heartbeat_failures_total")
            .tag("jobName", jobName)
            .register(registry)
            .increment();
    }

    public Timer.Sample startKafkaPublishTimer() {
        return Timer.start(registry);
    }

    public void stopKafkaPublishTimer(Timer.Sample sample) {
        sample.stop(Timer.builder("jeduler_kafka_publish_duration_seconds")
            .register(registry));
    }

    public void recordQueueDepth(String jobName, String status, long count) {
        String key = "queue_" + jobName + "_" + status;
        AtomicLong gauge = gaugeValues.computeIfAbsent(key, k -> {
            AtomicLong val = new AtomicLong();
            Gauge.builder("jeduler_queue_depth", val, AtomicLong::doubleValue)
                .tag("jobName", jobName)
                .tag("status", status)
                .register(registry);
            return val;
        });
        gauge.set(count);
    }
}
