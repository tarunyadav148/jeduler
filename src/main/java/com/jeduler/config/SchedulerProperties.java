package com.jeduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "job.scheduler")
public class SchedulerProperties {

    private boolean enabled = true;
    private int threadPoolSize = 10;
    private HeartbeatProperties heartbeat = new HeartbeatProperties();
    private ColdProperties cold = new ColdProperties();
    private ConcurrencyProperties concurrency = new ConcurrencyProperties();

    public static class HeartbeatProperties {
        private long checkIntervalMs = 30000;
        private int timeoutSeconds = 60;

        public long getCheckIntervalMs() { return checkIntervalMs; }
        public void setCheckIntervalMs(long checkIntervalMs) { this.checkIntervalMs = checkIntervalMs; }
        public int getTimeoutSeconds() { return timeoutSeconds; }
        public void setTimeoutSeconds(int timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }
    }

    public static class ColdProperties {
        private long intervalMs = 120000;
        private int staleThresholdMinutes = 2;

        public long getIntervalMs() { return intervalMs; }
        public void setIntervalMs(long intervalMs) { this.intervalMs = intervalMs; }
        public int getStaleThresholdMinutes() { return staleThresholdMinutes; }
        public void setStaleThresholdMinutes(int staleThresholdMinutes) { this.staleThresholdMinutes = staleThresholdMinutes; }
    }

    public static class ConcurrencyProperties {
        private long refreshIntervalMs = 300000;

        public long getRefreshIntervalMs() { return refreshIntervalMs; }
        public void setRefreshIntervalMs(long refreshIntervalMs) { this.refreshIntervalMs = refreshIntervalMs; }
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public int getThreadPoolSize() { return threadPoolSize; }
    public void setThreadPoolSize(int threadPoolSize) { this.threadPoolSize = threadPoolSize; }
    public HeartbeatProperties getHeartbeat() { return heartbeat; }
    public void setHeartbeat(HeartbeatProperties heartbeat) { this.heartbeat = heartbeat; }
    public ColdProperties getCold() { return cold; }
    public void setCold(ColdProperties cold) { this.cold = cold; }
    public ConcurrencyProperties getConcurrency() { return concurrency; }
    public void setConcurrency(ConcurrencyProperties concurrency) { this.concurrency = concurrency; }
}
