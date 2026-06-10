package com.jeduler.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class SchedulerConfig {

    @Value("${job.scheduler.thread-pool-size:10}")
    private int threadPoolSize;

    @Bean(name = "schedulerExecutor")
    public Executor schedulerExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threadPoolSize);
        executor.setMaxPoolSize(threadPoolSize);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("scheduler-");
        executor.initialize();
        return executor;
    }
}
