package com.jeduler.service;

import com.jeduler.model.entity.JobConfig;
import com.jeduler.repository.JobConfigRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class JobConfigService {

    private static final Logger log = LoggerFactory.getLogger(JobConfigService.class);

    private final JobConfigRepository jobConfigRepository;
    private final Map<String, JobConfig> configCache = new ConcurrentHashMap<>();

    public JobConfigService(JobConfigRepository jobConfigRepository) {
        this.jobConfigRepository = jobConfigRepository;
    }

    @PostConstruct
    public void loadConfigs() {
        List<JobConfig> configs = jobConfigRepository.findAll();
        configs.forEach(config -> configCache.put(config.getJobName(), config));
        log.info("Loaded {} job configurations", configs.size());
    }

    public Optional<JobConfig> getConfig(String jobName) {
        return Optional.ofNullable(configCache.get(jobName));
    }

    public Map<String, JobConfig> getAllConfigs() {
        return Map.copyOf(configCache);
    }

    public boolean isValidJobName(String jobName) {
        return configCache.containsKey(jobName);
    }

    public void refreshCache() {
        configCache.clear();
        loadConfigs();
    }
}
