package com.jeduler.repository;

import com.jeduler.model.entity.JobConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface JobConfigRepository extends JpaRepository<JobConfig, Long> {
    Optional<JobConfig> findByJobName(String jobName);
}
