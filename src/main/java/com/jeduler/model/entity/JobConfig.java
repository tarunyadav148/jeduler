package com.jeduler.model.entity;

import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.Map;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "jfc_job_config")
public class JobConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_name", nullable = false, unique = true)
    private String jobName;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "concurrency_rule", nullable = false, columnDefinition = "jsonb")
    private Map<String, Integer> concurrencyRule;

    @Column(name = "max_retries", nullable = false)
    private Integer maxRetries = 3;

    @Column(name = "destination_type", nullable = false)
    private String destinationType = "KAFKA";

    @Column(name = "destination", nullable = false)
    private String destination;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "destination_metadata", columnDefinition = "jsonb")
    private Map<String, Object> destinationMetadata;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = OffsetDateTime.now();
        updatedAt = OffsetDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = OffsetDateTime.now();
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }

    public Map<String, Integer> getConcurrencyRule() { return concurrencyRule; }
    public void setConcurrencyRule(Map<String, Integer> concurrencyRule) { this.concurrencyRule = concurrencyRule; }

    public Integer getMaxRetries() { return maxRetries; }
    public void setMaxRetries(Integer maxRetries) { this.maxRetries = maxRetries; }

    public String getDestinationType() { return destinationType; }
    public void setDestinationType(String destinationType) { this.destinationType = destinationType; }

    public String getDestination() { return destination; }
    public void setDestination(String destination) { this.destination = destination; }

    public Map<String, Object> getDestinationMetadata() { return destinationMetadata; }
    public void setDestinationMetadata(Map<String, Object> destinationMetadata) { this.destinationMetadata = destinationMetadata; }

    public OffsetDateTime getCreatedAt() { return createdAt; }
    public OffsetDateTime getUpdatedAt() { return updatedAt; }
}
