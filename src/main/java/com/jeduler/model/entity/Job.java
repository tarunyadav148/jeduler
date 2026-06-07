package com.jeduler.model.entity;

import com.jeduler.model.enums.JobStatus;
import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.Map;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "jfc_job")
public class Job {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_name", nullable = false)
    private String jobName;

    @Column(name = "tenant")
    private Integer tenant;

    @Column(name = "priority", nullable = false)
    private Integer priority = 10;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private JobStatus status = JobStatus.WAITING;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "jsonb")
    private Map<String, Object> payload;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "concurrency_control", nullable = false, columnDefinition = "jsonb")
    private Map<String, String> concurrencyControl;

    @Column(name = "execution_count", nullable = false)
    private Integer executionCount = 0;

    @Column(name = "last_failure_reason")
    private String lastFailureReason;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "job_processing_metadata", columnDefinition = "jsonb")
    private Map<String, Object> jobProcessingMetadata;

    @Column(name = "payload_hash")
    private String payloadHash;

    @Column(name = "submit_time", nullable = false, updatable = false)
    private OffsetDateTime submitTime;

    @Column(name = "last_updated", nullable = false)
    private OffsetDateTime lastUpdated;

    @PrePersist
    protected void onCreate() {
        submitTime = OffsetDateTime.now();
        lastUpdated = OffsetDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        lastUpdated = OffsetDateTime.now();
    }

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }

    public Integer getTenant() { return tenant; }
    public void setTenant(Integer tenant) { this.tenant = tenant; }

    public Integer getPriority() { return priority; }
    public void setPriority(Integer priority) { this.priority = priority; }

    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; }

    public Map<String, Object> getPayload() { return payload; }
    public void setPayload(Map<String, Object> payload) { this.payload = payload; }

    public Map<String, String> getConcurrencyControl() { return concurrencyControl; }
    public void setConcurrencyControl(Map<String, String> concurrencyControl) { this.concurrencyControl = concurrencyControl; }

    public Integer getExecutionCount() { return executionCount; }
    public void setExecutionCount(Integer executionCount) { this.executionCount = executionCount; }

    public String getLastFailureReason() { return lastFailureReason; }
    public void setLastFailureReason(String lastFailureReason) { this.lastFailureReason = lastFailureReason; }

    public Map<String, Object> getJobProcessingMetadata() { return jobProcessingMetadata; }
    public void setJobProcessingMetadata(Map<String, Object> jobProcessingMetadata) { this.jobProcessingMetadata = jobProcessingMetadata; }

    public String getPayloadHash() { return payloadHash; }
    public void setPayloadHash(String payloadHash) { this.payloadHash = payloadHash; }

    public OffsetDateTime getSubmitTime() { return submitTime; }
    public void setSubmitTime(OffsetDateTime submitTime) { this.submitTime = submitTime; }

    public OffsetDateTime getLastUpdated() { return lastUpdated; }
    public void setLastUpdated(OffsetDateTime lastUpdated) { this.lastUpdated = lastUpdated; }
}
