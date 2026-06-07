package com.jeduler.model.dto;

import java.time.OffsetDateTime;
import java.util.Map;

public record JobDetailResponse(
    Long jobId,
    String jobName,
    Integer tenant,
    Integer priority,
    String status,
    Integer executionCount,
    Map<String, Object> payload,
    Map<String, String> concurrencyControl,
    String lastFailureReason,
    OffsetDateTime submitTime,
    OffsetDateTime lastUpdated
) {}
