package com.jeduler.model.dto;

import java.time.OffsetDateTime;

public record JobSubmitResponse(
    Long jobId,
    String status,
    OffsetDateTime submittedAt
) {}
