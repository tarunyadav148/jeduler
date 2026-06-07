package com.jeduler.model.dto;

public record JobStatusUpdateResponse(
    Long jobId,
    String previousStatus,
    String currentStatus,
    boolean transitionAccepted
) {}
