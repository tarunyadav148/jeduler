package com.jeduler.model.dto;

public record HeartbeatResponse(
    boolean acknowledged,
    Long jobId,
    int ttlSeconds
) {}
