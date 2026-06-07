package com.jeduler.model.dto;

public record HeartbeatRequest(
    Integer progress,
    String message
) {}
