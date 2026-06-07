package com.jeduler.model.dto;

import java.util.Map;

public record JobStatusUpdateRequest(
    String status,
    Map<String, Object> source,
    Map<String, Object> result,
    String reason
) {}
