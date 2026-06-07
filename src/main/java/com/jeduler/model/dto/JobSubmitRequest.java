package com.jeduler.model.dto;

import java.util.Map;

public record JobSubmitRequest(
    String jobName,
    Integer priority,
    Integer tenant,
    Map<String, Object> payload,
    Map<String, String> concurrencyControl
) {}
