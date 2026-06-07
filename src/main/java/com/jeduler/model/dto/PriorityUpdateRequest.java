package com.jeduler.model.dto;

import java.util.List;

public record PriorityUpdateRequest(
    List<Long> jobIds,
    Integer priority
) {}
