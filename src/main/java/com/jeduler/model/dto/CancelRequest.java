package com.jeduler.model.dto;

import java.util.List;

public record CancelRequest(
    List<Long> jobIds,
    Integer tenant,
    String jobName
) {}
