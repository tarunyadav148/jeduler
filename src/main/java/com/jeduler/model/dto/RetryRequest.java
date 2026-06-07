package com.jeduler.model.dto;

import java.util.List;

public record RetryRequest(
    List<Long> jobIds
) {}
