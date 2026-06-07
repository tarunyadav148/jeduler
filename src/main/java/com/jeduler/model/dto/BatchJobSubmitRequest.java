package com.jeduler.model.dto;

import java.util.List;

public record BatchJobSubmitRequest(
    List<JobSubmitRequest> jobs
) {}
