package com.jeduler.model.dto;

import java.util.List;

public record BatchJobSubmitResponse(
    int submitted,
    int failed,
    List<JobSubmitResponse> jobs,
    List<BatchError> errors
) {
    public record BatchError(int index, String error, String message) {}
}
