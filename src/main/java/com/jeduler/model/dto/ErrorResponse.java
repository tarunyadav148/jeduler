package com.jeduler.model.dto;

import java.time.OffsetDateTime;

public record ErrorResponse(
    String error,
    String message,
    OffsetDateTime timestamp
) {
    public ErrorResponse(String error, String message) {
        this(error, message, OffsetDateTime.now());
    }
}
