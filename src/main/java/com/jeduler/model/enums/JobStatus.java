package com.jeduler.model.enums;

public enum JobStatus {
    WAITING,
    PUBLISHED,
    PROCESSING,
    SUCCESSFUL,
    FAILED,
    FAILED_BY_SCHEDULER,
    RETRY,
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCESSFUL || this == FAILED || this == CANCELLED;
    }

    public boolean isActive() {
        return this == WAITING || this == PUBLISHED || this == PROCESSING || this == RETRY;
    }

    public boolean isSchedulable() {
        return this == WAITING || this == RETRY;
    }

    public boolean canTransitionTo(JobStatus target) {
        return switch (this) {
            case WAITING -> target == PUBLISHED || target == CANCELLED;
            case PUBLISHED -> target == PROCESSING || target == FAILED_BY_SCHEDULER || target == CANCELLED;
            case PROCESSING -> target == SUCCESSFUL || target == FAILED || target == FAILED_BY_SCHEDULER;
            case FAILED, FAILED_BY_SCHEDULER -> target == RETRY || target == CANCELLED;
            case RETRY -> target == PUBLISHED || target == CANCELLED;
            case SUCCESSFUL, CANCELLED -> false;
        };
    }
}
