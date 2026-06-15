package com.jeduler.service;

import com.jeduler.model.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaPublisherService {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisherService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaPublisherService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publish a job dispatch message to the appropriate Kafka topic.
     */
    public CompletableFuture<SendResult<String, Object>> publishJob(Job job, String destination, int maxRetries) {
        Map<String, Object> message = buildDispatchMessage(job, maxRetries);
        String key = String.valueOf(job.getId());

        log.info("Publishing job {} to topic {}", job.getId(), destination);

        return kafkaTemplate.send(destination, key, message);
    }

    private Map<String, Object> buildDispatchMessage(Job job, int maxRetries) {
        Map<String, Object> message = new HashMap<>();
        message.put("schema", "job-dispatch-v1");
        message.put("jobId", job.getId());
        message.put("jobName", job.getJobName());
        message.put("tenant", job.getTenant());
        message.put("priority", job.getPriority());
        message.put("executionCount", job.getExecutionCount());
        message.put("maxRetries", maxRetries);
        message.put("payload", job.getPayload());
        message.put("concurrencyControl", job.getConcurrencyControl());
        message.put("dispatchedAt", OffsetDateTime.now().toString());
        message.put("callbackUrl", "http://scheduler:8080/api/v1/jobs/" + job.getId() + "/status");
        message.put("heartbeatUrl", "http://scheduler:8080/api/v1/jobs/" + job.getId() + "/heartbeat");
        return message;
    }
}
