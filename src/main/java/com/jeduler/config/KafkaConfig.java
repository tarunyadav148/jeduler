package com.jeduler.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic vulnerabilityScanTopic() {
        return TopicBuilder.name("job-dispatch.vulnerability-scan")
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic codeAnalysisTopic() {
        return TopicBuilder.name("job-dispatch.code-analysis")
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic complianceCheckTopic() {
        return TopicBuilder.name("job-dispatch.compliance-check")
            .partitions(3)
            .replicas(1)
            .build();
    }
}
