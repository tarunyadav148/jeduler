package com.jeduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class JedulerApplication {
    public static void main(String[] args) {
        SpringApplication.run(JedulerApplication.class, args);
    }
}
