package com.core.order_job;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@SpringBootApplication(scanBasePackages = "com.core")
@EntityScan(basePackages = "com.core.order_model.entity")
@EnableJpaRepositories(basePackages = "com.core.order_model.repository")
@EnableMongoRepositories(basePackages = "com.core.order_model.repository")
public class OrderJobApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderJobApplication.class, args);
    }
}
