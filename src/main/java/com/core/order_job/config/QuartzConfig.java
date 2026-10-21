package com.core.order_job.config;

import com.core.order_job.job.EventStoreCleanupJob;
import com.core.order_job.job.FailedEventsRetryJob;
import com.core.order_job.job.OutboxPollingPublisherJob;
import org.quartz.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Quartz Scheduler Jobs & Triggers configuration.
 */
@Configuration
public class QuartzConfig {

    public static final String OUTBOX_JOB_IDENTITY = "outboxPollingJob";
    public static final String OUTBOX_TRIGGER_IDENTITY = "outboxPollingTrigger";

    public static final String FAILED_EVENTS_JOB_IDENTITY = "failedEventsRetryJob";
    public static final String FAILED_EVENTS_TRIGGER_IDENTITY = "failedEventsRetryTrigger";

    public static final String CLEANUP_JOB_IDENTITY = "eventStoreCleanupJob";
    public static final String CLEANUP_TRIGGER_IDENTITY = "eventStoreCleanupTrigger";

    @Bean
    public JobDetail outboxJobDetail() {
        return JobBuilder.newJob(OutboxPollingPublisherJob.class)
                .withIdentity(OUTBOX_JOB_IDENTITY)
                .storeDurably()
                .withDescription("Polls transactional outbox table and dispatches events to Kafka")
                .build();
    }

    @Bean
    public Trigger outboxJobTrigger(JobDetail outboxJobDetail) {
        // Polls every 15 seconds to ensure any missed Outbox events are guaranteed to be published
        return TriggerBuilder.newTrigger()
                .forJob(outboxJobDetail)
                .withIdentity(OUTBOX_TRIGGER_IDENTITY)
                .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever(15))
                .build();
    }

    @Bean
    public JobDetail failedEventsJobDetail() {
        return JobBuilder.newJob(FailedEventsRetryJob.class)
                .withIdentity(FAILED_EVENTS_JOB_IDENTITY)
                .storeDurably()
                .withDescription("Retries Kafka events that failed processing up to 5 times")
                .build();
    }

    @Bean
    public Trigger failedEventsJobTrigger(JobDetail failedEventsJobDetail) {
        // Runs every 60 seconds
        return TriggerBuilder.newTrigger()
                .forJob(failedEventsJobDetail)
                .withIdentity(FAILED_EVENTS_TRIGGER_IDENTITY)
                .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever(60))
                .build();
    }

    @Bean
    public JobDetail cleanupJobDetail() {
        return JobBuilder.newJob(EventStoreCleanupJob.class)
                .withIdentity(CLEANUP_JOB_IDENTITY)
                .storeDurably()
                .withDescription("Cleans up historical idempotency records and stale locks")
                .build();
    }

    @Bean
    public Trigger cleanupJobTrigger(JobDetail cleanupJobDetail) {
        // Runs every 1 hour (3600 seconds)
        return TriggerBuilder.newTrigger()
                .forJob(cleanupJobDetail)
                .withIdentity(CLEANUP_TRIGGER_IDENTITY)
                .withSchedule(SimpleScheduleBuilder.repeatHourlyForever(1))
                .build();
    }
}
