package com.core.order_job.job;

import com.core.order_common.enums.EventStatus;
import com.core.order_common.util.TraceContext;
import com.core.order_model.document.OrderEventDocument;
import com.core.order_model.event.KafkaTopics;
import com.core.order_model.repository.OrderEventMongoRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Quartz Job that polls MongoDB for failed events and retries publishing/processing.
 */
@Slf4j
@Component
@DisallowConcurrentExecution
@RequiredArgsConstructor
public class FailedEventsRetryJob extends QuartzJobBean {

    private final OrderEventMongoRepository eventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final int MAX_RETRY_LIMIT = 5;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        TraceContext.setTraceId(TraceContext.generateTraceId());
        log.info("[QuartzJob] Starting FailedEventsRetryJob execution...");

        try {
            // Find all events with status FAILED and retryCount < 5
            List<OrderEventDocument> failedEvents = eventRepository
                    .findByStatusAndRetryCountLessThan(EventStatus.FAILED, MAX_RETRY_LIMIT);

            if (failedEvents.isEmpty()) {
                log.info("[QuartzJob] No failed events pending retry in MongoDB.");
                return;
            }

            log.info("[QuartzJob] Found [{}] failed events to retry.", failedEvents.size());

            for (OrderEventDocument event : failedEvents) {
                retryEvent(event);
            }
        } catch (Exception e) {
            log.error("[QuartzJob] Error during FailedEventsRetryJob: {}", e.getMessage(), e);
            throw new JobExecutionException(e);
        } finally {
            TraceContext.clear();
        }
    }

    private void retryEvent(OrderEventDocument event) {
        int nextRetry = event.getRetryCount() + 1;
        String topic = (event.getTopic() != null) ? event.getTopic() : KafkaTopics.ORDER_EVENTS_TOPIC;

        log.info("[QuartzJob] Retrying eventId=[{}] (attempt {}/{}) to topic=[{}]",
                event.getEventId(), nextRetry, MAX_RETRY_LIMIT, topic);

        try {
            kafkaTemplate.send(topic, event.getEventId(), event.getPayload()).whenComplete((result, ex) -> {
                if (ex == null) {
                    event.setRetryCount(nextRetry);
                    event.setStatus(EventStatus.RETRYING);
                    event.setUpdatedAt(Instant.now());
                    eventRepository.save(event);
                    log.info("[QuartzJob] Re-published failed event [{}] successfully.", event.getEventId());
                } else {
                    event.setRetryCount(nextRetry);
                    event.setLastError("Quartz retry error: " + ex.getMessage());
                    event.setUpdatedAt(Instant.now());
                    if (nextRetry >= MAX_RETRY_LIMIT) {
                        event.setStatus(EventStatus.DLT);
                        log.error("[QuartzJob] Event [{}] exceeded max 5 retries. Marked as DLT.", event.getEventId());
                    }
                    eventRepository.save(event);
                }
            });
        } catch (Exception ex) {
            log.error("[QuartzJob] Failed to send retry for event [{}]: {}", event.getEventId(), ex.getMessage());
        }
    }
}
