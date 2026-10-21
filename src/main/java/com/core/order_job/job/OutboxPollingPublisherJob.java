package com.core.order_job.job;

import com.core.order_common.enums.EventStatus;
import com.core.order_common.enums.OutboxStatus;
import com.core.order_common.util.TraceContext;
import com.core.order_model.document.OrderEventDocument;
import com.core.order_model.entity.OutboxEntity;
import com.core.order_model.repository.OrderEventMongoRepository;
import com.core.order_model.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Transactional Outbox Quartz Polling Job.
 * Continuously polls outbox_events table in the database and guarantees at-least-once delivery to Kafka.
 */
@Slf4j
@Component
@DisallowConcurrentExecution
@RequiredArgsConstructor
public class OutboxPollingPublisherJob extends QuartzJobBean {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OrderEventMongoRepository eventMongoRepository;

    private static final int BATCH_SIZE = 50;
    private static final int MAX_RETRY_LIMIT = 5;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        TraceContext.setTraceId(TraceContext.generateTraceId());

        try {
            // Find PENDING outbox events with retryCount < 5 in batches
            List<OutboxEntity> pendingEvents = outboxRepository
                    .findByStatusAndRetryCountLessThanOrderByCreatedAtAsc(
                            OutboxStatus.PENDING,
                            MAX_RETRY_LIMIT,
                            PageRequest.of(0, BATCH_SIZE)
                    );

            if (pendingEvents.isEmpty()) {
                return;
            }

            log.info("[OutboxJob] Polled [{}] pending outbox events for publishing", pendingEvents.size());

            for (OutboxEntity outbox : pendingEvents) {
                publishEvent(outbox);
            }
        } catch (Exception e) {
            log.error("[OutboxJob] Error in OutboxPollingPublisherJob: {}", e.getMessage(), e);
            throw new JobExecutionException(e);
        } finally {
            TraceContext.clear();
        }
    }

    private void publishEvent(OutboxEntity outbox) {
        String eventId = outbox.getEventId();
        String topic = outbox.getTopic();
        String traceId = outbox.getTraceId() != null ? outbox.getTraceId() : TraceContext.getTraceId();

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, outbox.getAggregateId(), outbox.getPayload());
        record.headers().add(new RecordHeader(TraceContext.TRACE_ID_KEY, traceId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(TraceContext.CORRELATION_ID_KEY, traceId.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("eventType", outbox.getEventType().getBytes(StandardCharsets.UTF_8)));

        try {
            kafkaTemplate.send(record).whenComplete((result, ex) -> {
                if (ex == null) {
                    var metadata = result.getRecordMetadata();
                    log.info("[OutboxJob] Published event [{}] -> topic=[{}], partition=[{}], offset=[{}]",
                            eventId, metadata.topic(), metadata.partition(), metadata.offset());

                    outbox.setStatus(OutboxStatus.PUBLISHED);
                    outbox.setProcessedAt(LocalDateTime.now());
                    outboxRepository.save(outbox);

                    // Sync to Mongo Event Store
                    syncToMongo(outbox, metadata.partition(), metadata.offset());
                } else {
                    int nextRetry = outbox.getRetryCount() + 1;
                    log.warn("[OutboxJob] Failed publishing event [{}] (attempt {}/{}): {}",
                            eventId, nextRetry, MAX_RETRY_LIMIT, ex.getMessage());

                    outbox.setRetryCount(nextRetry);
                    outbox.setLastError(ex.getMessage());
                    if (nextRetry >= MAX_RETRY_LIMIT) {
                        outbox.setStatus(OutboxStatus.FAILED);
                        log.error("[OutboxJob] Event [{}] marked as FAILED after {} retries", eventId, MAX_RETRY_LIMIT);
                    }
                    outboxRepository.save(outbox);
                }
            });
        } catch (Exception ex) {
            log.error("[OutboxJob] Synchronous dispatch error for event [{}]: {}", eventId, ex.getMessage());
        }
    }

    private void syncToMongo(OutboxEntity outbox, int partition, long offset) {
        try {
            OrderEventDocument doc = OrderEventDocument.builder()
                    .eventId(outbox.getEventId())
                    .eventType(outbox.getEventType())
                    .aggregateId(outbox.getAggregateId())
                    .topic(outbox.getTopic())
                    .partition(partition)
                    .offset(offset)
                    .payload(outbox.getPayload())
                    .status(EventStatus.PUBLISHED)
                    .traceId(outbox.getTraceId())
                    .createdAt(Instant.now())
                    .processedAt(Instant.now())
                    .build();
            eventMongoRepository.save(doc);
        } catch (Exception e) {
            log.warn("[OutboxJob] Failed to replicate to Mongo for event [{}]: {}", outbox.getEventId(), e.getMessage());
        }
    }
}
