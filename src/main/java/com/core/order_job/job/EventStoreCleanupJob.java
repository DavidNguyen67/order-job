package com.core.order_job.job;

import com.core.order_common.enums.IdempotencyStatus;
import com.core.order_common.util.TraceContext;
import com.core.order_model.document.IdempotencyRecordDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Quartz Job to clean up old idempotency records and expired locks.
 */
@Slf4j
@Component
@DisallowConcurrentExecution
@RequiredArgsConstructor
public class EventStoreCleanupJob extends QuartzJobBean {

    private final MongoTemplate mongoTemplate;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        TraceContext.setTraceId(TraceContext.generateTraceId());
        log.info("[QuartzJob] Starting EventStoreCleanupJob execution...");

        try {
            // Remove completed idempotency records older than 7 days
            Instant sevenDaysAgo = Instant.now().minusSeconds(7 * 86400L);
            Query query = new Query(
                    Criteria.where("status").is(IdempotencyStatus.COMPLETED)
                            .and("createdAt").lte(sevenDaysAgo)
            );
            var result = mongoTemplate.remove(query, IdempotencyRecordDocument.class);
            log.info("[QuartzJob] Cleaned up [{}] expired idempotency records.", result.getDeletedCount());
        } catch (Exception e) {
            log.error("[QuartzJob] Error in EventStoreCleanupJob: {}", e.getMessage(), e);
        } finally {
            TraceContext.clear();
        }
    }
}
