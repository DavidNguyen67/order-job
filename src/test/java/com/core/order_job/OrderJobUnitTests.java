package com.core.order_job;

import com.core.order_job.config.QuartzConfig;
import org.junit.jupiter.api.Test;
import org.quartz.JobDetail;
import org.quartz.Trigger;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class OrderJobUnitTests {

    @Test
    void testQuartzConfigDefinitions() {
        QuartzConfig config = new QuartzConfig();

        JobDetail outboxJob = config.outboxJobDetail();
        Trigger outboxTrigger = config.outboxJobTrigger(outboxJob);
        assertNotNull(outboxJob);
        assertNotNull(outboxTrigger);

        JobDetail failedJob = config.failedEventsJobDetail();
        Trigger failedTrigger = config.failedEventsJobTrigger(failedJob);
        assertNotNull(failedJob);
        assertNotNull(failedTrigger);

        assertNotNull(config.cleanupJobDetail());
        assertNotNull(config.cleanupJobTrigger(config.cleanupJobDetail()));
    }
}
