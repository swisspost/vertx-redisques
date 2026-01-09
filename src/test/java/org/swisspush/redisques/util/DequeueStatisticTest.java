package org.swisspush.redisques.util;

import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class DequeueStatisticTest {

    @Test
    public void testDefaultConstructor_initialState() {
        DequeueStatistic stat = new DequeueStatistic("queueName");

        assertTrue(stat.isEmpty());
        assertNull(stat.getLastDequeueAttemptTimestamp());
        assertNull(stat.getLastDequeueSuccessTimestamp());
        assertNull(stat.getNextDequeueDueTimestamp());
        assertNull(stat.getFailedReason());
        assertFalse(stat.isMarkedForRemoval());

        assertNotNull(stat.getLastUpdatedTimestamp());
    }

    @Test
    public void testParameterizedConstructor_initializesFields() {
        String queueName = "queueName";
        long lastAtt = 10L;
        long lastSuc = 20L;
        long nextDue = 30L;
        long lastUpd = 40L;
        String reason = "some reason";
        boolean mark = true;

        DequeueStatistic stat = new DequeueStatistic(
                queueName, lastAtt, lastSuc, nextDue, lastUpd, reason, mark
        );

        assertEquals(queueName, stat.getQueueName());
        assertEquals(Long.valueOf(lastAtt), stat.getLastDequeueAttemptTimestamp());
        assertEquals(Long.valueOf(lastSuc), stat.getLastDequeueSuccessTimestamp());
        assertEquals(Long.valueOf(nextDue), stat.getNextDequeueDueTimestamp());
        assertEquals(Long.valueOf(lastUpd), stat.getLastUpdatedTimestamp());
        assertEquals(reason, stat.getFailedReason());
        assertTrue(stat.isMarkedForRemoval());
        assertFalse(stat.isEmpty());
    }

    @Test
    public void testIsEmpty_falseWhenAnyTimestampSet() {
        DequeueStatistic stat = new DequeueStatistic("queueName");
        assertTrue(stat.isEmpty());

        stat.setLastDequeueAttemptTimestamp(123L);
        assertFalse(stat.isEmpty());
    }

    @Test
    public void testSetLastDequeueAttemptTimestamp_updatesFieldAndLastUpdated() throws Exception {
        DequeueStatistic stat = new DequeueStatistic("queueName");

        long before = System.currentTimeMillis();
        stat.setLastDequeueAttemptTimestamp(100L);

        assertEquals(Long.valueOf(100L), stat.getLastDequeueAttemptTimestamp());
        assertNotNull(stat.getLastUpdatedTimestamp());
        assertTrue(stat.getLastUpdatedTimestamp() >= before);
    }

    @Test
    public void testSetLastDequeueSuccessTimestamp_updatesFieldClearsNextDueAndReason() {
        DequeueStatistic stat = new DequeueStatistic("queueName");

        stat.setNextDequeueDueTimestamp(200L, "backoff");
        assertEquals(Long.valueOf(200L), stat.getNextDequeueDueTimestamp());
        assertEquals("backoff", stat.getFailedReason());

        stat.setLastDequeueSuccessTimestamp(300L);

        assertEquals(Long.valueOf(300L), stat.getLastDequeueSuccessTimestamp());
        assertNull(stat.getNextDequeueDueTimestamp());
        assertNull(stat.getFailedReason());
        assertNotNull(stat.getLastUpdatedTimestamp());
    }

    @Test
    public void testSetNextDequeueDueTimestamp_updatesFieldsAndLastUpdated() {
        DequeueStatistic stat = new DequeueStatistic("queueName");

        long before = System.currentTimeMillis();
        stat.setNextDequeueDueTimestamp(400L, "rate-limit");

        assertEquals(Long.valueOf(400L), stat.getNextDequeueDueTimestamp());
        assertEquals("rate-limit", stat.getFailedReason());
        assertNotNull(stat.getLastUpdatedTimestamp());
        assertTrue(stat.getLastUpdatedTimestamp() >= before);
    }

    @Test
    public void testSetMarkedForRemoval_setsFlagAndLastUpdated() {
        DequeueStatistic stat = new DequeueStatistic("queueName");

        long before = System.currentTimeMillis();
        stat.setMarkedForRemoval();

        assertTrue(stat.isMarkedForRemoval());
        assertNotNull(stat.getLastUpdatedTimestamp());
        assertTrue(stat.getLastUpdatedTimestamp() >= before);
    }

    @Test
    public void testAsJson_onDefaultInstance_onlyContainsMarkForRemovalFalse() {
        DequeueStatistic stat = new DequeueStatistic("queueName");

        JsonObject json = stat.asJson();

        // boolean always present
        assertTrue(json.containsKey(DequeueStatistic.KEY_MARK_FOR_REMOVAL));
        assertFalse(json.getBoolean(DequeueStatistic.KEY_MARK_FOR_REMOVAL));
        assertTrue(json.containsKey(DequeueStatistic.KEY_QUEUENAME));
        assertTrue(json.containsKey(DequeueStatistic.KEY_LAST_UPDATED_TS));

        // others should not be present because they are null
        assertFalse(json.containsKey(DequeueStatistic.KEY_LAST_DEQUEUE_ATTEMPT_TS));
        assertFalse(json.containsKey(DequeueStatistic.KEY_LAST_DEQUEUE_SUCCESS_TS));
        assertFalse(json.containsKey(DequeueStatistic.KEY_NEXT_DEQUEUE_DUE_TS));
        assertFalse(json.containsKey(DequeueStatistic.KEY_FAILED_REASON));
    }

    @Test
    public void testAsJson_includesNonNullFields() {
        String queueName = "queueName";
        long lastAtt = 10L;
        long lastSuc = 20L;
        long nextDue = 30L;
        long lastUpd = 40L;
        String reason = "failure";
        boolean mark = true;

        DequeueStatistic stat = new DequeueStatistic(
                queueName, lastAtt, lastSuc, nextDue, lastUpd, reason, mark
        );

        JsonObject json = stat.asJson();

        assertEquals(Long.valueOf(lastAtt), json.getLong(DequeueStatistic.KEY_LAST_DEQUEUE_ATTEMPT_TS));
        assertEquals(Long.valueOf(lastSuc), json.getLong(DequeueStatistic.KEY_LAST_DEQUEUE_SUCCESS_TS));
        assertEquals(Long.valueOf(nextDue), json.getLong(DequeueStatistic.KEY_NEXT_DEQUEUE_DUE_TS));
        assertEquals(Long.valueOf(lastUpd), json.getLong(DequeueStatistic.KEY_LAST_UPDATED_TS));
        assertEquals(reason, json.getString(DequeueStatistic.KEY_FAILED_REASON));
        assertEquals(queueName, json.getString(DequeueStatistic.KEY_QUEUENAME));
        assertTrue(json.getBoolean(DequeueStatistic.KEY_MARK_FOR_REMOVAL));
    }

    @Test
    public void testFromJson_roundTrip() {
        DequeueStatistic original = new DequeueStatistic("queueName");
        original.setLastDequeueAttemptTimestamp(111L);
        original.setNextDequeueDueTimestamp(222L, "some reason");
        original.setMarkedForRemoval(); // also updates lastUpdatedTimestamp

        JsonObject json = original.asJson();
        DequeueStatistic copy = DequeueStatistic.fromJson(json);

        assertEquals(original.getLastDequeueAttemptTimestamp(), copy.getLastDequeueAttemptTimestamp());
        assertEquals(original.getLastDequeueSuccessTimestamp(), copy.getLastDequeueSuccessTimestamp());
        assertEquals(original.getNextDequeueDueTimestamp(), copy.getNextDequeueDueTimestamp());
        assertEquals(original.getFailedReason(), copy.getFailedReason());
        assertEquals(original.isMarkedForRemoval(), copy.isMarkedForRemoval());

        // lastUpdatedTimestamp is time-based, but should be preserved by asJson/fromJson
        assertEquals(original.getLastUpdatedTimestamp(), copy.getLastUpdatedTimestamp());
    }

    @Test
    public void testFromJson_missingMarkForRemoval_defaultsToFalse() {
        JsonObject json = new JsonObject()
                .put(DequeueStatistic.KEY_LAST_DEQUEUE_ATTEMPT_TS, 123L);

        DequeueStatistic stat = DequeueStatistic.fromJson(json);

        assertEquals(Long.valueOf(123L), stat.getLastDequeueAttemptTimestamp());
        assertFalse(stat.isMarkedForRemoval());
    }

    @Test
    public void testFromJson_withMinimalJson() {
        JsonObject json = new JsonObject()
                .put(DequeueStatistic.KEY_MARK_FOR_REMOVAL, true);

        DequeueStatistic stat = DequeueStatistic.fromJson(json);

        assertNull(stat.getLastDequeueAttemptTimestamp());
        assertNull(stat.getLastDequeueSuccessTimestamp());
        assertNull(stat.getNextDequeueDueTimestamp());
        assertNull(stat.getLastUpdatedTimestamp());
        assertNull(stat.getFailedReason());
        assertTrue(stat.isMarkedForRemoval());
        assertTrue(stat.isEmpty());
    }
}
