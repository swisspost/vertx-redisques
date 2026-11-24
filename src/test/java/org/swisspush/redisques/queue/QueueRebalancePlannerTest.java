package org.swisspush.redisques.queue;

import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueueRebalancePlannerTest {

    @Test
    public void computePlan_BalancesOverloadedAndUnderloaded() {
        Map<String, List<String>> input = new LinkedHashMap<>();
        input.put("A", List.of("q1", "q2", "q3", "q4"));
        input.put("B", List.of("q5"));
        input.put("C", List.of("q6"));

        QueueRebalancePlanner.Plan plan = new QueueRebalancePlanner().computePlan(input, 10);

        assertEquals(2, plan.getMoves().size());
        assertEquals(6, plan.getTotalQueues());
        assertEquals(3, plan.getActiveConsumers());
        assertEquals("q1", plan.getMoves().get(0).getQueueName());
        assertEquals("A", plan.getMoves().get(0).getSourceConsumerId());
        assertEquals("B", plan.getMoves().get(0).getTargetConsumerId());
        assertEquals("q2", plan.getMoves().get(1).getQueueName());
        assertEquals("A", plan.getMoves().get(1).getSourceConsumerId());
        assertEquals("C", plan.getMoves().get(1).getTargetConsumerId());
    }

    @Test
    public void computePlan_UsesEvenDistributionWithRemainder() {
        Map<String, List<String>> input = new LinkedHashMap<>();
        input.put("A", List.of("q1", "q2", "q3"));
        input.put("B", List.of("q4"));
        input.put("C", List.of("q5"));

        QueueRebalancePlanner.Plan plan = new QueueRebalancePlanner().computePlan(input, 10);

        assertEquals(1, plan.getMoves().size());
        assertEquals("q1", plan.getMoves().get(0).getQueueName());
        assertEquals("A", plan.getMoves().get(0).getSourceConsumerId());
        assertEquals("B", plan.getMoves().get(0).getTargetConsumerId());
    }

    @Test
    public void computePlan_AlreadyBalancedProducesNoMoves() {
        Map<String, List<String>> input = new LinkedHashMap<>();
        input.put("A", List.of("q1", "q2"));
        input.put("B", List.of("q3", "q4"));

        QueueRebalancePlanner.Plan plan = new QueueRebalancePlanner().computePlan(input, 10);

        assertTrue(plan.getMoves().isEmpty());
        assertEquals(4, plan.getTotalQueues());
        assertEquals(2, plan.getActiveConsumers());
    }

    @Test
    public void computePlan_AlreadyBalancedRemainderProducesNoMoves() {
        Map<String, List<String>> input = new LinkedHashMap<>();
        input.put("A", List.of("q1"));
        input.put("B", List.of("q2", "q3"));
        input.put("C", List.of("q4", "q5"));

        QueueRebalancePlanner.Plan plan = new QueueRebalancePlanner().computePlan(input, 10);

        assertTrue(plan.getMoves().isEmpty());
        assertEquals(5, plan.getTotalQueues());
        assertEquals(3, plan.getActiveConsumers());
    }

    @Test
    public void computePlan_EmptyInputProducesEmptyPlan() {
        QueueRebalancePlanner.Plan plan = new QueueRebalancePlanner().computePlan(Map.of(), 10);

        assertTrue(plan.getMoves().isEmpty());
        assertEquals(0, plan.getTotalQueues());
        assertEquals(0, plan.getActiveConsumers());
    }

    @Test
    public void computePlan_SingleConsumerProducesNoMoves() {
        Map<String, List<String>> input = Map.of("A", List.of("q1", "q2", "q3"));

        QueueRebalancePlanner.Plan plan = new QueueRebalancePlanner().computePlan(input, 10);

        assertTrue(plan.getMoves().isEmpty());
        assertEquals(3, plan.getTotalQueues());
        assertEquals(1, plan.getActiveConsumers());
    }

    @Test
    public void computePlan_RespectsMaxMovesPerRun() {
        Map<String, List<String>> input = new LinkedHashMap<>();
        input.put("A", List.of("q1", "q2", "q3", "q4", "q5"));
        input.put("B", List.of());
        input.put("C", List.of());

        QueueRebalancePlanner.Plan plan = new QueueRebalancePlanner().computePlan(input, 1);

        assertEquals(1, plan.getMoves().size());
        assertEquals("q1", plan.getMoves().get(0).getQueueName());
        assertEquals("A", plan.getMoves().get(0).getSourceConsumerId());
        assertEquals("B", plan.getMoves().get(0).getTargetConsumerId());
    }

    @Test
    public void computePlan_ReselectsLeastLoadedReceiverForEachMove() {
        Map<String, List<String>> input = new LinkedHashMap<>();
        input.put("A", List.of("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8"));
        input.put("B", List.of("q9", "q10", "q11"));
        input.put("C", List.of());
        input.put("D", List.of());

        QueueRebalancePlanner.Plan plan = new QueueRebalancePlanner().computePlan(input, 2);

        assertEquals(2, plan.getMoves().size());
        assertEquals("q1", plan.getMoves().get(0).getQueueName());
        assertEquals("A", plan.getMoves().get(0).getSourceConsumerId());
        assertEquals("C", plan.getMoves().get(0).getTargetConsumerId());
        assertEquals("q2", plan.getMoves().get(1).getQueueName());
        assertEquals("A", plan.getMoves().get(1).getSourceConsumerId());
        assertEquals("D", plan.getMoves().get(1).getTargetConsumerId());
    }

    @Test
    public void computePlan_ZeroMaxMovesKeepsMetadata() {
        Map<String, List<String>> input = new LinkedHashMap<>();
        input.put("A", List.of("q1", "q2"));
        input.put("B", List.of("q3"));

        QueueRebalancePlanner.Plan plan = new QueueRebalancePlanner().computePlan(input, 0);

        assertTrue(plan.getMoves().isEmpty());
        assertEquals(3, plan.getTotalQueues());
        assertEquals(2, plan.getActiveConsumers());
    }

    @Test
    public void planAndMoveExposeGetterStyleApi() {
        QueueRebalancePlanner.Move move = new QueueRebalancePlanner.Move("q1", "A", "B");
        QueueRebalancePlanner.Plan plan = new QueueRebalancePlanner.Plan(List.of(move), 1, 1);

        assertEquals(move, plan.getMoves().get(0));
        assertEquals("q1", move.getQueueName());
        assertEquals("A", move.getSourceConsumerId());
        assertEquals("B", move.getTargetConsumerId());
        assertEquals(1, plan.getTotalQueues());
        assertEquals(1, plan.getActiveConsumers());
    }
}
