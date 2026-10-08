package org.swisspush.redisques.queue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Computes an optimal plan to rebalance queues across consumers.
 *
 * The rebalancer attempts to distribute queues evenly among active consumers
 * while respecting the maximum number of moves per run to avoid excessive churn.
 */
public class QueueRebalancePlanner {

    /**
     * Computes a rebalancing plan from a map of queues by consumer.
     *
     * @param queuesByConsumer map of consumer IDs to their lists of queues
     * @param maxMovesPerRun maximum number of queue moves allowed in the plan
     * @return a Plan containing the moves to rebalance queues across consumers
     */
    public Plan computePlan(Map<String, List<String>> queuesByConsumer, int maxMovesPerRun) {
        if (queuesByConsumer == null || queuesByConsumer.isEmpty()) {
            return new Plan(List.of(), 0, 0);
        }

        return computePlan(loadsFromQueues(queuesByConsumer), queuesByConsumer, maxMovesPerRun);
    }

    /**
     * Computes a rebalancing plan from a map of consumer loads and movable queues.
     *
     * This method distributes the total queue load evenly among active consumers,
     * assigning higher target loads to consumers with more initial load. It then
     * generates moves from over-subscribed (surplus) consumers to under-subscribed
     * (deficit) consumers until the maximum moves per run is reached or balance
     * is achieved.
     *
     * @param loadByConsumer map of consumer IDs to their current queue counts
     * @param movableQueuesByConsumer map of consumer IDs to lists of queues they can move
     * @param maxMovesPerRun maximum number of queue moves allowed in the plan
     * @return a Plan containing the moves to rebalance queues across consumers
     */
    public Plan computePlan(Map<String, Integer> loadByConsumer, Map<String, List<String>> movableQueuesByConsumer, int maxMovesPerRun) {
        if ((loadByConsumer == null || loadByConsumer.isEmpty())
                && (movableQueuesByConsumer == null || movableQueuesByConsumer.isEmpty())) {
            return new Plan(List.of(), 0, 0);
        }

        // Build a list of consumer states with their current load and movable queues
        List<ConsumerState> consumers = consumerIds(loadByConsumer, movableQueuesByConsumer).stream()
                .map(consumerId -> new ConsumerState(consumerId, loadForConsumer(loadByConsumer, consumerId),
                        sortedQueues(movableQueuesByConsumer == null ? null : movableQueuesByConsumer.get(consumerId))))
                .sorted(Comparator.comparing(ConsumerState::consumerId))
                .collect(Collectors.toList());

        int activeConsumers = consumers.size();
        if (activeConsumers == 0) {
            return new Plan(List.of(), 0, 0);
        }

        // Calculate total queues across all consumers
        int totalQueues = consumers.stream().mapToInt(ConsumerState::load).sum();
        if (totalQueues == 0) {
            return new Plan(List.of(), 0, activeConsumers);
        }

        // If moves are not permitted, return a plan indicating no moves
        if (maxMovesPerRun <= 0) {
            return new Plan(List.of(), totalQueues, activeConsumers);
        }

        // Calculate target load for each consumer: baseTarget + (1 for some to distribute remainder)
        int baseTarget = totalQueues / activeConsumers;
        int remainder = totalQueues % activeConsumers;
        int[] targets = new int[activeConsumers];
        int[] currentLoads = new int[activeConsumers];
        for (int i = 0; i < activeConsumers; i++) {
            targets[i] = baseTarget;
            currentLoads[i] = consumers.get(i).load();
        }

        // Determine which consumers get +1 to their target: prioritize those already holding more queues
        List<Integer> targetPriority = new ArrayList<>();
        for (int i = 0; i < activeConsumers; i++) {
            targetPriority.add(i);
        }
        targetPriority.sort(Comparator
                .comparingInt((Integer index) -> currentLoads[index]).reversed()
                .thenComparing(index -> consumers.get(index).consumerId()));
        for (int i = 0; i < remainder; i++) {
            targets[targetPriority.get(i)]++;
        }

        // Generate moves from surplus consumers (above target) to deficit consumers (below target)
        List<Move> moves = new ArrayList<>();
        int donorIndex = 0;
        while (moves.size() < maxMovesPerRun) {
            donorIndex = nextSurplusIndex(consumers, currentLoads, targets, donorIndex);
            int receiverIndex = nextDeficitIndex(currentLoads, targets);
            if (donorIndex < 0 || receiverIndex < 0) {
                break;
            }

            ConsumerState donor = consumers.get(donorIndex);
            ConsumerState receiver = consumers.get(receiverIndex);
            String queueName = donor.removeNextQueueName();
            if (queueName == null) {
                donorIndex++;
                continue;
            }
            moves.add(new Move(queueName, donor.consumerId, receiver.consumerId));
            currentLoads[donorIndex]--;
            currentLoads[receiverIndex]++;
        }

        return new Plan(List.copyOf(moves), totalQueues, activeConsumers);
    }

    /**
     * Finds the next consumer with surplus queues (current load > target load)
     * that has movable queues available, starting from the given index.
     *
     * @param consumers list of consumer states
     * @param currentLoads current load for each consumer index
     * @param targets target load for each consumer index
     * @param startIndex index to start searching from
     * @return the index of the next surplus consumer, or -1 if none found
     */
    private static int nextSurplusIndex(List<ConsumerState> consumers, int[] currentLoads, int[] targets, int startIndex) {
        for (int i = Math.max(0, startIndex); i < currentLoads.length; i++) {
            if (currentLoads[i] > targets[i] && consumers.get(i).hasMovableQueues()) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Finds the next consumer with a deficit (current load < target load).
     * Prioritizes consumers with the lowest current load; breaks ties by index.
     *
     * @param currentLoads current load for each consumer index
     * @param targets target load for each consumer index
     * @return the index of the next deficit consumer, or -1 if none found
     */
    private static int nextDeficitIndex(int[] currentLoads, int[] targets) {
        int selectedIndex = -1;
        for (int i = 0; i < currentLoads.length; i++) {
            if (currentLoads[i] < targets[i]) {
                if (selectedIndex < 0
                        || currentLoads[i] < currentLoads[selectedIndex]
                        || (currentLoads[i] == currentLoads[selectedIndex]
                        && i < selectedIndex)) {
                    selectedIndex = i;
                }
            }
        }
        return selectedIndex;
    }

    /**
     * Computes the load (queue count) for each consumer from a map of queues.
     *
     * @param queuesByConsumer map of consumer IDs to their lists of queues
     * @return map of consumer IDs to their queue counts
     */
    private static Map<String, Integer> loadsFromQueues(Map<String, List<String>> queuesByConsumer) {
        return queuesByConsumer.entrySet().stream()
                .filter(entry -> entry.getKey() != null)
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> sortedQueues(entry.getValue()).size(),
                        (left, right) -> left,
                        LinkedHashMap::new));
    }

    /**
     * Extracts the unique set of consumer IDs from both load and movable queue maps.
     *
     * @param loadByConsumer map of consumer IDs to their loads (may be null)
     * @param movableQueuesByConsumer map of consumer IDs to their movable queues (may be null)
     * @return list of unique consumer IDs
     */
    private static List<String> consumerIds(Map<String, Integer> loadByConsumer, Map<String, List<String>> movableQueuesByConsumer) {
        return java.util.stream.Stream.concat(
                        loadByConsumer == null ? java.util.stream.Stream.empty() : loadByConsumer.keySet().stream(),
                        movableQueuesByConsumer == null ? java.util.stream.Stream.empty() : movableQueuesByConsumer.keySet().stream())
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Retrieves the load for a specific consumer, defaulting to 0 if not found.
     *
     * @param loadByConsumer map of consumer IDs to their loads (may be null)
     * @param consumerId the consumer to look up
     * @return the load for this consumer, or 0 if not present or null
     */
    private static int loadForConsumer(Map<String, Integer> loadByConsumer, String consumerId) {
        if (loadByConsumer == null) {
            return 0;
        }
        Integer load = loadByConsumer.get(consumerId);
        return load == null ? 0 : Math.max(load, 0);
    }

    /**
     * Sorts and filters queue names, removing nulls and duplicates.
     *
     * @param queueNames the list of queue names to sort
     * @return a sorted list of non-null queue names
     */
    private static List<String> sortedQueues(List<String> queueNames) {
        if (queueNames == null || queueNames.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> result = queueNames.stream()
                .filter(Objects::nonNull)
                .sorted()
                .collect(Collectors.toList());
        return new ArrayList<>(result);
    }

    /**
     * Represents the state of a consumer during rebalancing.
     * Tracks the consumer ID, current load, and a list of movable queue names.
     */
    private static final class ConsumerState {
        private final String consumerId;
        private final int load;
        private final List<String> queueNames;

        private ConsumerState(String consumerId, int load, List<String> queueNames) {
            this.consumerId = consumerId;
            this.load = load;
            this.queueNames = queueNames;
        }

        private String consumerId() {
            return consumerId;
        }

        private int load() {
            return load;
        }

        private boolean hasMovableQueues() {
            return !queueNames.isEmpty();
        }

        private String removeNextQueueName() {
            if (queueNames.isEmpty()) {
                return null;
            }
            return queueNames.remove(0);
        }
    }

    /**
     * Represents a single queue move operation.
     * Specifies a queue to move from one consumer (source) to another (target).
     */
    public static final class Move {
        private final String queueName;
        private final String sourceConsumerId;
        private final String targetConsumerId;

        public Move(String queueName, String sourceConsumerId, String targetConsumerId) {
            this.queueName = Objects.requireNonNull(queueName, "queueName");
            this.sourceConsumerId = Objects.requireNonNull(sourceConsumerId, "sourceConsumerId");
            this.targetConsumerId = Objects.requireNonNull(targetConsumerId, "targetConsumerId");
        }

        public String queueName() {
            return queueName;
        }

        public String getQueueName() {
            return queueName();
        }

        public String sourceConsumerId() {
            return sourceConsumerId;
        }

        public String getSourceConsumerId() {
            return sourceConsumerId();
        }

        public String targetConsumerId() {
            return targetConsumerId;
        }

        public String getTargetConsumerId() {
            return targetConsumerId();
        }
    }

    /**
     * Represents a rebalancing plan containing the moves to execute and metadata.
     */
    public static final class Plan {
        private final List<Move> moves;
        private final int totalQueues;
        private final int activeConsumers;

        public Plan(List<Move> moves, int totalQueues, int activeConsumers) {
            this.moves = List.copyOf(Objects.requireNonNull(moves, "moves"));
            this.totalQueues = totalQueues;
            this.activeConsumers = activeConsumers;
        }

        public List<Move> moves() {
            return moves;
        }

        public List<Move> getMoves() {
            return moves();
        }

        public int totalQueues() {
            return totalQueues;
        }

        public int getTotalQueues() {
            return totalQueues();
        }

        public int activeConsumers() {
            return activeConsumers;
        }

        public int getActiveConsumers() {
            return activeConsumers();
        }
    }
}
