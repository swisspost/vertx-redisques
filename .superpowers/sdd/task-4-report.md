Status: DONE

Summary:
- Implemented `RebalanceQueuesAction` with payload validation, queue-scope lookup, running-state collection, planner input building, dry-run response handling, and bounded execution with per-queue skip reasons.
- Added focused unit tests covering dry-run success, invalid `maxMovesPerRun`, successful execution, and partial skip behavior.

Files changed:
- `src/main/java/org/swisspush/redisques/action/RebalanceQueuesAction.java`
- `src/test/java/org/swisspush/redisques/action/RebalanceQueuesActionTest.java`

Tests:
- `mvn -q "-Dtest=RebalanceQueuesActionTest" test`
- `mvn -q "-Dtest=QueueRebalancePlannerTest" test`
- `mvn -q "-Dtest=RebalanceQueuesActionTest,QueueRebalancePlannerTest" test`

Commit:
- `2233632a6dad07b7016db95d133f3078d43069f0`

Review fixes (2026-08-10):
- Changed rebalance handoff protocol so `RebalanceQueuesAction` asks the source consumer to release first, then asks the planned target consumer to claim the queue through the rebalance control channel.
- Added target-side claim handling in `QueueConsumerRunner` so successful moves update both Redis ownership and the target consumer's local queue state before consumption resumes.
- Filtered planner input to `READY` queues only, avoiding wasted move budget on non-ready queues.
- Strengthened `RebalanceQueuesActionTest` to model real ownership transitions and assert release-before-claim ordering instead of stubbing release success independently of ownership state.

Additional validation:
- `mvn -q "-Dtest=RebalanceQueuesActionTest,QueueRebalancePlannerTest" test` (PASS)
- `mvn -q "-Dtest=QueueConsumerRunnerTest#releaseQueueIfReadyAndOwnedControlPath_ReleasesReadyQueue+releaseQueueIfReadyAndOwnedControlPath_DoesNotReleaseNonReadyQueue+releaseQueueIfReadyAndOwned_IsIdempotent" test` (PASS)
