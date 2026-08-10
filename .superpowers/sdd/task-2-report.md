Status: DONE_WITH_CONCERNS

Task: Task 2 - Add running-state metadata and source-release control path

Commit:
- 98cfb65

What was implemented:
- Running-state payload entries now use `{consumerId, queues}`.
- Added rebalance-control address support in `KeyspaceHelper`.
- Added guarded, idempotent release hook for locally owned READY queues in `QueueConsumerRunner`.
- Updated running-state tests to assert new payload shape.
- Updated directly impacted queue-size/statistics tests for new running-state payload format.

Tests run:
- `mvn -q "-Dtest=GetQueueRunningStatesActionTest,QueueConsumerRunnerTest#releaseQueueIfReadyAndOwnedControlPath_ReleasesReadyQueue+releaseQueueIfReadyAndOwnedControlPath_DoesNotReleaseNonReadyQueue+releaseQueueIfReadyAndOwned_IsIdempotent,QueueStatisticsCollectorTest#testGetAllApproximateQueueSize_TimestampZeroShouldNotOverwriteNewerData+testGetAllApproximateQueueSize_NewerTimestampWins" test` (PASS)

Concern:
- Targeted tests passed, but teardown emitted noisy Vert.x/Redis shutdown SEVERE logs.

---

Fix follow-up:
- `QueueConsumerRunner.releaseQueueIfReadyAndOwned(...)` now refreshes and verifies the Redis consumer registration, deletes the Redis ownership key, and only then removes local READY ownership state.
- `QueueConsumerRunnerTest` now verifies release/non-release semantics against the Redis consumer key in addition to local state.
- Running-state tests were tightened to avoid blocking `Thread.sleep(...)` delays and to use shorter timer windows for timeout/expected-reply coverage.

Focused verification:
- `mvn -q "-Dtest=QueueConsumerRunnerTest#releaseQueueIfReadyAndOwnedControlPath_ReleasesReadyQueue+releaseQueueIfReadyAndOwnedControlPath_DoesNotReleaseNonReadyQueue+releaseQueueIfReadyAndOwned_IsIdempotent,GetQueueRunningStatesActionTest,QueueStatisticsCollectorTest#testGetAllApproximateQueueSize_TimestampZeroShouldNotOverwriteNewerData+testGetAllApproximateQueueSize_NewerTimestampWins" test` (PASS, with existing noisy Vert.x/Redis shutdown logs during teardown)
