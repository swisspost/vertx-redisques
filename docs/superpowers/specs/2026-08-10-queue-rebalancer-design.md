# Queue Rebalancer Design (Host-Triggered)

## Goal

Provide a host-triggered Redisques operation that redistributes queue ownership across active verticles so queue counts are balanced as evenly as possible.

## Scope

- Manual trigger from host app via Redisques event-bus operation.
- Move queue **ownership only** (same Redis data, no cross-cluster data copy).
- Move only queues in `READY` state.
- Auto-pick least-loaded target verticle.

Out of scope:

- Continuous autonomous balancing loops.
- Moving `CONSUMING` queues.
- Queue payload migration between Redis clusters.

## API Contract

### New operation

- `operation`: `rebalanceQueues`
- `payload`:
  - `filter` (optional string): queue-name filter regex/pattern, same semantics as existing filtered operations.
  - `dryRun` (optional boolean, default `false`): compute plan without executing moves.
  - `maxMovesPerRun` (optional int, default `100`, hard-capped): upper bound on executed moves in one request.

### Response

On success:

```json
{
  "status": "ok",
  "value": {
    "plannedMoves": 12,
    "executedMoves": 10,
    "skipped": 2,
    "reasonsByQueue": {
      "queue-a": "owner-changed",
      "queue-b": "not-ready"
    }
  }
}
```

On failure:

```json
{
  "status": "error",
  "message": "..."
}
```

## Architecture Changes

1. Extend `RedisquesAPI.QueueOperation` with `rebalanceQueues`.
2. Add `buildRebalanceQueuesOperation(...)` helper in `RedisquesAPI`.
3. Wire new action in:
   - `QueueActionFactory`
   - `QueueActionsService`
4. Implement `RebalanceQueuesAction` to orchestrate planning and execution.

## Required Running-State Metadata

Current running-state aggregation returns queue maps but not sender identity. Rebalancing needs per-verticle counts, so running-state replies must include:

- `consumerId` (verticle uid)
- `queues` (existing queue processing states map)

This can be done by wrapping current payload entries in an object per verticle response.

## Rebalancing Algorithm

1. Fetch filtered queue universe.
2. Fetch running states from all verticles.
3. Build load map: `consumerId -> ownedQueueCount` for queues in scope.
4. Compute target balance:
   - Let `Q` = total scoped queues, `V` = active verticles.
   - Ideal base = `Q / V`, with remainder distributed by +1.
5. Build donor list (above target) and receiver list (below target).
6. Generate move plan donor->receiver until balanced or `maxMovesPerRun` reached.
7. Prefer queues in `READY` state; skip others.

## Hard Handoff Protocol (per queue)

For each planned move `queue: source -> target`:

1. Verify source still owns the queue (read current consumer key).
2. Verify queue is still `READY` per latest known state (or skip if stale/unknown).
3. Set consumer key to target with normal registration TTL.
4. Send internal release command to source verticle to drop local `myQueues` ownership state.
5. Notify target verticle to consume.
6. Record success/skip reason in result.

If any check fails, skip that queue and continue (partial success model).

## Internal Coordination Additions

Add an internal event-bus command for source verticle release, e.g.:

- Address: internal queue-control address derived from `KeyspaceHelper`.
- Payload: `{action:"releaseQueue", queue:"...", expectedOwner:"..."}`

Release is idempotent and safe:

- If source no longer owns queue locally, treat as no-op success.
- If expected owner mismatch, return skip reason.

## Error Handling

- Validation errors: bad input (`maxMovesPerRun <= 0`, invalid filter) -> `status=error`.
- Upstream timeouts/failures (running-state or redis call failures): `status=error` with message.
- Per-queue race failures during execution: not fatal for whole run; tracked as skipped with reason.
- No eligible moves found: `status=ok` with `plannedMoves=0`.

## Concurrency and Safety

- Single request should be bounded by `maxMovesPerRun`.
- Prefer reusing existing quota/retry patterns where available.
- Execution is best-effort under concurrent ownership churn; correctness relies on per-queue ownership re-check before handoff.
- Never move `CONSUMING` queues.

## Testing Strategy

1. Unit tests (planner):
   - Even distribution math (exact and remainder cases).
   - Cap enforcement.
   - Empty/single-verticle scenarios.
2. Action tests:
   - Dry-run output.
   - Successful move execution path.
   - Partial execution with skip reasons (`owner-changed`, `not-ready`).
   - Input validation errors.
3. Integration tests:
   - Multi-verticle setup where one verticle is overloaded.
   - Trigger rebalance and verify ownership counts become balanced.
   - Verify only `READY` queues are moved.

## Rollout Notes

- Default behavior remains unchanged unless host triggers `rebalanceQueues`.
- Feature is backward compatible for existing operations.
