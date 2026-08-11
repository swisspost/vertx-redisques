# AGENTS.md — vertx-redisques

## Build & test

```bash
mvn clean install                          # full build + all tests
mvn test -Dtest=EnqueueActionTest          # single test class
mvn test -Dtest=EnqueueActionTest#testXyz  # single method
mvn install -Dmaven.javadoc.skip=true -B   # CI-style (skips javadoc)
```

Java 11 required (enforced by CI and `maven-compiler-plugin`).

## Integration tests require a local Redis

Tests in `RedisQuesTest`, `RedisQuesProcessorTest`, `QueueRegistryServiceTest`, `DequeueStatisticCollectorTest`, and others under `AbstractTestCase` connect to **Redis on `localhost:6379`**. Without it they fail with `Connection refused`. This is expected — these are not broken tests.

Unit tests under `src/test/java/…/action/` and `…/util/` use mocks only and run without Redis.

## Architecture

- **`RedisQues`** — the Vert.x `AbstractVerticle` entry point. Wired via `RedisQuesBuilder`.
- **`RedisQues.initialize()`** — called inside `start()` after Redis connects. Creates all services: `QueueMetrics`, `QueueRegistryService`, `QueueActionsService`.
- **`QueueActionsService` → `QueueActionFactory`** — dispatches event-bus operations to action classes (`EnqueueAction`, `LockedEnqueueAction`, etc.) in `src/main/java/…/action/`.
- **`QueueRegistryService`** — owns consumer lifecycle, queue timestamp tracking, `notifyConsumer`.
- **`QueueMetrics`** — owns Micrometer metric registration. Applies `BackendRegistries.getDefaultNow()` fallback inside `initMicrometerMetrics()`. Call `getMeterRegistry()` after `initMicrometerMetrics()` to get the resolved registry.

## Micrometer metrics wiring (important)

`MeterRegistry` flows: `RedisQues` → `QueueMetrics` → `QueueActionsService` → `EnqueueAction`.

`QueueMetrics.initMicrometerMetrics()` resolves the registry (including `BackendRegistries` fallback). `RedisQues.initialize()` reads it back via `queueMetrics.getMeterRegistry()` before constructing `QueueActionsService` — this is intentional and was the fix for [NEMO-12348](https://github.com/swisspost/vertx-redisques/issues/406). Do not remove that line.

`EnqueueAction` only registers `ENQUEUE_SUCCESS` / `ENQUEUE_FAIL` counters when `meterRegistry != null`.

## Test patterns

- Action unit tests extend `AbstractQueueActionTest` (mocks Redis, keyspaceHelper, config).
- `redisquesConfiguration.getMicrometerMetricsIdentifier()` returns `"foo"` in the base setup — match this tag when asserting on counters.
- Integration tests extend `AbstractTestCase` and use Jedis directly to set up/tear down Redis state.

## Code Exploration
For repository analysis, ALWAYS use CodeGraph (when available) before using Grep, Find, or reading multiple files. Grep should only be used when searching for raw text that is not represented in the code graph (comments, TODOs, configuration values, literals, etc.).
