# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),

## [4.1.42-SNAPSHOT] - 2026-06-22

### New features
- Add support config timeout
  To enable this feature, you need set it via QueueConfiguration.configExpireTimeout. '0' means timeout disabled
  the config will be removed after the timeout, if no update via setPerQueueConfiguration happened
    - Example:
```json
{
  "pattern":"listener-hook-http.*",
  "retryIntervals":[10],
  "configExpireTimeout": 10
}
```

## [4.1.41-SNAPSHOT] - 2026-05-21
### Major changes
- From this version the Redis API version requirement change to > 7

### Minor changes
- Update [`supercharge/redis-github-action`](https://github.com/supercharge/redis-github-action) from `1.4.0` to `1.8.1`
- Update [`redis`](https://github.com/supercharge/redis-github-action) from `4` to `7`


### New features
- Add support batch queue items dispatch
  To enable this feature, you need set it via QueueConfiguration.withMaximumItemInBatchDispatch. once you have item in batch > 2,
  the payload in message will change from a normal string to a JsonArray which contains mutilple items.
    - Additional parameter:
        - minimumItemInBatchDispatch: Minimum queue items required to do a batch, if not enough items, will wait until reach the condition, '0' means not in use
        - maxBatchItemDispatchWaitTimeout: How many seconds need to wait the queue items reach the condition, '0' means always wait
    - Example:
```json
{
  "queue": "batch-queue",
  "payload": "[\"message-1\", \"message-2\", \"message-3\"]"
}
```

## [4.1.41-SNAPSHOT] - 2026-05-20
### Major changes
- removed "globalQueuePatrol" from RedisquesConfiguration, queue patrol replaced with Per-Queue config

### New features
- Add rule based queue patrol

Can config by QueueConfiguration or use setPerQueueConfiguration action
```json
{
  "operation": "setPerQueueConfiguration",
  "payload": {
    "configName": "queu-limiter-5k",
    "filter": "queue.*",
    "enqueuePatrolLimit": 5000
  }
}
```

- Added new actions:
    - getPerQueueConfiguration
    - deleteQueueConfiguration
    - getQueuesSizeStatistics


- Added new endpoint to get all approximate queue size:
    - GET /queuing/statistics/queuesize