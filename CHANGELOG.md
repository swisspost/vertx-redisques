# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),

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