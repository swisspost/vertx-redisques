# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),

## [4.1.41-SNAPSHOT] - 2026-05-21
### Major changes
- From this version the Redis API version requirement change to > 7

### Minor changes
- Update [`supercharge/redis-github-action`](https://github.com/supercharge/redis-github-action) from `1.4.0` to `1.8.1`
- Update [`redis`](https://github.com/supercharge/redis-github-action) from `4` to `7`


### New features
- Add support batch queue items dispatch
  
To enable this feature, you need set it via QueueConfiguration.withNumberOfBatchItemDispatch. once you have item in batch > 2, 
the payload in message will change from a normal string to a JsonArray which contains mutilple items, example:

```json
{
  "queue": "batch-queue",
  "payload": "[\"message-1\", \"message-2\", \"message-3\"]"
}
```