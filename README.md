# vertx-redisques

[![Build Status](https://drone.io/github.com/swisspush/vertx-redisques/status.png)](https://drone.io/github.com/swisspush/vertx-redisques/latest)

A highly scalable redis-persistent queuing system for vert.x.

## Dynamic Queues

They are stored as redis lists, thus created only when needed and removed when empty. 
There is nothing left even if you just used thousands of queues with thousands of messages.

## Smart Consuming

It is guaranteed that a queue is consumed by the same RedisQuesVerticle instance (a consumer). 
If no consumer is registered for a queue, one is assigned (this uses redis setnx to ensure only one is assigned).
When idle for a given time, a consumer is removed. This prevents subscription leaks and makes recovering automatic
when a consumer dies.

## Safe Distribution

There is no single point of control/failure. Just create many instances of RedisQues, they will work together.
If an instance dies, its queues will be assigned to other instances.

## Configuration

The following configuration values are available:

| Property | Default value | Description |
|:--------- | :----------- | :----------- |
| address | redisques | The eventbus address the redisques module is listening to |
| redis-prefix | redisques: | Prefix for redis keys holding queues and consumers |
| processor-address | redisques-processor | Address of message processors |
| refresh-period | 10 | The frequency of consumers refreshing their subscriptions to consume |
| redisHost | localhost | The host where redis is running on |
| redisPort | 6379 | The port where redis is running on |
| redisEncoding | UTF-8 | The encoding to use in redis |

### Configuration util

The configurations have to be passed as JsonObject to the module. For a simplyfied configuration the _RedisquesConfigurationBuilder_ can be used.

Example:

```java
RedisquesConfiguration config = RedisquesConfiguration.with()
		.redisHost("anotherhost")
		.redisPort(1234)
		.build();

JsonObject json = config.asJsonObject();
```

Properties not overriden will not be changed. Thus remaining default.

To use default values only, the _RedisquesConfiguration_ constructor without parameters can be used:

```java
JsonObject json  = new RedisquesConfiguration().asJsonObject();
```

## RedisQue APIs

Redisque API for Vert.x - Eventbus

**Evenbus Settings:**

	address = redisque

### enqueue

Request Data

	{
		"operation": "enqueue",
		"queue": <str QUEUENAME>,
		"message": {
			"method": "POST",
			"uri": <st REQUEST URI>,
			"payload": null
		}
	}

Response Data

	{
		"status": "ok" / "error",
		"message": "enqueued" / <str RESULT>
	}

### check

Request Data

	{
		"operation": "check"
	}

Response Data

	{}

### reset

Request Data

	{
		"operation": "reset"
	}

Response Data

	{}

### lock

Request Data

	{
		"operation": "lock",
		"queue": <str QUEUENAME>
	}

Response Data

	{
		"status": "ok" / "error"
	}

### stop

Request Data

	{
		"operation": "stop"
	}

Response Data

	{
		"status": "ok" / "error"
	}

### getListRange

Request Data

	{
		"operation": "getListRange",
		"payload": {
			"queuename": <str QUEUENAME>,
			"limit": <str LIMIT>
		}
	}

Response Data

	{
		"status": "ok" / "error",
		"value": <objArr RESULT>,
		"info": <nbrArray with result array (value property) size and total queue item count (can be greater than limit)>
	}

### addItem

Request Data

	{
		"operation": "addItem",
		"payload": {
			"queuename": <str QUEUENAME>,
			"buffer": <str BUFFERDATA>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

### getItem

Request Data

	{
		"operation": "getItem",
		"payload": {
			"queuename": <str QUEUENAME>,
			"index": <int INDEX>
		}
	}

Response Data

	{
		"status": "ok" / "error",
		"value": <obj RESULT>
	}

### replaceItem

Request Data

	{
		"operation": "replaceItem",
		"payload": {
			"queuename": <str QUEUENAME>,
			"buffer": <str BUFFERDATA>,
			"index": <int INDEX>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

### deleteItem

Request Data

	{
		"operation": "deleteItem",
		"payload": {
			"queuename": <str QUEUENAME>,
			"index": <int INDEX>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

### deleteAllQueueItems

Request Data

	{
		"operation": "deleteAllQueueItems",
		"payload": {
			"queuename": <str QUEUENAME>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

### getAllLocks

Request Data

	{
		"operation": "getAllLocks",
		"payload": {}
	}

Response Data

	{
		"status": "ok" / "error",
		"value": <obj RESULT>
	}

### putLock

Request Data

	{
		"operation": "putLock",
		"payload": {
			"queuename": <str QUEUENAME>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

### getLock

Request Data

	{
		"operation": "getLock",
		"payload": {
			"queuename": <str QUEUENAME>
		}
	}

Response Data

	{
		"status": "ok" / "error",
		"value": <obj RESULT>
	}

### deleteLock

Request Data

	{
		"operation": "deleteLock",
		"payload": {
			"queuename": <str QUEUENAME>
		}
	}

Response Data

	{
		"status": "ok" / "error"
	}

## Dependencies

Redisques versions greater than 01.00.17 depend on Vert.x v3.2.0 and therefore require Java 8.

## Use gradle with alternative repositories

As standard the default maven repositories are set.
You can overwrite these repositories by setting these properties (`-Pproperty=value`):
* `repository` this is the repository where resources are fetched
* `uploadRepository` the repository used in `uploadArchives`
* `repoUsername` the username for uploading archives
* `repoPassword` the password for uploading archives

[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/lbovet/vertx-redisques/trend.png)](https://bitdeli.com/free "Bitdeli Badge")