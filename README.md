# vertx-redisques
 
![Build Status](https://github.com/swisspost/vertx-redisques/actions/workflows/maven.yml/badge.svg)
[![codecov](https://codecov.io/gh/swisspost/vertx-redisques/branch/master/graph/badge.svg?token=z0zQXWoqzB)](https://codecov.io/gh/swisspost/vertx-redisques)
[![Maven Central](https://img.shields.io/maven-central/v/org.swisspush/redisques.svg)](http://search.maven.org/#artifactdetails|org.swisspush|redisques|2.2.0|)

A highly scalable redis-persistent queuing system for vert.x

## Getting Started
### Install
* Clone this repository or unzip [archive](https://github.com/swisspush/vertx-redisques/archive/master.zip)
* Install and start Redis
  * Debian/Ubuntu: `apt-get install redis-server`
  * Fedora/RedHat/CentOS: `yum install redis`
  * OS X: `brew install redis`
  * [Windows](https://github.com/MSOpenTech/redis/releases/download/win-2.8.2400/Redis-x64-2.8.2400.zip)
  * [Other](http://redis.io/download)

### Build
You need **Java 11** and Maven.
```
cd vertx-redisques
mvn clean install
```

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

| Property                                | Default value                   | Description                                                                                                                                                                                                      |
|:----------------------------------------|:--------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| address                                 | redisques                       | The eventbus address the redisques module is listening to                                                                                                                                                        |
| configuration-updated-address           | redisques-configuration-updated | The eventbus address the redisques module publishes the configuration updates to                                                                                                                                 |
| redis-prefix                            | redisques:                      | Prefix for redis keys holding queues and consumers                                                                                                                                                               |
| processor-address                       | redisques-processor             | Address of message processors                                                                                                                                                                                    |
| refresh-period                          | 10                              | The frequency [s] of consumers refreshing their subscriptions to consume                                                                                                                                         |
| processorTimeout                        | 240000                          | The timeout [ms] to wait for the queue processor to answer the event bus message                                                                                                                                 |
| processorDelayMax                       | 0                               | The maximum delay [ms] to wait between queue items before notify the consumer                                                                                                                                    |
| redisHost                               | localhost                       | The host where redis is running on                                                                                                                                                                               |
| redisPort                               | 6379                            | The port where redis is running on                                                                                                                                                                               |
| redisAuth                               |                                 | The authentication key (password) to connect to redis                                                                                                                                                            |
| maxPoolSize                             | 200                             | The maximum size of the redis connection pool                                                                                                                                                                    |
| maxPoolWaitingSize                      | -1                              | The maximum waiting requests for a connection from the pool                                                                                                                                                      |
| maxPipelineWaitingSize                  | 2048                            | The maximum allowed queued waiting handlers                                                                                                                                                                      |
| checkInterval                           | 60                              | The interval [s] to check timestamps of not-active / empty queues by executing **check** queue operation. _checkInterval_ value must be greater 0, otherwise the default is used.                                |
| queueSpeedIntervalSec                   | 60                              | The interval [s] to check queue speed                                                                                                                                                                            |
| memoryUsageLimitPercent                 | 100                             | Percentage of the available system memory to be used by vertx-redisques. Only values between 0 and 100 are allowed. When the used memory ratio is higher than this limit, enqueues are rejected                  |
| memoryUsageCheckIntervalSec             | 60                              | The interval [s] to check the current memory usage. _memoryUsageCheckIntervalSec_ value must be greater 0, otherwise the default is used.                                                                        |
| redisReconnectAttempts                  | 0                               | The amount of attempts to reconnect when redis connection is lost. Use **0** to not reconnect at all or **-1** to reconnect indefinitely.                                                                        |
| redisReconnectDelaySec                  | 30                              | The interval [s] to attempt to reconnect when redis connection is lost.                                                                                                                                          |
| redisPoolRecycleTimeoutMs               | 180000                          | The timeout [ms] when the connection pool is recycled. Use **-1** when having reconnect feature enabled.                                                                                                         |
| micrometerMetricsEnabled                | false                           | Enable / disable collection of metrics using micrometer                                                                                                                                                          |
| httpRequestHandlerEnabled               | false                           | Enable / disable the HTTP API                                                                                                                                                                                    |
| httpRequestHandlerAuthenticationEnabled | false                           | Enable / disable authentication for the HTTP API                                                                                                                                                                 |
| httpRequestHandlerUsername              |                                 | The username for the HTTP API authentication                                                                                                                                                                     |
| httpRequestHandlerPassword              |                                 | The password for the HTTP API authentication                                                                                                                                                                     |
| enableQueueNameDecoding                 | true                            | Enable / disable the encoding of queue names when using the HTTP API                                                                                                                                             |
| httpRequestHandlerPrefix                | /queuing                        | The url prefix for all HTTP API endpoints                                                                                                                                                                        |
| httpRequestHandlerPort                  | 7070                            | The port of the HTTP API                                                                                                                                                                                         |
| httpRequestHandlerUserHeader            | x-rp-usr                        | The name of the header property where the user information is provided. Used for the HTTP API                                                                                                                    |
| queueConfigurations                     |                                 | Configure retry intervals and enqueue delaying for queue patterns                                                                                                                                                |
| dequeueStatisticReportIntervalSec       | -1                              | The interval [s] to publish the dequeue statistics into shared map. Use **-1** to not publish at all. In a hazelcast-cluster environment need config Semaphore first, see: [Semaphore Config](#Semaphore Config) |
| publish-metrics-address                 |                                 | The EventBus address to send collected redis metrics to                                                                                                                                                          |
| metric-storage-name                     | queue                           | The name of the storage used in the published metrics                                                                                                                                                            |
| metric-refresh-period                   | 10                              | The frequency [s] of collecting metrics from redis database                                                                                                                                                      |

### Configuration util

The configurations have to be passed as JsonObject to the module. For a simplified configuration the _RedisquesConfigurationBuilder_ can be used.

Example:

```java
RedisquesConfiguration config = RedisquesConfiguration.with()
		.redisHost("anotherhost")
		.redisPort(1234)
		.build();

JsonObject json = config.asJsonObject();
```

Properties not overridden will not be changed. Thus remaining default.

To use default values only, the _RedisquesConfiguration_ constructor without parameters can be used:

```java
JsonObject json  = new RedisquesConfiguration().asJsonObject();
```

## RedisQues APIs

Redisques API for Vert.x - Eventbus

**Evenbus Settings:**

> address = redisque

### RedisquesAPI util
For a simplified working with the Redisques module, see the RedisquesAPI class:

> org.swisspush.redisques.util.RedisquesAPI

This class provides utility methods for a simple configuration of the queue operations. See _Queue operations_ chapter below for details.

### Queue operations
The following operations are available in the Redisques module.

#### getConfiguration

Request Data

```
{
    "operation": "getConfiguration"
}
```

Response Data

```
{
    "status": "ok" / "error",
    "value": <obj RESULT>
}
```

#### setConfiguration

Request Data

```
{
    "operation": "setConfiguration",
    "payload": {
        "<str propertyName>": <str propertyValue>,
        "<str property2Name>": <str property2Value>,
        "<str property3Name>": <str property3Value>
    }    
}
```

Response Data

```
{
    "status": "ok" / "error",
    "message": <string error message when status=error>
}
```

#### enqueue

Request Data

```
{
    "operation": "enqueue",
    "payload": {
        "queuename": <str QUEUENAME>
    },
    "message": {
        "method": "POST",
        "uri": <st REQUEST URI>,
        "payload": null
    }
}
```

Response Data

```
{
    "status": "ok" / "error",
    "message": "enqueued" / <str RESULT>
}
```

#### lockedEnqueue
Request Data

```
{
    "operation": "lockedEnqueue",
    "payload": {
        "queuename": <str QUEUENAME>,
        "requestedBy": <str user who created the lock>
    },
    "message": {
        "method": "POST",
        "uri": <st REQUEST URI>,
        "payload": null
    }
}
```
Response Data
```
{
    "status": "ok" / "error",
    "message": "enqueued" / <str RESULT>
}
```

#### getQueues

Request Data

```
{
    "operation": "getQueues"
}
```

Response Data

```
{
    "status": "ok" / "error",
    "value": <objArr RESULT>
}
```

#### getQueuesCount

Request Data

```
{
    "operation": "getQueuesCount",
    "payload": {
        "filter": <str regex pattern to filter queues to count (optional)>
    }
}
```

Response Data

```
{
    "status": "ok" / "error",
    "value": <long RESULT>
}
```

#### getQueueItemsCount

Request Data
```
{
    "operation": "getQueueItemsCount",
    "payload": {
        "queuename": <str QUEUENAME>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <long RESULT>
}
```

#### check

Request Data
```
{
    "operation": "check"
}
```

Response Data
```
{}
```

#### reset

Request Data
```
{
    "operation": "reset"
}
```

Response Data
```
{}
```

#### stop

Request Data
```
{
    "operation": "stop"
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### getQueueItems

Request Data
```
{
    "operation": "getQueueItems",
    "payload": {
        "queuename": <str QUEUENAME>,
        "limit": <str LIMIT>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <objArr RESULT>,
    "info": <nbrArray with result array (value property) size and total queue item count (can be greater than limit)>
}
```

#### addQueueItem

Request Data
```
{
    "operation": "addQueueItem",
    "payload": {
        "queuename": <str QUEUENAME>,
        "buffer": <str BUFFERDATA>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### getQueueItem

Request Data
```
{
    "operation": "getQueueItem",
    "payload": {
        "queuename": <str QUEUENAME>,
        "index": <int INDEX>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <obj RESULT>
}
```

#### replaceQueueItem

Request Data
```
{
    "operation": "replaceQueueItem",
    "payload": {
        "queuename": <str QUEUENAME>,
        "buffer": <str BUFFERDATA>,
        "index": <int INDEX>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### deleteQueueItem

Request Data
```
{
    "operation": "deleteQueueItem",
    "payload": {
        "queuename": <str QUEUENAME>,
        "index": <int INDEX>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### deleteAllQueueItems

Request Data
```
{
    "operation": "deleteAllQueueItems",
    "payload": {
        "queuename": <str QUEUENAME>,
        "unlock": true/false
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <long 1 when the queue was deleted, long 0 when no queue was found>
}
```

#### bulkDeleteQueues

Request Data
```
{
    "operation": "bulkDeleteQueues",
    "payload": {
        "queues": <JsonArray queues to delete>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <Long Amount of deleted queues>
}
```

#### getAllLocks

Request Data
```
{
    "operation": "getAllLocks",
    "payload": {
        "filter": <str regex pattern to filter locks (optional)>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <obj RESULT>
}
```

#### putLock

Request Data
```
{
    "operation": "putLock",
    "payload": {
        "queuename": <str QUEUENAME>,
        "requestedBy": <str user who created the lock>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### bulkPutLocks

Request Data
```
{
    "operation": "bulkPutLocks",
    "payload": {
        "locks": <JsonArray locks to add>,
        "requestedBy": <str user who created the locks>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### getLock

Request Data
```
{
    "operation": "getLock",
    "payload": {
        "queuename": <str QUEUENAME>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <obj RESULT>
}
```

#### deleteLock

Request Data
```
{
    "operation": "deleteLock",
    "payload": {
        "queuename": <str QUEUENAME>
    }
}
```

Response Data
```
{
    "status": "ok" / "error"
}
```

#### bulkDeleteLocks

Request Data
```
{
    "operation": "bulkDeleteLocks",
    "payload": {
        "locks": <JsonArray locks to delete>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <Long Amount of deleted locks>
}
```

#### deleteAllLocks

Request Data
```
{
    "operation": "deleteAllLocks"
}
```

Response Data
```
{
    "status": "ok" / "error",
    "value": <Long Amount of deleted locks>
}
```

#### getQueuesStatistics

Request Data
```
{
    "operation": "getQueueStatistics",
    "payload": {
        "filter": <str regex pattern to filter queues to retrieve statistics information (optional)>
    }
}
```

Response Data
```
{
    "status": "ok" / "error",
    "queues": [   <JsonArray of resulting queue statistics>
    {
      "name": <str QUEUENAME>
      "size": <Long Size of queue>
      "failures": <Long Number of current failure count of queue>
      "slowdownTime": <Long Current slowdown time in ms>
      "backpressureTime": <Long Current backpressure time in ms>
    }
    ]    
}
```

#### getQueuesSpeed

Request Data
```
{
    "operation": "getQueuesSpeed",
    "payload": {
        "filter": <str regex pattern to filter queues to retrieve the cumulated speed (optional)>
    }
}
```

Response Data
```
{
    "speed": <Long speed>
    "unitSec": <Long seconds>
}
```

## RedisQues HTTP API
RedisQues provides a HTTP API to modify queues, queue items and get information about queue counts and queue item counts.

### Configuration
To enable the HTTP API, the _httpRequestHandlerEnabled_ configuration property has to be set to _TRUE_. When authentication for the HTTP API
is enabled, an _Authorization_ request header (basic auth) has to be provided.

For additional configuration options relating the HTTP API, see the [Configuration](#configuration) section.

### API Endpoints
The following request examples will use a configured prefix of _/queuing_

### List endpoints
To list the available endpoints use
> GET /queuing

The result will be a json object with the available endpoints like the example below

```json
{
  "queuing": [
    "locks/",
    "queues/",
    "monitor/",
    "configuration/"
  ]
}
```

### Get configuration
The configuration information contains the currently active configuration values. To get the configuration use
> GET /queuing/configuration

The result will be a json object with the configuration values like the example below

```json
{
  "address": "redisques",
  "configuration-updated-address": "redisques-configuration-updated",
  "redis-prefix": "redisques:",
  "processor-address": "processor-address",
  "refresh-period": 2,
  "redisHost": "localhost",
  "redisPort": 6379,
  "redisReconnectAttempts": 0,
  "redisReconnectDelaySec": 30,
  "redisPoolRecycleTimeoutMs": 180000,  
  "redisAuth": null,
  "checkInterval": 60,
  "processorTimeout": 240000,
  "processorDelayMax": 0,
  "httpRequestHandlerEnabled": false,
  "httpRequestHandlerPrefix": "/queuing",
  "httpRequestHandlerPort": 7070,
  "httpRequestHandlerUserHeader": "x-rp-usr",
  "queueConfigurations": [{
    "pattern": "queue.*",
    "retryIntervals": [2, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52],
    "enqueueDelayFactorMillis": 0.0,
    "enqueueMaxDelayMillis": 0
  }],
  "enableQueueNameDecoding": true,
  "maxPoolSize": 200,
  "maxPoolWaitingSize": -1,
  "maxPipelineWaitingSize": 2048,
  "queueSpeedIntervalSec": 60,
  "memoryUsageLimitPercent": 100,
  "memoryUsageCheckIntervalSec": 60
}
```

### Set configuration
To set the configuration use
> POST /queuing/configuration

having the payload in the request body. The current implementation supports the following configuration values only:
```
{
  "processorDelayMax": 0, // number value in milliseconds 
  "processorTimeout": 1 // number value in milliseconds 
}
```
The following conditions will cause a _400 Bad Request_ response with a corresponding error message:
* Body is not a valid json object
* Body contains not supported configuration values

### Get monitor information
The monitor information contains the active queues and their queue items count. To get the monitor information use
> GET /queuing/monitor

Available url parameters are:
* _limit_: The maximum amount of queues to list
* _emptyQueues=true_: Also show empty queues

The result will be a json object with the monitor information like the example below

```json
{
  "queues": [
    {
      "name": "queue_1",
      "size": 3
    },
    {
      "name": "queue_2",
      "size": 2
    }
  ]
}
```

### Enqueue
To enqueue a new queue use
> PUT /queuing/enqueue/myNewQueue

having the payload in the request body. When the request body is not a valid json object, a statusCode 400 with the error message _'Bad Request'_ will be returned.

Available url parameters are:
* _locked=true_: Lock the queue before enqueuing to prevent processing

When the _locked=true_ url parameter is set, the configured _httpRequestHandlerUserHeader_ property will be used to define the user which requested the lock. If no header is provided, "Unknown" will be used instead.

### List or count queues
To list the active queues use
> GET /queuing/queues

Available url parameters are:
* _filter=<regex pattern>_: Filter the queues to list or count

The result will be a json object with a list of active queues like the example below

```json
{
  "queues": [
    "queue_1",
    "queue_2",
    "queue_3"
  ]
}
```
**Attention:** The result will also contain empty queues when requested before the internal cleanup has passed. Use the monitor endpoint when non-empty queues should be listed only.

To get the count of active queues only, use
> GET /queuing/queues?count=true

The result will be a json object with the count of active queues like the example below

```json
{
  "count": 3
}
```
**Attention:** The count will also contain empty queues when requested before the internal cleanup has passed. Use the monitor endpoint when non-empty queues should be counted only.

### List or count queue items
To list the queue items of a single queue use
> GET /queuing/queues/myQueue

Add the _limit_ url parameter to define the maximum amount of queue items to retrieve

The result will be a json object with a list of queue items like the example below

```json
{
  "myQueue": [
    "queueItem1",
    "queueItem2",
    "queueItem3",
    "queueItem4"
  ]
}
```

To get the count of queue items only, use
> GET /queuing/queues/myQueue?count=true

The result will be a json object with the count of queue items like the example below

```json
{
  "count": 4
}
```

### Delete all queue items
To delete all queue items of a single queue use
> DELETE /queuing/queues/myQueue

The result will be a statusCode _200 OK_ when the queue could be successfully deleted or a _404 Not Found_ when the queue did not exist. Any error will result in a statusCode _500 Internal Server Error_. 

Available url parameters are:
* _unlock=true_: Unlock the queue after deleting all queue items

### Bulk delete queues
To delete a custom subset of existing queues use
> POST /queuing/queues?bulkDelete=true

The payload must contain an array with the queues to delete.

Example:
```json
{
  "queues": [
    "queue1",
    "queue2"
  ]
}
```

The result will be a json object containing the number of deleted queues like the example below

```json
{
  "deleted": 2
}
```

### Get single queue item
To get a single queue item use
> GET /queuing/queues/myQueue/0

The result will be a json object representing the queue item at the given index (0 in the example above). When no queue item at the given index exists, a statusCode 404 with the error message _'Not Found'_ will be returned.

### Replace single queue item
To replace a single queue item use
> PUT /queuing/queues/myQueue/0

having the payload in the request body. The queue must be locked to perform this operation, otherwise a statusCode 409 with the error message _'Queue must be locked to perform this operation'_ will be returned. When no queue item at the given index exists, a statusCode 404 with the error message _'Not Found'_ will be returned.

### Delete single queue item
To delete a single queue item use
> DELETE /queuing/queues/myQueue/0

The queue must be locked to perform this operation, otherwise a statusCode 409 with the error message _'Queue must be locked to perform this operation'_ will be returned. When no queue item at the given index exists, a statusCode 404 with the error message _'Not Found'_ will be returned.

### Add queue item
To add a queue item (to the end of the queue) use
> POST /queuing/queues/myQueue/

having the payload in the request body. When the request body is not a valid json object, a statusCode 400 with the error message _'Bad Request'_ will be returned.

### Get all locks
To list all existing locks use
> GET /queuing/locks/

Available url parameters are:
* _filter=<regex pattern>_: Filter the locks to return

The result will be a json object with a list of all locks like the example below

```json
{
  "locks": [
    "queue1",
    "queue2"
  ]
}
```

### Add lock
To add a lock use
> PUT /queuing/locks/myQueue

having an empty json object {} in the body. The configured _httpRequestHandlerUserHeader_ property will be used to define the user which requested the lock. If no header is provided, "Unknown" will be used instead.

### Bulk add locks
To add multiple locks use
> POST /queuing/locks

The payload must contain an array with the locks to add.

Example:
```json
{
  "locks": [
    "queue1",
    "queue2"
  ]
}
```

### Get single lock
To get a single lock use
> GET /queuing/locks/queue1

The result will be a json object with the lock information like the example below

```json
{
  "requestedBy": "someuser",
  "timestamp": 1478100330698
}
```

### Delete single lock
To delete a single lock use
> DELETE /queuing/locks/queue1

### Bulk delete locks
To delete a custom subset of existing locks use
> POST /queuing/locks?bulkDelete=true

The payload must contain an array with the locks to delete.

Example:
```json
{
  "locks": [
    "queue1",
    "queue2"
  ]
}
```

The result will be a json object containing the number of deleted locks like the example below

```json
{
  "deleted": 2
}
```

### Delete all locks
To delete all existing locks use
> DELETE /queuing/locks

The result will be a json object containing the number of deleted locks like the example below

```json
{
  "deleted": 22
}
```

### Get statistics information
The statistics information contains the active queues and their queue items count plus the current number of failures and delay times in ms. 
To get the monitor information use
> GET /queuing/statistics

Available url parameters are:
* _limit_: The maximum amount of queues to list
* _emptyQueues=true_: Also show empty queues
* _filter=<regex pattern>_: Filter the queues to list or count

The result will be a json object with the statistics information like the example below.

```json
{
    "queues": [
    {
      "name": "queue_1",
      "size": 3,
      "failures": 0,
      "slowdownTime": 0,
      "backpressureTime": 0
    }
    ]    
}
```

### Get queue speed
The speed evaluation collects and calculates the cumulated speed of all queues matchting the given filter.
To get the speed information use
> GET /queuing/speed

Available url parameters are:
* _filter=<regex pattern>_: Filter the queues for which the cumulated speed is evaluated

The result will be a json object with the speed of the last measurement period calculated
over all queues matching the given filter regex. Additionally the used measurement time in seconds
is returned (eg. 60 seconds by default)

```json
{
  "speed": 42,
  "unitSec": 60
}
```

### Semaphore Config
If you are running in a Hazelcast cluster, the semaphore default permit must set to **1**.
https://github.com/vert-x3/vertx-hazelcast/blob/master/src/main/asciidoc/index.adoc#using-an-existing-hazelcast-cluster

```java
SemaphoreConfig semaphoreConfig = new SemaphoreConfig().setInitialPermits(1).setJDKCompatible(false).setName("__vertx.*");
hazelcastConfig.getCPSubsystemConfig().addSemaphoreConfig(semaphoreConfig);
```

## Metric collection
Besides the API, redisques provides some key metrics collected by [micrometer.io](https://micrometer.io/).

The collected metrics include:

| Metric name                     | Description                                                 |
|:--------------------------------|:------------------------------------------------------------|
| redisques_enqueue_success_total | Overall count of queue items to be enqueued                 |
| redisques_enqueue_fail_total    | Overall count of queue items to be enqueued                 |
| redisques_dequeue_total         | Overall count of queue items to be dequeued from the queues |

### Testing locally
When you include redisques in you project, you probably already have the configuration for publishing the metrics.

To export the metrics locally you have to add this dependency to the `pom.xml`

```
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
    <version>${micrometer.version}</version>
</dependency>
```
Also add the micrometer configuration to `RedisQuesRunner` class like this:

```java
MicrometerMetricsOptions options = new MicrometerMetricsOptions()
        .setPrometheusOptions(new VertxPrometheusOptions()
                .setStartEmbeddedServer(true)
                .setEmbeddedServerOptions(new HttpServerOptions().setPort(9101))
                .setEnabled(true))
        .setEnabled(true);
Vertx vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(options));
```
Using the configuration above, the metrics can be accessed with

> GET http://localhost:9101/metrics


## Dependencies

- Starting from version 2.6.x redisques requires **Java 11**.

- Redisques versions greater than 01.00.17 depend on Vert.x v3.2.0 and therefore require Java 8.
