# What people use it for

Kafka fits when many programs need the same ordered history, and each reader should track its own position. It is a poor fit when you need a queryable table, a per-item task queue with complex routing, or a request that must complete in one database transaction.

## The original job: activity and telemetry

LinkedIn's case is still the clearest. Product events, service logs, and metrics are appended once. Search indexing, fraud scoring, dashboards, and the warehouse each consume at their own speed. A slow warehouse does not stall the checkout service, because the checkout service only had to append.

## Service choreography

An order service appends `OrderPlaced`. Payments, inventory, and email each consume that topic and append their own facts (`PaymentAuthorized`, `StockReserved`). Nobody calls a chain of synchronous APIs that all have to be up. The cost is that you now operate an asynchronous system: consumers must be idempotent, and "did it happen?" is answered by reading the log, not by a return value.

## Change data capture

A database already has a commit log. Debezium and similar connectors tail that log and append row changes to Kafka. Downstream systems — search, cache, warehouse, another service — rebuild their view from the stream. The source database stays the system of record. Kafka is the transport and the buffer.

## Stream processing

Kafka Streams, Flink, and similar systems consume a topic, keep state, and append results to another topic. A compacted changelog topic lets them rebuild that state after a crash. The partition key is again the rule: all events for one key stay in one partition, so the state for that key stays local to one task.

## Commit log as the integration contract

Teams publish facts and let subscribers appear later. A new consumer can start at the beginning (`auto.offset.reset=earliest`) and replay history, as long as retention still holds it. That replay is the operational superpower, and also the thing you must design for: schemas evolve (Schema Registry, or a version field you control), and a replay will re-fire side effects unless consumers are idempotent.

## Where the shape breaks

- **A work queue of short tasks**, where any worker can take any item and you want to delete it on success. A consumer group can imitate this, but acknowledgement, retry topics, and poison messages are your problem. Partition order is a constraint you may not want.
- **The primary store for a user-facing read API.** A compacted topic can hold the latest value per key, and Kafka has a key-value abstraction on top of that, but range queries, secondary indexes, and transactions across entities belong in a database.
- **Large files.** Records are meant to be small. The batch and the page cache are sized for events, not video.

The decision test: do you need a durable, partitioned, replayable history that many independent readers can share? If yes, you are looking at a log. If you need "run this once, then forget it," you are looking at a queue.
