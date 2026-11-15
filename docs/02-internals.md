# What is actually inside

Kafka stores a **log**. A topic is a name for a set of logs. Each log is a **partition**: an append-only sequence of records. Order is guaranteed inside one partition, and nowhere else.

A record is a small tuple:

- **offset** — the position in that partition, assigned by the leader when the record is appended. It starts at 0 and only increases.
- **timestamp** — event time or the broker's append time.
- **key** — optional bytes. The default partitioner hashes the key so the same key always lands on the same partition (while the partition count stays put).
- **value** — the payload. Kafka does not interpret it.
- **headers** — optional key/value metadata.

```
topic orders, after scripts/demo.sh
  partition 0:  [0 alice] [1 bob] [2 alice] [3 bob]
  partition 1:  (empty — dave would land here)
  partition 2:  [0 carol]
```

Alice's cart and her checkout stay in order because they share a key, and a key always selects the same partition. Bob's hash is different, but with only three partitions the remainder collides, so Bob shares partition 0 and there is an order between Alice and Bob. Carol does not collide, so there is no order between Carol and Alice. If you need "checkout happens after add-to-cart" for one customer, key by customer id. Do not expect different keys to land on different partitions.

## The write path

1. The producer batches records in memory. Batching, not the per-record API, is why Kafka is fast.
2. The partitioner picks a partition. With a key, Kafka uses murmur2 and then `hash % partitionCount` (the sign bit is cleared first). With no key, the sticky partitioner fills one partition for a batch interval, then switches. That keeps batches full.
3. The batch is sent to the **leader** of that partition. Followers do not accept produces.
4. The leader appends the batch to its log and assigns offsets. The append is a sequential write. Kafka leans on the operating system's page cache instead of trying to be a database buffer pool. A flush to disk is configurable; the usual durability story is replication, not `fsync` on every record.
5. Followers pull the new bytes and append them to their own logs. A follower that is caught up sits in the **in-sync replica set (ISR)**.
6. The leader advances the **high watermark** to the offset every ISR member has. Consumers are only allowed to read up to the high watermark, so they never observe a record that could vanish if the leader dies.
7. The producer is done when its `acks` setting is satisfied. `acks=0` does not wait. `acks=1` waits for the leader's append. `acks=all` waits until every current ISR member has the record (and there are at least `min.insync.replicas` of them).

If the producer has idempotence on (the default since 3.0), each batch carries a producer id and a sequence number. The leader rejects a replay, so a retry does not create a second copy.

## The log on disk

Each partition is a directory of **segments**. The active segment is the one being appended to. Older segments are closed files. Beside the `.log` bytes, Kafka writes a sparse offset index and a time index, so a consumer can jump to "offset 10_000" or "the record nearest this timestamp" without scanning the whole file.

Retention deletes or compacts old segments.

- **Delete** policy drops segments older than `retention.ms` or larger than `retention.bytes`. This is the activity-stream case: you care about recent history.
- **Compact** policy keeps the latest record for each key. This is a changelog: "the current email of user 42," not every edit. Compacted topics are how Kafka Streams stores state changelogs, and how a connector can rebuild a table.

Reading is a pull. The consumer asks the leader (or a follower, if the cluster allows follower fetch) for "bytes from offset N." The broker serves that from the page cache, often with a zero-copy transfer (`sendfile`) from the file cache to the socket. The consumer commits its offset, usually to the internal topic `__consumer_offsets`, when it is ready. The broker does not delete the record just because someone read it. That is the whole difference from a classic queue.

## Groups

A **consumer group** is a set of processes that share one subscription. The group coordinator (a broker) gives each member a subset of the partitions. One partition is read by only one member of the group at a time, which is how Kafka keeps per-partition order while still scaling out.

If a member joins or dies, the group rebalances. The older eager protocol stops everyone and reassigns. The cooperative sticky protocol moves only the partitions that have to move. Kafka 4 also ships a newer group protocol (KIP-848) where the broker computes the assignment instead of a client-side leader.

The number of useful consumers in a group is capped by the number of partitions. Six consumers and three partitions means three consumers sit idle.

## Replication and the controller

Every partition has one leader and zero or more followers. The leader is the only broker that takes writes. The ISR is the leader plus every follower that has caught up within `replica.lag.time.max.ms`. A follower that stalls drops out of the ISR. When the leader dies, a new leader is chosen from the ISR, so the new leader is not missing records the old one had acknowledged with `acks=all`.

Who decides the leader? The **controller**.

Historically one broker won a ZooKeeper election and owned that job, and ZooKeeper also stored the list of topics and partitions. That split is gone. In KRaft a Raft quorum of controllers replicates a metadata log (the `__cluster_metadata` topic). The same facts — which brokers are alive, who leads partition 2 of `orders`, what the config is — are records in a log. This lab runs **combined** mode: `KAFKA_PROCESS_ROLES=broker,controller` on every node, with the quorum listed in `KAFKA_CONTROLLER_QUORUM_VOTERS`.

`min.insync.replicas=2` on a topic with replication factor 3 means a produce with `acks=all` needs the leader and at least one follower. You can lose one broker and still accept writes. Lose two, and producers that wait for the full ISR get an error instead of silently acknowledging a record that now exists on only one disk.

## What this lab will not pretend

A single laptop cluster is not a production layout. There is no rack awareness, no TLS, no authentication, and the Kubernetes manifests use `emptyDir`, so a restart drops the log. The point is to see leaders, replicas, offsets, and the page-cache-shaped write path with the real broker, then read the failure rules above with that picture in mind.
