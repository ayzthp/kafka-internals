# Kafka internals lab

A small, runnable picture of Apache Kafka: why it was built, what a partition actually is, and the same three-node KRaft cluster under Docker and under Kubernetes.

Kafka here is **4.4.0**. There is no ZooKeeper. Each node is a broker and a controller.

## Read it in this order

1. [How it began](docs/01-how-it-began.md) — LinkedIn, 2010, and why a queue was the wrong shape.
2. [Internals](docs/02-internals.md) — the log, the write path, the high watermark, consumer groups, KRaft.
3. [Use cases](docs/03-use-cases.md) — activity streams, choreography, change data capture, and the jobs it should not do.
4. [visual/index.html](visual/index.html) — click through a produce and watch the key pick a partition. Open the file in a browser. It uses the same murmur2 partitioner as the broker, and it does not need the cluster to be up.

## Run the cluster

Docker Desktop (or another engine) has to be running.

```bash
docker compose up -d
./scripts/demo.sh
```

`demo.sh` creates a topic named `orders` with 3 partitions and replication factor 3, appends five keyed events, then prints the leader and ISR for each partition and the offset of each record.

The same records are visible in Kafka UI at [http://localhost:8080](http://localhost:8080). Open the `orders` topic and look at the per-partition leader.

Client ports on the host, if you connect from outside Compose:

| Broker | Host port |
| --- | --- |
| kafka-1 | localhost:29092 |
| kafka-2 | localhost:39092 |
| kafka-3 | localhost:49092 |

Inside the Compose network the brokers advertise `kafka-1:19092` and its siblings. The UI uses those.

Stop one broker and describe the topic again. The partition whose leader was that broker elects a new leader from the remaining in-sync replicas. Internal topics such as `__consumer_offsets` stay at replication factor 1 so the cluster can finish starting; `orders` is the topic that actually has three copies.

```bash
docker compose stop kafka-3
./scripts/describe.sh
docker compose start kafka-3
```

Tear down:

```bash
docker compose down
```

## The same cluster on Kubernetes

The manifests in `k8s/` are a StatefulSet of three combined nodes. The pod ordinal is the KRaft node id (`kafka-0` is node 1). Storage is an `emptyDir`: this is a picture of the process layout, and a pod restart drops the log.

Three Compose brokers plus a kind node is enough to stall a laptop's Docker VM. If `kubectl` starts timing out on the API server, stop Compose first (`docker compose stop`), then create the cluster.

```bash
kind create cluster --name kafka-lab
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/kafka-ui.yaml
kubectl -n kafka-lab rollout status statefulset/kafka --timeout=180s
kubectl -n kafka-lab port-forward svc/kafka-ui 8080:8080
```

From another shell, the same describe you ran under Docker:

```bash
kubectl -n kafka-lab exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-0.kafka.kafka-lab.svc.cluster.local:9092 \
  --create --if-not-exists --topic orders --partitions 3 --replication-factor 3
kubectl -n kafka-lab exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-0.kafka.kafka-lab.svc.cluster.local:9092 \
  --describe --topic orders
```

A production cluster would use a real volume, TLS, and usually an operator such as Strimzi to own rolling restarts and per-broker identity. The StatefulSet is here so you can see that identity directly: node id, advertised listener, and controller quorum are plain environment variables.

## What to look at while it is running

- **Same key, same partition.** `alice` always hashes to partition 0. Her add-to-cart stays before her checkout. `bob` has a different hash that still collides on partition 0. `carol` lands on partition 2. `dave` would land on partition 1.
- **Leader column.** Only the leader takes writes for that partition. The other two brokers in the replica list are followers.
- **ISR.** After a clean start all three replicas are in sync. Stop one and the ISR shrinks. The high watermark only moves as far as the ISR.
- **Offsets do not reset when you read.** Run the consumer in `demo.sh` twice. The records are still there. A group remembers its position in `__consumer_offsets`; the log itself is unchanged.
- **Internal topics.** `__consumer_offsets` and the KRaft metadata topic are logs too. The controller's source of truth is a replicated log, which is the point of KRaft.
