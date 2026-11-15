# How Kafka began

Kafka was built at LinkedIn, around 2010, by Jay Kreps, Neha Narkhede, and Jun Rao. The job was the activity feed: who viewed a profile, which button was clicked, which service logged an error. That data had to move from many producers to many consumers — search, monitoring, recommendations, warehouse loads — without each pair of systems inventing its own pipeline.

The message brokers of the time were a poor fit. They treated each message as a queue item to be handed to one consumer and then deleted. LinkedIn needed something closer to a durable, ordered log: many independent readers, each remembering its own position, reading the same history at its own pace. Throughput mattered more than per-message routing features.

They named it after Franz Kafka. Kreps has said the name stuck because the system is optimized for writing.

A paper, "Kafka: a Distributed Messaging System for Log Processing" (NetDB 2011), described the design. The project was open sourced in early 2011, entered the Apache Incubator that year, and became a top-level Apache project on 23 October 2012.

In December 2013 Kreps published "The Log: What every software engineer should know about real-time data's unifying abstraction." The argument is the one this lab is built around: a lot of data systems are logs. A database's commit log, a replication stream, a queue, an activity feed — same structure, different readers. Kafka is that structure, pulled out and run as a service.

The three authors founded Confluent in 2014 to build a company around the project. Kafka itself stayed an Apache project.

## What changed after the first release

The original system already had the log, partitions, and a pull-based consumer. Later releases filled in the parts people now treat as basic:

| When | What landed | Why it matters |
| --- | --- | --- |
| 0.8 (2013) | Replication inside Kafka | Before this, durability was largely "the disk of one broker." |
| 0.9 (2015) | The modern consumer group | Group membership and offset commits moved into Kafka. ZooKeeper stopped being on the consumer's path. |
| 0.11 (2017) | Idempotent producer and transactions | A producer can retry without duplicating records, and can write to several partitions atomically. |
| 1.0 (November 2017) | First release the project called stable | The log model was no longer an experiment. |
| 2.8 (2021) | KRaft preview | Metadata moves off ZooKeeper onto a Raft quorum. |
| 3.3 (2022) | KRaft marked production-ready | A cluster can run with no ZooKeeper. |
| 4.0 (March 2025) | ZooKeeper removed | This lab runs 4.4.0. There is no ZooKeeper to start. |

KRaft is the controller design you will see in Docker and in Kubernetes. A quorum of controllers stores the cluster metadata in an internal log. In this lab each node plays both roles: broker and controller. Production clusters often separate them, so a metadata hiccup and a data disk are not the same process.
