# Kafka
- nats.Connect() - Bootstrap servers -> broker connection
- Subject -> Topic
- Stream (persisted log) -> Topic is the persisted log in Kafka. No separate "stream" object is needed.
- Message (seq number) -> Record (key, value, headers) at an offset.
- Durable consumer -> Consumer group with committed offsets.
- Queue group (load-balancing members) -> Multiple consumers in the same group.
- Fan out (several durables on one stream) -> several different consumer groups.
- msg.Ack() / AckPolicy -> offset commit
- Redelivery on no-ack -> Re-read fromm last committed offset.
- MaxAge/Retention -> Re-read from last committed offset.
- Replicas 3 -> replication.factor

- in NATS, a queue group can have any number of members sharing load. In kafka, the unit of paralleslism is the **partition**.
- A consumer group can have at most N active consumers where N = the topic's partition count - extra consumers sit idle.
- **Make a topic with 3 partitions, spin up 4 consumer goroutines in one group, and watch the 4th get nothing.**

- Go client **github.com/segmentio/kafka-go** clean writer(producer)/reader(consumer) that exposes partitions, groups, offsets, and manual commits clearly.
- Broker: Apache kafka in docker, KRaft mode (no zookeeper) - single broker.
- Kafka ui - web dashboard to see your topics, partitions, consumer groups, and lag.

- Broker is a single kafka server process.
- Partition is a topic that has been split.
- demo topic:
  Partition 0:  [off0][off1][off2][off3] →
  Partition 1:  [off0][off1] →
  Partition 2:  [off0][off1][off2] →


- imagine a topic called bet-placed. Instead of one giant list of every bet ever placed, kafka splits it into, say, 3 partitions:
    Partition 0: [msg] [msg] [msg] [msg] ...
    Partition 1: [msg] [msg] [msg] [msg] ...
    Partition 2: [msg] [msg] [msg] [msg] ...

- Why split it up?
1. Parallelism: different partitions can be processed by different consumers at the same time.
- With 3 partitions, you can have 3 consumers working in parallel instead of 1 consumer bottlenecking everything.

2. Ordering guarantee is per partition, not per topic. Kafka only guarantees message order within a single partition, not across the whole topic.

- if you don't specify a key, messages get spread round-robin across partitions, fine for throughput, but you lose ordering guarantees btn messages.

# OFFSET
- Position /index of a message withing its partition

    Partition 0:
    Offset:  0     1     2     3     4     5
    Message: [A]   [B]   [C]   [D]   [E]   [F]

**How consumers use offsets**
- A consumer tracks which offset its read up to per partition.
- Thats how kafka does "i've processed this far" book-keeping

- if your consumer has read upto offset 3 on partition 0, it knows the next message to fetch is 4. If your consumer crashes and restarts, it picks back up from offset 3.


- Contrary to Rabbit where when the consumer reads and acknowledges the messages, the message is removed from the queue, while kafka persists that message until a certain period is over, whether the message has been read or not.

- A broker is single kafka server process, the one kafka container I spun up is one broker. Kafka as a whole is a cluster of brokers working together; 

# Reliability
- Producer durability (don't lose messages on the way in) and consumer resilience (retries + DLQ on the way out).

- if you retry a bad message forever in place, you freeze the whole partition - every message behind it waits.
- Hence a bounded number of retries, then get it out of the way DLQ, and keep moving.
- DLQ - **normal topic(<topic>.DLT)** where failures go with diagnostic headers, for a human or separate repair process to inspect.


# DLQ
- Kafka has no built in DLQ, so you to implement it yourself, by creating a topic. eg demo.DLT
- consume → try to process (with a few retries) → still failing? → publish to demo.DLT (with error headers) → commit → move on
- its whole purpose is to get the posion message out of the way so one bad record doesn't freeze the partition behind it, while preserving it for later inspection instead of silently dropping it.

# DIFFERENCE BTN KAFKA AND RABBIT
**The core difference: Kafka is a durable log; RabbitMQ is a message broker/queue.** From that, Kafka's wins:

| | Kafka | RabbitMQ |
|---|---|---|
| **Model** | Retained log — messages stay after being read | Queue — message deleted on ack |
| **Replay** | ✅ Rewind offsets, re-read history | ❌ Once acked, it's gone |
| **Throughput** | Very high (millions/sec), scales via partitions | Lower ceiling |
| **Fan-out** | Many consumer groups each read *all* messages cheaply | Needs extra queues/bindings per consumer |
| **Ordering** | Guaranteed per partition | Weaker under concurrency/redelivery |
| **Retention / event sourcing** | ✅ Built for it; source-of-truth log | Not designed for it |
| **Ecosystem** | Streams, Connect, ksqlDB for stream processing | Mostly messaging |

- choose kafka when you need high-throughput streaming, replay, retention, or many independent consumers of the same data.
- Where RabbitMQ shines:
i. Flexible routing 
ii. Built in DLQ + retries
iii. Priority queues, per-message TTL
iv. Lower latency.

- Kafka = event streaming & data pipelines; RabbitMQ = task distribution & flexible message routing.
