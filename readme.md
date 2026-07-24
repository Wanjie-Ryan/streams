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