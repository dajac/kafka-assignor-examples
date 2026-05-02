# kafka-assignor-examples

A collection of examples of custom server-side partition assignors for the
Apache Kafka consumer rebalance protocol. These are *examples for learning*,
not production-grade implementations.

Currently included:

- **Canary assignor** &mdash; a configurable subset of consumers receive only
  a handful of partitions each so they can be exercised with a controlled
  blast radius.

## What is a server-side partition assignor?

[KIP-848][kip-848] introduced the **consumer rebalance protocol**, in which
the group coordinator (a broker) decides partition-to-consumer assignments
instead of one of the clients. The decision is made by a *server-side
partition assignor*: a stateless function that the group coordinator
invokes whenever its inputs change.

```
assign(group spec, previous assignment) -> new assignment
```

The function is invoked on:

- a member joining or leaving the group,
- a member's subscription changing,
- a topic being created or deleted,
- a topic gaining partitions.

A well-behaved assignor satisfies a few properties:

| Property | Required | Meaning |
|---|---|---|
| Uniqueness            | yes | Every partition is assigned to exactly one member. |
| Completeness          | strongly recommended | Every subscribed partition is assigned to some member. |
| Subscription respect  | strongly recommended | A member is never assigned a topic it isn't subscribed to. |
| Balance               | strongly recommended | Partitions are spread roughly uniformly within constraints. |
| Stickiness            | strongly recommended | Partition movement across rebalances is minimised. |
| Determinism           | strongly recommended | Same input produces the same output. |
| Performance           | strongly recommended | Aim for `O(M + P)` time and few allocations. |

The first three are correctness; the others are quality. The interesting
design space sits in the quality bucket &mdash; `CanaryAssignor` shows one
example. Performance lives there too because the assignor runs on the
broker for every group, so it must be fast enough not to impact others.

## Building a custom assignor

Add a single Maven dependency:

```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-group-coordinator-api</artifactId>
    <version>4.2.0</version>
    <scope>provided</scope>
</dependency>
```

`provided` because the broker brings the runtime; the resulting JAR
contains only your code.

Implement `ConsumerGroupPartitionAssignor` and, optionally, `Configurable`
(to receive the broker's config map at startup):

```java
public class MyAssignor implements ConsumerGroupPartitionAssignor, Configurable {
    @Override
    public String name() { 
      return "my-assignor";
    }

    @Override
    public void configure(Map<String, ?> configs) {
      /* read your configs */
    }

    @Override
    public GroupAssignment assign(GroupSpec spec, SubscribedTopicDescriber topics) {
        /* return the new assignment */
    }
}
```

The relevant types live in
`org.apache.kafka.coordinator.group.api.assignor`:

- **`GroupSpec`** &mdash; the input. Lists current members, their
  subscriptions (`MemberSubscription`), and their current assignment
  (`MemberAssignment`).
- **`SubscribedTopicDescriber`** &mdash; topic metadata: partition counts
  and rack information per partition.
- **`GroupAssignment`** &mdash; the output. Wraps
  `Map<String, MemberAssignment>` where each `MemberAssignment` is a
  `Map<topic-id, set-of-partitions>`.

## The canary assignor

`CanaryAssignor` looks at each member's `client.rack`. If it equals a
configured value, the member is a **canary** and is allotted at most
*N* partitions. The remaining partitions are split uniformly across
non-canary ("regular") members. Existing assignments are preserved across
rebalances whenever the new quotas allow it (stickiness).

A typical use case: deploy a new consumer image as a separate group of
instances configured with `client.rack=canary`, set
`canary.assignor.max.partitions.per.member=1`, and they will each handle
exactly one partition. The bulk of the load stays with the existing
fleet. Once the new image is healthy, you roll it out to the rest.

| Key                                           | Required | Default | Description |
|-----------------------------------------------|----------|---------|-------------|
| `canary.assignor.rack`                        | yes      | &mdash; | Rack id that identifies canary members. |
| `canary.assignor.max.partitions.per.member`   | no       | `1`     | Maximum partitions per canary member.   |

Only homogeneous subscriptions (all members subscribed to the same set of
topics) are supported. The assignor runs in `O(M + P)` where `M` is the
number of members and `P` is the total number of subscribed partitions.

## Build

```sh
./mvnw package
```

Produces `target/kafka-assignor-examples-0.1.0.jar`.

## Deploy to a Kafka broker

1. Copy the JAR onto every broker:
   ```sh
   cp target/kafka-assignor-examples-0.1.0.jar $KAFKA_HOME/libs/
   ```

2. Register the assignor in `server.properties`:
   ```properties
   group.consumer.assignors=uniform,range,dev.dajac.assignor.canary.CanaryAssignor

   canary.assignor.rack=canary
   canary.assignor.max.partitions.per.member=1
   ```

3. Rolling restart the brokers. No client changes are needed yet.

## Use it from a consumer

Set the consumer properties:

```properties
group.protocol=consumer
group.remote.assignor=canary

# Canary instances:
client.rack=canary

# Regular instances: omit client.rack, or set it to anything other than "canary".
```

## Run the demo locally

```sh
./mvnw package
docker compose up
```

The compose file runs a single Kafka 4.2.0 node in KRaft mode with the
freshly-built JAR mounted into `/opt/kafka/libs/`. See `docker-compose.yml`
for the full configuration.

## Try it

With the demo broker running, open another terminal and run:

```sh
# Create a 12-partition topic.
docker exec canary-assignor-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic events \
  --partitions 12

# Start one canary consumer (client.rack=canary).
docker exec -d canary-assignor-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events \
  --group demo \
  --consumer-property group.protocol=consumer \
  --consumer-property group.remote.assignor=canary \
  --consumer-property client.rack=canary

# Start two regular consumers (no client.rack).
for i in 1 2; do
  docker exec -d canary-assignor-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic events \
    --group demo \
    --consumer-property group.protocol=consumer \
    --consumer-property group.remote.assignor=canary
done

# Wait for the group to stabilise, then inspect the assignment.
docker exec canary-assignor-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group demo
```

The compose file caps canaries at 2 partitions, so you should see the canary
consumer owning 2 partitions while the two regular consumers split the
remaining 10 (5 each).

## Limitations of this example

- Homogeneous subscriptions only.
- No rack-awareness for partition placement; `racksForPartition` is unused.
- No dynamic reconfiguration; the configs are read once at startup.

## Further reading

- [Apache Kafka consumer documentation][kafka-docs]
- [Public Assignor API][gc-api]
- [JMH harnesses for the built-in assignors][jmh-assignor] &mdash; useful as
  a template for benchmarking your own.

[kip-848]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
[kafka-docs]: https://kafka.apache.org/documentation/
[gc-api]: https://javadoc.io/doc/org.apache.kafka/kafka-group-coordinator-api/4.2.0/org/apache/kafka/coordinator/group/api/assignor/package-summary.html
[jmh-assignor]: https://github.com/apache/kafka/tree/4.2.0/jmh-benchmarks/src/main/java/org/apache/kafka/jmh/assignor

## License

Apache License 2.0. See [LICENSE](LICENSE).
