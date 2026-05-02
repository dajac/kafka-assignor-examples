/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.dajac.assignor.canary;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CanaryAssignorTest {

    // ====================================================================
    // Test scaffolding
    // ====================================================================

    private static final Uuid TOPIC = Uuid.fromString("aaaaaaaaaaaaaaaaaaaaaa");
    private static final String CANARY_RACK = "canary";

    /** A test member that doubles as its own {@link MemberSubscription} and {@link MemberAssignment}. */
    private record TestMember(
        Optional<String> rackId,
        Set<Uuid> subscribedTopicIds,
        Map<Uuid, Set<Integer>> partitions
    ) implements MemberSubscription, MemberAssignment {
        @Override
        public Optional<String> instanceId() {
            return Optional.empty();
        }
    }

    /** A simple {@link GroupSpec} backed by a single member map. */
    private record TestGroupSpec(
        Map<String, TestMember> members,
        SubscriptionType subscriptionType
    ) implements GroupSpec {
        @Override
        public Collection<String> memberIds() {
            return members.keySet();
        }

        @Override
        public boolean isPartitionAssigned(Uuid topicId, int partitionId) {
            for (var m : members.values()) {
                Set<Integer> p = m.partitions().get(topicId);
                if (p != null && p.contains(partitionId)) return true;
            }
            return false;
        }

        @Override
        public boolean isPartitionAssignable(Uuid topicId, int partitionId) {
            return true;
        }

        @Override
        public MemberSubscription memberSubscription(String memberId) {
            return members.get(memberId);
        }

        @Override
        public MemberAssignment memberAssignment(String memberId) {
            var m = members.get(memberId);
            return m == null ? () -> Map.of() : m;
        }
    }

    /**
     * Fluent builder for a {@link GroupSpec}. Members are added in order;
     * iteration order is preserved (assignments are deterministic).
     */
    private static final class SpecBuilder {
        private final LinkedHashMap<String, TestMember> members = new LinkedHashMap<>();
        private SubscriptionType type = SubscriptionType.HOMOGENEOUS;

        /** Override the subscription type (default: HOMOGENEOUS). */
        SpecBuilder subscriptionType(SubscriptionType type) {
            this.type = type;
            return this;
        }

        /** Adds a member with the given rack, subscription, and current assignment. */
        SpecBuilder member(
            String memberId,
            Optional<String> rack,
            Set<Uuid> subscribedTopicIds,
            Map<Uuid, Set<Integer>> currentAssignment
        ) {
            members.put(memberId, new TestMember(rack, subscribedTopicIds, currentAssignment));
            return this;
        }

        GroupSpec build() {
            return new TestGroupSpec(new LinkedHashMap<>(members), type);
        }
    }

    /** Returns a {@link SubscribedTopicDescriber} that knows about the given topics. */
    private static SubscribedTopicDescriber describer(Map<Uuid, Integer> partitionsByTopic) {
        return new SubscribedTopicDescriber() {
            @Override
            public int numPartitions(Uuid id) {
                return partitionsByTopic.getOrDefault(id, -1);
            }

            @Override
            public Set<String> racksForPartition(Uuid id, int p) {
                return Set.of();
            }
        };
    }

    /** Builds a freshly-configured {@link CanaryAssignor}. */
    private static CanaryAssignor canaryAssignor(String rack, int maxPerCanary) {
        var assignor = new CanaryAssignor();
        assignor.configure(Map.of(
            CanaryAssignor.RACK_CONFIG, rack,
            CanaryAssignor.MAX_PARTITIONS_PER_MEMBER_CONFIG, String.valueOf(maxPerCanary)
        ));
        return assignor;
    }

    /** Asserts that {@code actual} equals {@code expected}, member-for-member. */
    private static void assertAssignment(
        Map<String, Map<Uuid, Set<Integer>>> expected,
        GroupAssignment actual
    ) {
        assertEquals(expected, actual.members().entrySet().stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().partitions())));
    }

    /** Returns a sorted set of integers - useful for stable iteration in prior assignments. */
    private static Set<Integer> sortedSetOf(int... ids) {
        var set = new TreeSet<Integer>();
        for (int id : ids) set.add(id);
        return set;
    }

    // ====================================================================
    // Tests
    // ====================================================================

    private final CanaryAssignor assignor = canaryAssignor(CANARY_RACK, 1);

    @Test
    void emptyGroupReturnsEmptyAssignment() {
        var spec = new SpecBuilder().build();
        assertAssignment(Map.of(), assignor.assign(spec, describer(Map.of(TOPIC, 12))));
    }

    @Test
    void heterogeneousSubscriptionRejected() {
        var spec = new SpecBuilder()
            .subscriptionType(SubscriptionType.HETEROGENEOUS)
            .member("m1", Optional.empty(), Set.of(TOPIC), Map.of())
            .build();
        assertThrows(PartitionAssignorException.class,
            () -> assignor.assign(spec, describer(Map.of(TOPIC, 12))));
    }

    @Test
    void missingTopicMetadataRejected() {
        var spec = new SpecBuilder()
            .member("m1", Optional.empty(), Set.of(TOPIC), Map.of())
            .build();
        assertThrows(PartitionAssignorException.class,
            () -> assignor.assign(spec, describer(Map.of())));
    }

    @Test
    void allRegularMembersBalanceUniformly() {
        var spec = new SpecBuilder()
            .member("m1", Optional.empty(), Set.of(TOPIC), Map.of())
            .member("m2", Optional.empty(), Set.of(TOPIC), Map.of())
            .member("m3", Optional.empty(), Set.of(TOPIC), Map.of())
            .build();

        assertAssignment(
            Map.of(
                "m1", Map.of(TOPIC, Set.of(0, 1, 2, 3)),
                "m2", Map.of(TOPIC, Set.of(4, 5, 6, 7)),
                "m3", Map.of(TOPIC, Set.of(8, 9, 10, 11))
            ),
            assignor.assign(spec, describer(Map.of(TOPIC, 12)))
        );
    }

    @Test
    void canaryCappedAtMax() {
        // 1 canary + 2 regulars, max per canary = 1. Canary takes one partition;
        // the floating-extra goes to m2 because m1 has nothing to keep so the
        // heuristic releases the extra to a downstream member.
        var spec = new SpecBuilder()
            .member("c1", Optional.of(CANARY_RACK), Set.of(TOPIC), Map.of())
            .member("m1", Optional.empty(), Set.of(TOPIC), Map.of())
            .member("m2", Optional.empty(), Set.of(TOPIC), Map.of())
            .build();

        assertAssignment(
            Map.of(
                "c1", Map.of(TOPIC, Set.of(0)),
                "m1", Map.of(TOPIC, Set.of(1, 2, 3, 4, 5)),
                "m2", Map.of(TOPIC, Set.of(6, 7, 8, 9, 10, 11))
            ),
            assignor.assign(spec, describer(Map.of(TOPIC, 12)))
        );
    }

    @Test
    void multipleCanariesEachGetMaxPartitions() {
        var assignor = canaryAssignor(CANARY_RACK, 2);
        // 2 canaries x 2 = 4 partitions for canaries; 2 regulars share 8.
        var spec = new SpecBuilder()
            .member("c1", Optional.of(CANARY_RACK), Set.of(TOPIC), Map.of())
            .member("c2", Optional.of(CANARY_RACK), Set.of(TOPIC), Map.of())
            .member("m1", Optional.empty(), Set.of(TOPIC), Map.of())
            .member("m2", Optional.empty(), Set.of(TOPIC), Map.of())
            .build();

        assertAssignment(
            Map.of(
                "c1", Map.of(TOPIC, Set.of(0, 1)),
                "c2", Map.of(TOPIC, Set.of(2, 3)),
                "m1", Map.of(TOPIC, Set.of(4, 5, 6, 7)),
                "m2", Map.of(TOPIC, Set.of(8, 9, 10, 11))
            ),
            assignor.assign(spec, describer(Map.of(TOPIC, 12)))
        );
    }

    @Test
    void memberWithNonCanaryRackTreatedAsRegular() {
        var spec = new SpecBuilder()
            .member("m1", Optional.of("production"), Set.of(TOPIC), Map.of())
            .member("m2", Optional.of("production"), Set.of(TOPIC), Map.of())
            .build();

        assertAssignment(
            Map.of(
                "m1", Map.of(TOPIC, Set.of(0, 1, 2, 3, 4, 5)),
                "m2", Map.of(TOPIC, Set.of(6, 7, 8, 9, 10, 11))
            ),
            assignor.assign(spec, describer(Map.of(TOPIC, 12)))
        );
    }

    @Test
    void stickyAssignmentPreservesPartitionsAcrossRebalance() {
        // Initial: m1 owns 0..5, m2 owns 6..11. m3 joins.
        var spec = new SpecBuilder()
            .member("m1", Optional.empty(), Set.of(TOPIC), Map.of(TOPIC, sortedSetOf(0, 1, 2, 3, 4, 5)))
            .member("m2", Optional.empty(), Set.of(TOPIC), Map.of(TOPIC, sortedSetOf(6, 7, 8, 9, 10, 11)))
            .member("m3", Optional.empty(), Set.of(TOPIC), Map.of())
            .build();

        // m1 and m2 each keep 4 of their existing partitions; m3 picks up the
        // 4 partitions revoked from m1 and m2.
        assertAssignment(
            Map.of(
                "m1", Map.of(TOPIC, Set.of(0, 1, 2, 3)),
                "m2", Map.of(TOPIC, Set.of(6, 7, 8, 9)),
                "m3", Map.of(TOPIC, Set.of(4, 5, 10, 11))
            ),
            assignor.assign(spec, describer(Map.of(TOPIC, 12)))
        );
    }

    @Test
    void floatingExtraSlotPreservesStickiness() {
        // 11 partitions across 3 members, prior owners are (3, 4, 4). With a
        // fixed quota distribution (extras to the first members) m1 would be
        // capped at 3 and one of its partitions would be moved to a fresh
        // slot. The floating extra should let m0 release its +1 so all three
        // members keep what they have.
        var spec = new SpecBuilder()
            .member("m0", Optional.empty(), Set.of(TOPIC), Map.of(TOPIC, sortedSetOf(0, 1, 2)))
            .member("m1", Optional.empty(), Set.of(TOPIC), Map.of(TOPIC, sortedSetOf(3, 4, 5, 6)))
            .member("m2", Optional.empty(), Set.of(TOPIC), Map.of(TOPIC, sortedSetOf(7, 8, 9, 10)))
            .build();

        assertAssignment(
            Map.of(
                "m0", Map.of(TOPIC, Set.of(0, 1, 2)),
                "m1", Map.of(TOPIC, Set.of(3, 4, 5, 6)),
                "m2", Map.of(TOPIC, Set.of(7, 8, 9, 10))
            ),
            assignor.assign(spec, describer(Map.of(TOPIC, 11)))
        );
    }

    @Test
    void stickyDropsPartitionsOfUnsubscribedTopic() {
        var otherTopic = Uuid.fromString("bbbbbbbbbbbbbbbbbbbbbb");
        // m1 was previously assigned partitions from TOPIC and otherTopic;
        // now only TOPIC is subscribed. otherTopic partitions are dropped.
        var spec = new SpecBuilder()
            .member("m1", Optional.empty(), Set.of(TOPIC), Map.of(
                TOPIC, sortedSetOf(0, 1, 2),
                otherTopic, sortedSetOf(0)
            ))
            .build();

        assertAssignment(
            Map.of("m1", Map.of(TOPIC, Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11))),
            assignor.assign(spec, describer(Map.of(TOPIC, 12)))
        );
    }

    @Test
    void requiresRackConfig() {
        var ex = assertThrows(Exception.class, () -> new CanaryAssignor().configure(Map.of()));
        assertTrue(ex.getMessage().contains(CanaryAssignor.RACK_CONFIG));
    }

    @Test
    void rejectsNegativeMaxPartitions() {
        var ex = assertThrows(Exception.class, () -> canaryAssignor(CANARY_RACK, -1));
        assertTrue(ex.getMessage().contains(CanaryAssignor.MAX_PARTITIONS_PER_MEMBER_CONFIG));
    }
}
