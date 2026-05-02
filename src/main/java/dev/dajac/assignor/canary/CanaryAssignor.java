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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A KIP-848 server-side partition assignor that supports canary deployments.
 *
 * <p>Members whose {@code client.rack} equals the configured canary rack are
 * treated as canary members and receive at most {@code N} partitions each. The
 * remaining partitions are distributed uniformly among non-canary members.
 * Existing assignments are preserved across rebalances whenever the new quotas
 * allow it.
 *
 * <p>Only homogeneous subscriptions (all members subscribed to the same set of
 * topics) are supported. The assignment runs in {@code O(M + P)} where
 * {@code M} is the number of members and {@code P} is the total number of
 * subscribed partitions.
 *
 * <p>Configuration:
 * <ul>
 *   <li>{@code canary.assignor.rack} &mdash; rack id that identifies canary
 *       members. Members whose {@code client.rack} equals this value are
 *       canary; everyone else (including members without a rack) is regular.
 *       Required.</li>
 *   <li>{@code canary.assignor.max.partitions.per.member} &mdash; maximum
 *       partitions per canary member. Default: 1.</li>
 * </ul>
 */
public class CanaryAssignor implements ConsumerGroupPartitionAssignor, Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(CanaryAssignor.class);

    /** Name of this assignor. Used by clients in {@code group.remote.assignor}. */
    public static final String NAME = "canary";

    /** Configuration key for the rack id that identifies canary members. */
    public static final String RACK_CONFIG = "canary.assignor.rack";

    /** Configuration key for the maximum partitions per canary member. */
    public static final String MAX_PARTITIONS_PER_MEMBER_CONFIG = "canary.assignor.max.partitions.per.member";

    /** Default value for {@link #MAX_PARTITIONS_PER_MEMBER_CONFIG}. */
    public static final int DEFAULT_MAX_PARTITIONS_PER_MEMBER = 1;

    /** Rack id that identifies canary members. */
    private String canaryRack;

    /** Maximum number of partitions assigned to each canary member. */
    private int maxPartitionsPerCanaryMember;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Required: the rack id that identifies canary members.
        Object rackValue = configs.get(RACK_CONFIG);
        if (rackValue == null) {
            throw new ConfigException("Missing required configuration: " + RACK_CONFIG);
        }
        canaryRack = rackValue.toString();

        // Optional: the per-canary partition cap. Defaults to 1.
        Object maxPartitionsValue = configs.get(MAX_PARTITIONS_PER_MEMBER_CONFIG);
        if (maxPartitionsValue != null) {
            maxPartitionsPerCanaryMember = Integer.parseInt(maxPartitionsValue.toString());
            if (maxPartitionsPerCanaryMember < 0) {
                throw new ConfigException(MAX_PARTITIONS_PER_MEMBER_CONFIG + " must be non-negative.");
            }
        } else {
            maxPartitionsPerCanaryMember = DEFAULT_MAX_PARTITIONS_PER_MEMBER;
        }

        LOG.info("Configured canary assignor: canaryRack={}, maxPartitionsPerCanaryMember={}",
            canaryRack, maxPartitionsPerCanaryMember);
    }

    @Override
    public GroupAssignment assign(
        GroupSpec spec,
        SubscribedTopicDescriber describer
    ) throws PartitionAssignorException {
        // Nothing to do for an empty group.
        if (spec.memberIds().isEmpty()) {
            return new GroupAssignment(Map.of());
        }

        // The algorithm only handles uniform subscriptions.
        if (spec.subscriptionType() != SubscriptionType.HOMOGENEOUS) {
            throw new PartitionAssignorException(
                "CanaryAssignor only supports homogeneous subscriptions."
            );
        }

        return new UniformBuilder(spec, describer).build();
    }

    /** A subscribed partition identified by its topic id and partition number. */
    private record TopicIdPartition(
        Uuid topicId,
        int partitionId
    ) {}

    /** A member with free quota slots and a direct reference to its assignment. */
    private record UnfilledMember(
        Assignment assignment,
        int remainingQuota
    ) {}

    /** A {@link MemberAssignment} that supports incremental mutation. */
    private record Assignment(
        Map<Uuid, Set<Integer>> partitions
    ) implements MemberAssignment {
        /** Creates an empty assignment. */
        Assignment() {
            this(new HashMap<>());
        }

        /** Adds a partition to this assignment. */
        void add(Uuid topicId, int partition) {
            partitions.computeIfAbsent(topicId, __ -> new HashSet<>()).add(partition);
        }
    }

    /**
     * Builds a partition assignment for a non-empty group with homogeneous
     * subscriptions. A fresh instance is created for each call to {@link #assign}.
     */
    private final class UniformBuilder {
        /** The group's specification: members, subscriptions, and current assignment. */
        private final GroupSpec spec;

        /** Topic metadata: partition counts and rack information. */
        private final SubscribedTopicDescriber describer;

        /** Topic ids the group is subscribed to. */
        private final Set<Uuid> subscribedTopicIds;

        /** Members whose rack matches the canary rack. */
        private final List<String> canaryMembers = new ArrayList<>();

        /** All other members (no rack, or a rack other than the canary rack). */
        private final List<String> regularMembers = new ArrayList<>();

        /** Subscribed partitions not yet assigned to any member. */
        private final List<TopicIdPartition> unassignedPartitions = new ArrayList<>();

        /** Members that still have free quota slots. */
        private final List<UnfilledMember> unfilledMembers = new ArrayList<>();

        /** The output assignment, built incrementally and returned as-is. */
        private final Map<String, MemberAssignment> targetAssignment = new HashMap<>();

        /** Total subscribed partition count across all topics. */
        private int totalPartitions;

        UniformBuilder(GroupSpec spec, SubscribedTopicDescriber describer) {
            this.spec = spec;
            this.describer = describer;

            // Every member shares the same subscription under HOMOGENEOUS,
            // so any one will do.
            this.subscribedTopicIds = spec
                .memberSubscription(spec.memberIds().iterator().next())
                .subscribedTopicIds();
        }

        /** Builds the partition assignment for the group. */
        GroupAssignment build() {
            if (subscribedTopicIds.isEmpty()) {
                return new GroupAssignment(Map.of());
            }

            // Step 1 - gather the inputs: which partitions need placing, and
            // which members belong to the canary vs regular sub-group.
            findUnassignedPartitions();
            classifyMembers();

            // Step 2 - sticky-retain what each member already owns, up to its
            // share of the sub-group quota. Anything not retained joins the
            // unassigned pool.
            int canaryQuota = Math.min(canaryMembers.size() * maxPartitionsPerCanaryMember, totalPartitions);
            maybeRevokePartitionsFor(canaryMembers, canaryQuota);
            maybeRevokePartitionsFor(regularMembers, totalPartitions - canaryQuota);

            // Step 3 - drain the unassigned pool into the members that still
            // have free quota slots.
            assignRemainingPartitions();

            return new GroupAssignment(targetAssignment);
        }

        /** Populates {@link #unassignedPartitions} and {@link #totalPartitions}. */
        private void findUnassignedPartitions() {
            for (Uuid topicId : subscribedTopicIds) {
                int numPartitions = describer.numPartitions(topicId);
                if (numPartitions == -1) {
                    throw new PartitionAssignorException(
                        "Subscribed topic " + topicId + " not found in topic metadata."
                    );
                }

                // Track partitions not currently owned by any member.
                for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                    if (!spec.isPartitionAssigned(topicId, partitionId)) {
                        unassignedPartitions.add(new TopicIdPartition(topicId, partitionId));
                    }
                }

                totalPartitions += numPartitions;
            }
        }

        /** Populates {@link #canaryMembers} and {@link #regularMembers}. */
        private void classifyMembers() {
            for (String memberId : spec.memberIds()) {
                if (isCanary(memberId)) {
                    canaryMembers.add(memberId);
                } else {
                    regularMembers.add(memberId);
                }
            }
        }

        /** Returns whether the given member belongs to the canary sub-group. */
        private boolean isCanary(String memberId) {
            return spec
                .memberSubscription(memberId)
                .rackId()
                .filter(canaryRack::equals)
                .isPresent();
        }

        /**
         * Distributes {@code subgroupTotal} partitions evenly across {@code members},
         * applying sticky retention to each via {@link #maybeRevokeMemberPartitions}.
         */
        private void maybeRevokePartitionsFor(List<String> members, int subgroupTotal) {
            int memberCount = members.size();
            if (memberCount == 0) return;

            // Each member gets at least `min` partitions; the first `extras`
            // of them get one more.
            int min = subgroupTotal / memberCount;
            int extras = subgroupTotal % memberCount;

            int memberIndex = 0;
            for (String memberId : members) {
                int remainingMembers = memberCount - memberIndex - 1;
                extras = maybeRevokeMemberPartitions(memberId, min, extras, remainingMembers);
                memberIndex++;
            }
        }

        /**
         * Keeps the member's existing partitions up to its quota, revoking anything
         * else into {@link #unassignedPartitions}. Releases the floating extra slot
         * back to downstream members if this one cannot fill it.
         *
         * @return the (possibly decremented) extras for the next member.
         */
        private int maybeRevokeMemberPartitions(String memberId, int min, int extras, int remainingMembers) {
            // Compute the member's quota: the minimum, plus one extra if any
            // are still available in the sub-group's pool.
            int quota = min;
            boolean takesExtra = extras > 0;
            if (takesExtra) quota++;

            // Walk the member's existing assignment. Keep partitions for
            // subscribed topics up to the quota; revoke anything else (over
            // quota OR for an unsubscribed topic) into the unassigned pool.
            Assignment newAssignment = new Assignment();
            targetAssignment.put(memberId, newAssignment);

            for (Map.Entry<Uuid, Set<Integer>> entry : spec.memberAssignment(memberId).partitions().entrySet()) {
                Uuid topicId = entry.getKey();
                if (!subscribedTopicIds.contains(topicId)) continue;

                for (Integer partitionId : entry.getValue()) {
                    if (quota > 0) {
                        newAssignment.add(topicId, partitionId);
                        quota--;
                    } else {
                        unassignedPartitions.add(new TopicIdPartition(topicId, partitionId));
                    }
                }
            }

            // If the member couldn't fill its extra slot from existing partitions
            // and downstream members can still absorb the remaining extras,
            // release the extra back into the pool so a fuller downstream
            // member can claim it. This protects stickiness.
            if (quota > 0 && takesExtra && remainingMembers >= extras) {
                quota--;
                takesExtra = false;
            }
            if (takesExtra) extras--;

            // Record the result, and queue the member as unfilled if it still
            // has free quota slots.
            if (quota > 0) {
                unfilledMembers.add(new UnfilledMember(newAssignment, quota));
            }

            return extras;
        }

        /** Distributes the unassigned partitions across the members still below their quota. */
        private void assignRemainingPartitions() {
            int nextUnassigned = 0;
            for (UnfilledMember unfilled : unfilledMembers) {
                for (int slot = 0; slot < unfilled.remainingQuota(); slot++) {
                    if (nextUnassigned == unassignedPartitions.size()) {
                        throw new PartitionAssignorException(
                            "Bug: ran out of partitions while members still need more."
                        );
                    }

                    TopicIdPartition partition = unassignedPartitions.get(nextUnassigned++);
                    unfilled.assignment().add(partition.topicId(), partition.partitionId());
                }
            }
        }
    }
}
