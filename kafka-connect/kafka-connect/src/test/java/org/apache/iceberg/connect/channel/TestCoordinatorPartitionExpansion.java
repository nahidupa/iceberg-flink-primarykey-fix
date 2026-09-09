/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.channel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

class TestCoordinatorPartitionExpansion extends ChannelTestBase {
  private static final TopicPartition LEADER_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 0);
  private static final TopicPartition OTHER_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 1);
  private static final TopicPartition ADDED_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 2);
  private static final TopicPartition CONTROL_PARTITION = new TopicPartition(CTL_TOPIC_NAME, 0);
  private static final OffsetDateTime LEADER_TIMESTAMP =
      OffsetDateTime.parse("2026-09-08T12:00:00Z");
  private static final OffsetDateTime OTHER_TIMESTAMP = LEADER_TIMESTAMP.minusHours(1);

  private final List<Coordinator> coordinators = Lists.newArrayList();

  @AfterEach
  void stopCoordinators() {
    for (Coordinator coordinator : coordinators) {
      coordinator.terminate();
      coordinator.stop();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void retainedLeaderWaitsForAllExpandedPartitions(boolean includeRetainedPartition) {
    verifyExpandedQuorum(true, includeRetainedPartition);
  }

  @Test
  void freshCoordinatorWaitsForAllExpandedPartitions() {
    verifyExpandedQuorum(false, false);
  }

  /**
   * A count that could not be verified must not authorize a full completion. The group is
   * unreadable or mid-rebalance when the cycle starts, so the stale count of two would otherwise
   * satisfy readiness once the leader reports its two partitions.
   */
  @ParameterizedTest
  @ValueSource(strings = {"unstable", "unavailable"})
  void unverifiedCountCannotPublishACompletenessWatermark(String failureMode) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    SinkTaskContext context = mock(SinkTaskContext.class);
    ConsumerGroupDescription group = mock(ConsumerGroupDescription.class);
    CommitterImpl committer = new CommitterImpl();

    try (MockedConstruction<KafkaClientFactory> factories =
            mockConstruction(
                KafkaClientFactory.class,
                (factory, construction) -> {
                  when(factory.createProducer(any())).thenReturn(producer);
                  when(factory.createConsumer(any())).thenReturn(consumer);
                  when(factory.createAdmin()).thenReturn(admin);
                });
        MockedConstruction<CoordinatorThread> threads =
            mockConstruction(
                CoordinatorThread.class,
                (thread, construction) ->
                    coordinators.add((Coordinator) construction.arguments().get(0)));
        MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class)) {
      when(group.state()).thenReturn(ConsumerGroupState.STABLE);
      when(group.members())
          .thenReturn(
              List.of(
                  member("leader", Set.of(LEADER_PARTITION)),
                  member("other", Set.of(OTHER_PARTITION))));
      kafkaUtils
          .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
          .thenReturn(group);

      when(context.assignment()).thenReturn(Set.of(LEADER_PARTITION));
      committer.open(catalog, config, context, Set.of(LEADER_PARTITION));
      assertThat(coordinators).hasSize(1);
      initializeCoordinator();
      Coordinator coordinator = coordinators.get(0);

      // the assignment expands, but the group can no longer be verified
      Set<TopicPartition> expandedAssignment = Set.of(LEADER_PARTITION, ADDED_PARTITION);
      when(context.assignment()).thenReturn(expandedAssignment);
      committer.open(catalog, config, context, expandedAssignment);
      if ("unstable".equals(failureMode)) {
        when(group.state()).thenReturn(ConsumerGroupState.PREPARING_REBALANCE);
      } else {
        kafkaUtils
            .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
            .thenThrow(new ConnectException("group unavailable"));
      }

      coordinator.process();
      UUID commitId = currentCommitId();
      writeFile(commitId, dataFile("leader"), 1L);
      ready(
          commitId,
          List.of(
              assignment(LEADER_PARTITION, LEADER_TIMESTAMP),
              assignment(ADDED_PARTITION, LEADER_TIMESTAMP)),
          2L);
      coordinator.process();

      table.refresh();
      assertThat(table.currentSnapshot())
          .as("an unverified partition count must not authorize a full commit")
          .isNull();
      assertThat(completions()).isEmpty();
      assertCheckpoint(1L);
    }
  }

  /**
   * A count verified at the start of a cycle can be invalidated by an assignment change before the
   * responses arrive. The cycle must lose its full-completion eligibility rather than settle on the
   * topology it was opened with.
   */
  @Test
  void assignmentChangeDuringACycleWithdrawsFullCompletion() {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    SinkTaskContext context = mock(SinkTaskContext.class);
    ConsumerGroupDescription group = mock(ConsumerGroupDescription.class);
    CommitterImpl committer = new CommitterImpl();

    try (MockedConstruction<KafkaClientFactory> factories =
            mockConstruction(
                KafkaClientFactory.class,
                (factory, construction) -> {
                  when(factory.createProducer(any())).thenReturn(producer);
                  when(factory.createConsumer(any())).thenReturn(consumer);
                  when(factory.createAdmin()).thenReturn(admin);
                });
        MockedConstruction<CoordinatorThread> threads =
            mockConstruction(
                CoordinatorThread.class,
                (thread, construction) ->
                    coordinators.add((Coordinator) construction.arguments().get(0)));
        MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class)) {
      when(group.state()).thenReturn(ConsumerGroupState.STABLE);
      when(group.members())
          .thenReturn(
              List.of(
                  member("leader", Set.of(LEADER_PARTITION)),
                  member("other", Set.of(OTHER_PARTITION))));
      kafkaUtils
          .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
          .thenReturn(group);

      when(context.assignment()).thenReturn(Set.of(LEADER_PARTITION));
      committer.open(catalog, config, context, Set.of(LEADER_PARTITION));
      initializeCoordinator();
      Coordinator coordinator = coordinators.get(0);

      // the cycle opens against a verified two-partition topology
      coordinator.process();
      UUID commitId = currentCommitId();

      // expansion lands after StartCommit, before any response
      Set<TopicPartition> expandedAssignment = Set.of(LEADER_PARTITION, ADDED_PARTITION);
      when(context.assignment()).thenReturn(expandedAssignment);
      when(group.members())
          .thenReturn(
              List.of(
                  member("leader", expandedAssignment), member("other", Set.of(OTHER_PARTITION))));
      committer.open(catalog, config, context, Set.of(ADDED_PARTITION));

      writeFile(commitId, dataFile("leader"), 1L);
      ready(
          commitId,
          List.of(
              assignment(LEADER_PARTITION, LEADER_TIMESTAMP),
              assignment(ADDED_PARTITION, LEADER_TIMESTAMP)),
          2L);
      coordinator.process();

      table.refresh();
      assertThat(table.currentSnapshot())
          .as("a cycle whose assignment changed must not claim completeness")
          .isNull();
      assertThat(completions()).isEmpty();
      assertCheckpoint(1L);
    }
  }

  private void verifyExpandedQuorum(boolean retainCoordinator, boolean includeRetainedPartition) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    SinkTaskContext context = mock(SinkTaskContext.class);
    ConsumerGroupDescription group = mock(ConsumerGroupDescription.class);
    CommitterImpl committer = new CommitterImpl();

    try (MockedConstruction<KafkaClientFactory> factories =
            mockConstruction(
                KafkaClientFactory.class,
                (factory, construction) -> {
                  when(factory.createProducer(any())).thenReturn(producer);
                  when(factory.createConsumer(any())).thenReturn(consumer);
                  when(factory.createAdmin()).thenReturn(admin);
                });
        MockedConstruction<CoordinatorThread> threads =
            mockConstruction(
                CoordinatorThread.class,
                (thread, construction) ->
                    coordinators.add((Coordinator) construction.arguments().get(0)));
        MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class)) {
      when(group.state()).thenReturn(ConsumerGroupState.STABLE);
      kafkaUtils
          .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
          .thenReturn(group);

      if (retainCoordinator) {
        when(context.assignment()).thenReturn(Set.of(LEADER_PARTITION));
        when(group.members())
            .thenReturn(
                List.of(
                    member("leader", Set.of(LEADER_PARTITION)),
                    member("other", Set.of(OTHER_PARTITION))));
        committer.open(catalog, config, context, Set.of(LEADER_PARTITION));
        assertThat(coordinators).hasSize(1);
        initializeCoordinator();
      }

      Set<TopicPartition> expandedAssignment = Set.of(LEADER_PARTITION, ADDED_PARTITION);
      when(context.assignment()).thenReturn(expandedAssignment);
      when(group.members())
          .thenReturn(
              List.of(
                  member("leader", expandedAssignment), member("other", Set.of(OTHER_PARTITION))));
      committer.open(
          catalog,
          config,
          context,
          retainCoordinator && !includeRetainedPartition
              ? Set.of(ADDED_PARTITION)
              : expandedAssignment);

      assertThat(factories.constructed()).hasSize(1);
      assertThat(threads.constructed()).hasSize(1);
      verify(threads.constructed().get(0)).start();
      verify(threads.constructed().get(0), never()).terminate();
      assertThat(coordinators).hasSize(1);
      Coordinator coordinator = coordinators.get(0);
      if (!retainCoordinator) {
        initializeCoordinator();
      }

      coordinator.process();
      UUID commitId = currentCommitId();

      DataFile leaderFile = dataFile("leader");
      DataFile otherFile = dataFile("other");
      writeFile(commitId, leaderFile, 1L);
      ready(
          commitId,
          List.of(
              assignment(LEADER_PARTITION, LEADER_TIMESTAMP),
              assignment(ADDED_PARTITION, LEADER_TIMESTAMP)),
          2L);
      coordinator.process();

      table.refresh();
      assertThat(table.currentSnapshot())
          .as("two of three source partitions must not produce a full commit")
          .isNull();
      assertThat(completions()).isEmpty();
      assertCheckpoint(1L);

      writeFile(commitId, otherFile, 3L);
      ready(commitId, List.of(assignment(OTHER_PARTITION, OTHER_TIMESTAMP)), 4L);
      coordinator.process();

      table.refresh();
      assertThat(table.snapshots()).hasSize(1);
      assertAddedFiles(leaderFile, otherFile);
      assertThat(table.currentSnapshot().summary())
          .containsEntry(VALID_THROUGH_TS_SNAPSHOT_PROP, OTHER_TIMESTAMP.toString());
      assertThat(completions())
          .extracting(CommitComplete::validThroughTs)
          .containsExactly(OTHER_TIMESTAMP);
      assertCheckpoint(5L);
    }
  }

  private void initializeCoordinator() {
    coordinators.get(0).start();
    initConsumer();
    consumer.commitSync(Map.of(CONTROL_PARTITION, new OffsetAndMetadata(1L)));
  }

  private void assertAddedFiles(DataFile... expected) {
    assertThat(
            SnapshotChanges.builderFor(table)
                .snapshot(table.currentSnapshot())
                .build()
                .addedDataFiles())
        .extracting(DataFile::location)
        .containsExactlyInAnyOrderElementsOf(
            List.of(expected).stream().map(DataFile::location).toList());
  }

  private void writeFile(UUID commitId, DataFile file, long controlOffset) {
    DataWritten written =
        new DataWritten(
            StructType.of(),
            commitId,
            TableReference.of("catalog", TABLE_IDENTIFIER, table.uuid()),
            List.of(file),
            List.of());
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME,
            0,
            controlOffset,
            "key",
            AvroUtil.encode(new Event(CONNECT_CONSUMER_GROUP_ID, written))));
  }

  private void ready(UUID commitId, List<TopicPartitionOffset> assignments, long controlOffset) {
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME,
            0,
            controlOffset,
            "key",
            AvroUtil.encode(
                new Event(CONNECT_CONSUMER_GROUP_ID, new DataComplete(commitId, assignments)))));
  }

  private UUID currentCommitId() {
    return producer.history().stream()
        .map(record -> AvroUtil.decode(record.value()).payload())
        .filter(StartCommit.class::isInstance)
        .map(StartCommit.class::cast)
        .map(StartCommit::commitId)
        .reduce((previous, current) -> current)
        .orElseThrow();
  }

  private List<CommitComplete> completions() {
    return producer.history().stream()
        .map(record -> AvroUtil.decode(record.value()).payload())
        .filter(CommitComplete.class::isInstance)
        .map(CommitComplete.class::cast)
        .toList();
  }

  private void assertCheckpoint(long offset) {
    assertThat(consumer.committed(Set.of(CONTROL_PARTITION)))
        .containsEntry(CONTROL_PARTITION, new OffsetAndMetadata(offset));
  }

  private static TopicPartitionOffset assignment(
      TopicPartition partition, OffsetDateTime timestamp) {
    return new TopicPartitionOffset(partition.topic(), partition.partition(), 1L, timestamp);
  }

  private static MemberDescription member(String name, Set<TopicPartition> partitions) {
    return new MemberDescription(
        name, Optional.empty(), name, "host", new MemberAssignment(partitions));
  }

  private static DataFile dataFile(String name) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(name + ".parquet")
        .withFileSizeInBytes(100)
        .withRecordCount(1)
        .build();
  }
}
