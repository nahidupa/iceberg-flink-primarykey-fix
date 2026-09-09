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
import static org.assertj.core.api.SoftAssertions.assertSoftly;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

class TestCoordinatorPartitionCountVerification extends ChannelTestBase {
  private static final TopicPartition LEADER_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 0);
  private static final TopicPartition OTHER_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 1);
  private static final TopicPartition ADDED_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 2);
  private static final TopicPartition FOURTH_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 3);
  private static final TopicPartition CONTROL_PARTITION = new TopicPartition(CTL_TOPIC_NAME, 0);
  private static final OffsetDateTime REPORTED_TIMESTAMP =
      OffsetDateTime.parse("2026-09-09T12:00:00Z");

  private final List<Coordinator> coordinators = Lists.newArrayList();

  enum Scenario {
    STABLE_REFRESH_CONTROL,
    FAILED_REFRESH_WITHOUT_LOCAL_OPEN,
    UNSTABLE_REFRESH_WITHOUT_LOCAL_OPEN,
    EMPTY_REFRESH_WITHOUT_LOCAL_OPEN,
    ASSIGNMENT_CHANGED_DURING_REFRESH,
    NON_LEADER_REVOCATION
  }

  @AfterEach
  void stopCoordinators() {
    for (Coordinator coordinator : coordinators) {
      coordinator.terminate();
      coordinator.stop();
    }
  }

  @ParameterizedTest
  @EnumSource(Scenario.class)
  void incompleteTopologyMustNotPublishFullCompletion(Scenario scenario) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    SinkTaskContext context = mock(SinkTaskContext.class);
    CommitterImpl committer = new CommitterImpl();
    Set<TopicPartition> initialLeaderAssignment =
        scenario == Scenario.NON_LEADER_REVOCATION
            ? Set.of(LEADER_PARTITION, ADDED_PARTITION)
            : Set.of(LEADER_PARTITION);
    ConsumerGroupDescription initialGroup =
        group(
            ConsumerGroupState.STABLE,
            List.of(
                member("leader", initialLeaderAssignment),
                member("other", Set.of(OTHER_PARTITION))));
    ConsumerGroupDescription remoteExpansion =
        group(
            ConsumerGroupState.STABLE,
            List.of(
                member("leader", Set.of(LEADER_PARTITION)),
                member("other", Set.of(OTHER_PARTITION, ADDED_PARTITION))));

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
      kafkaUtils
          .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
          .thenReturn(initialGroup);
      when(context.assignment()).thenReturn(initialLeaderAssignment);
      committer.open(catalog, config, context, initialLeaderAssignment);
      assertThat(coordinators).hasSize(1);
      Coordinator coordinator = coordinators.get(0);
      coordinator.start();
      initConsumer();
      consumer.commitSync(Map.of(CONTROL_PARTITION, new OffsetAndMetadata(1L)));

      List<TopicPartition> reportingPartitions = List.of(OTHER_PARTITION, ADDED_PARTITION);
      switch (scenario) {
        case STABLE_REFRESH_CONTROL ->
            kafkaUtils
                .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
                .thenReturn(remoteExpansion);
        case FAILED_REFRESH_WITHOUT_LOCAL_OPEN ->
            kafkaUtils
                .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
                .thenThrow(new ConnectException("Injected describe failure"));
        case UNSTABLE_REFRESH_WITHOUT_LOCAL_OPEN -> {
          when(remoteExpansion.state()).thenReturn(ConsumerGroupState.PREPARING_REBALANCE);
          kafkaUtils
              .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
              .thenReturn(remoteExpansion);
        }
        case EMPTY_REFRESH_WITHOUT_LOCAL_OPEN -> {
          when(remoteExpansion.members()).thenReturn(List.of());
          kafkaUtils
              .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
              .thenReturn(remoteExpansion);
        }
        case ASSIGNMENT_CHANGED_DURING_REFRESH -> {
          Set<TopicPartition> expandedLeader = Set.of(LEADER_PARTITION, ADDED_PARTITION);
          ConsumerGroupDescription expandedGroup =
              group(
                  ConsumerGroupState.STABLE,
                  List.of(
                      member("leader", expandedLeader), member("other", Set.of(OTHER_PARTITION))));
          AtomicBoolean callbackPending = new AtomicBoolean(true);
          kafkaUtils
              .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
              .thenAnswer(
                  invocation -> {
                    if (callbackPending.compareAndSet(true, false)) {
                      when(context.assignment()).thenReturn(expandedLeader);
                      committer.open(catalog, config, context, Set.of(ADDED_PARTITION));
                      return initialGroup;
                    }

                    return expandedGroup;
                  });
          reportingPartitions = List.of(LEADER_PARTITION, ADDED_PARTITION);
        }
        case NON_LEADER_REVOCATION -> {
          coordinator.process();
          ConsumerGroupDescription expandedGroup =
              group(
                  ConsumerGroupState.STABLE,
                  List.of(
                      member("leader", Set.of(LEADER_PARTITION)),
                      member("other", Set.of(OTHER_PARTITION, ADDED_PARTITION, FOURTH_PARTITION))));
          kafkaUtils
              .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
              .thenReturn(expandedGroup);
          when(context.assignment()).thenReturn(Set.of(LEADER_PARTITION));
          committer.close(Set.of(ADDED_PARTITION));
          reportingPartitions = List.of(OTHER_PARTITION, ADDED_PARTITION, FOURTH_PARTITION);
        }
      }

      if (scenario != Scenario.NON_LEADER_REVOCATION) {
        coordinator.process();
      }

      assertThat(factories.constructed()).hasSize(1);
      assertThat(threads.constructed()).hasSize(1);
      verify(threads.constructed().get(0), never()).terminate();
      assertWaitsForRemainingPartitions(coordinator, reportingPartitions, scenario);
    }
  }

  private void assertWaitsForRemainingPartitions(
      Coordinator coordinator, List<TopicPartition> reportingPartitions, Scenario scenario) {
    UUID commitId =
        producer.history().stream()
            .map(record -> AvroUtil.decode(record.value()).payload())
            .filter(StartCommit.class::isInstance)
            .map(StartCommit.class::cast)
            .map(StartCommit::commitId)
            .findFirst()
            .orElseThrow();
    DataWritten written =
        new DataWritten(
            StructType.of(),
            commitId,
            TableReference.of("catalog", TABLE_IDENTIFIER, table.uuid()),
            List.of(EventTestUtil.createDataFile()),
            List.of());
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME,
            0,
            1L,
            "writer",
            AvroUtil.encode(new Event(CONNECT_CONSUMER_GROUP_ID, written))));
    DataComplete ready =
        new DataComplete(
            commitId,
            reportingPartitions.stream()
                .map(
                    partition ->
                        new TopicPartitionOffset(
                            partition.topic(), partition.partition(), 1L, REPORTED_TIMESTAMP))
                .toList());
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME,
            0,
            2L,
            "writer",
            AvroUtil.encode(new Event(CONNECT_CONSUMER_GROUP_ID, ready))));
    coordinator.process();
    table.refresh();

    assertSoftly(
        assertions -> {
          assertions
              .assertThat(table.currentSnapshot())
              .as("Missing source partition must prevent full snapshot: %s", scenario)
              .isNull();
          assertions
              .assertThat(
                  producer.history().stream()
                      .map(record -> AvroUtil.decode(record.value()).payload())
                      .filter(CommitComplete.class::isInstance)
                      .map(CommitComplete.class::cast)
                      .map(CommitComplete::validThroughTs)
                      .toList())
              .as("No completeness watermark before all source partitions report")
              .isEmpty();
          assertions
              .assertThat(consumer.committed(Set.of(CONTROL_PARTITION)))
              .containsEntry(CONTROL_PARTITION, new OffsetAndMetadata(1L));
        });
  }

  private static MemberDescription member(String name, Set<TopicPartition> partitions) {
    return new MemberDescription(
        name, Optional.empty(), name, "host", new MemberAssignment(partitions));
  }

  private static ConsumerGroupDescription group(
      ConsumerGroupState state, List<MemberDescription> members) {
    ConsumerGroupDescription description = mock(ConsumerGroupDescription.class);
    when(description.state()).thenReturn(state);
    when(description.members()).thenReturn(members);
    return description;
  }
}
