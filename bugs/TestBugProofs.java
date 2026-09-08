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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.IcebergWriterResult;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

/** SCRATCH: proofs for audit findings. Not for commit. */
public class TestBugProofs extends ChannelTestBase {

  private static MemberDescription memberWith(int... srcPartitions) {
    ImmutableSet.Builder<TopicPartition> tps = ImmutableSet.builder();
    for (int p : srcPartitions) {
      tps.add(new TopicPartition(SRC_TOPIC_NAME, p));
    }
    return new MemberDescription(null, Optional.empty(), null, null, new MemberAssignment(tps.build()));
  }

  private Coordinator newCoordinator(MemberDescription... members) {
    return new Coordinator(
        catalog, config, ImmutableList.copyOf(members), clientFactory, mock(SinkTaskContext.class));
  }

  private UUID startCommit(Coordinator coordinator) {
    coordinator.process();
    byte[] bytes = producer.history().get(producer.history().size() - 1).value();
    return ((StartCommit) AvroUtil.decode(bytes).payload()).commitId();
  }

  private void deliverFileAndComplete(UUID commitId, DataFile file, long baseOffset) {
    Event written =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitId,
                TableReference.of("catalog", TABLE_IDENTIFIER, table.uuid()),
                ImmutableList.of(file),
                ImmutableList.of()));
    Event complete =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId,
                ImmutableList.of(new TopicPartitionOffset(SRC_TOPIC_NAME, 0, 1L, null))));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, baseOffset, "key", AvroUtil.encode(written)));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, baseOffset + 1, "key", AvroUtil.encode(complete)));
  }

  // ---------------------------------------------------------------- L3
  // A commit must never be "ready" when the coordinator has heard from nobody.
  @Test
  public void proofL3_commitIsReadyWithZeroResponses() {
    CommitState state = new CommitState(config);
    state.startNewCommit();

    assertThat(state.isCommitReady(0))
        .as("a commit with zero expected AND zero received partitions must not be ready")
        .isFalse();
  }

  // ---------------------------------------------------------------- H2
  // A DataWritten for a table that is temporarily missing must not be silently dropped.
  @Test
  public void proofH2_missingTableSilentlyDiscardsBufferedFiles() {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    Coordinator coordinator = newCoordinator(memberWith(0));
    coordinator.start();
    initConsumer();

    // the table vanishes between write and commit
    doThrow(new NoSuchTableException("table dropped")).when(catalog).loadTable(any());

    UUID commitId = startCommit(coordinator);
    DataFile file = EventTestUtil.createDataFile();
    deliverFileAndComplete(commitId, file, 1L);
    coordinator.process(); // commit fires, commitToTable returns early, clearResponses() runs

    // the table comes back
    doCallRealMethod().when(catalog).loadTable(any());

    // a later cycle with no new data: the earlier file should still be committed
    UUID second = startCommit(coordinator);
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME,
            0,
            10L,
            "key",
            AvroUtil.encode(
                new Event(
                    config.connectGroupId(),
                    new DataComplete(
                        second,
                        ImmutableList.of(
                            new TopicPartitionOffset(SRC_TOPIC_NAME, 0, 2L, null)))))));
    coordinator.process();

    table.refresh();
    assertThat(table.snapshots())
        .as("file buffered while the table was missing must not be lost")
        .isNotEmpty();
  }

  // ---------------------------------------------------------------- M1
  // A Kafka offset-commit failure after a successful Iceberg commit must not kill the task.
  @Test
  public void proofM1_kafkaCommitFailedExceptionIsTreatedAsFatal() {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    Consumer<String, byte[]> spied = spy(consumer);
    doThrow(new org.apache.kafka.clients.consumer.CommitFailedException())
        .when(spied)
        .commitSync(anyMap());
    when(clientFactory.createConsumer(any())).thenReturn(spied);

    Coordinator coordinator = newCoordinator(memberWith(0));
    coordinator.start();
    initConsumer();

    UUID commitId = startCommit(coordinator);
    deliverFileAndComplete(commitId, EventTestUtil.createDataFile(), 1L);

    assertThatCode(coordinator::process)
        .as("a Kafka offset-commit failure must be retried, not propagated as fatal")
        .doesNotThrowAnyException();
  }

  // ---------------------------------------------------------------- H3
  // totalPartitionCount is frozen at construction from a possibly-partial member snapshot.
  // A commit must not be declared complete, nor stamp a watermark, while partitions are missing.
  @Test
  public void proofH3_staleTotalPartitionCountCommitsEarlyAndStampsWatermark() {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    // The group really has two source partitions, but the describeConsumerGroups snapshot
    // captured mid-rebalance only reported one member holding one of them.
    Coordinator coordinator = newCoordinator(memberWith(0));
    coordinator.start();
    initConsumer();

    UUID commitId = startCommit(coordinator);

    // only the worker owning source partition 0 reports; partition 1's worker is still writing
    Event written =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitId,
                TableReference.of("catalog", TABLE_IDENTIFIER, table.uuid()),
                ImmutableList.of(EventTestUtil.createDataFile()),
                ImmutableList.of()));
    Event complete =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId,
                ImmutableList.of(
                    new TopicPartitionOffset(SRC_TOPIC_NAME, 0, 1L, OffsetDateTime.now()))));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1L, "key", AvroUtil.encode(written)));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2L, "key", AvroUtil.encode(complete)));
    coordinator.process();

    table.refresh();
    assertThat(table.currentSnapshot())
        .as("commit must not be declared ready while source partition 1 has not reported")
        .isNull();
  }

  // ---------------------------------------------------------------- H1
  // A coordinator replaced before its group's first successful commit must not lose buffered files.
  @Test
  public void proofH1_coordinatorRestartBeforeFirstCommitLosesBufferedFiles() {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    TopicPartition ctl = new TopicPartition(CTL_TOPIC_NAME, 0);

    // production default: iceberg.kafka.auto.offset.reset = latest (KafkaClientFactory:55)
    MockConsumer<String, byte[]> first = new MockConsumer<>(OffsetResetStrategy.LATEST);
    MockConsumer<String, byte[]> second = new MockConsumer<>(OffsetResetStrategy.LATEST);
    when(clientFactory.createConsumer(any())).thenReturn(first, second);

    Coordinator c1 = newCoordinator(memberWith(0));
    c1.start();
    first.rebalance(ImmutableList.of(ctl));
    first.updateBeginningOffsets(ImmutableMap.of(ctl, 0L));
    first.updateEndOffsets(ImmutableMap.of(ctl, 0L));

    UUID commitId = startCommit(c1);
    DataFile file = EventTestUtil.createDataFile();
    Event written =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitId,
                TableReference.of("catalog", TABLE_IDENTIFIER, table.uuid()),
                ImmutableList.of(file),
                ImmutableList.of()));
    first.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0L, "key", AvroUtil.encode(written)));
    c1.process(); // buffers the file; no DataComplete yet, so no commit and no offset commit

    // the coordinator is replaced (rebalance moves the leader partition). Its buffer dies with it.
    // The -coord group never committed an offset, so the replacement resets to the log end.
    Coordinator c2 = newCoordinator(memberWith(0));
    c2.start();
    second.rebalance(ImmutableList.of(ctl));
    second.updateBeginningOffsets(ImmutableMap.of(ctl, 0L));
    second.updateEndOffsets(ImmutableMap.of(ctl, 1L)); // log end: record 0 is behind us

    UUID second2 = startCommit(c2);
    second.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME,
            0,
            1L,
            "key",
            AvroUtil.encode(
                new Event(
                    config.connectGroupId(),
                    new DataComplete(
                        second2,
                        ImmutableList.of(
                            new TopicPartitionOffset(SRC_TOPIC_NAME, 0, 1L, null)))))));
    c2.process();

    table.refresh();
    assertThat(table.currentSnapshot())
        .as("the file announced before the coordinator restart must still reach the table")
        .isNotNull();
  }

  // ---------------------------------------------------------------- M2
  // A ValidationException from the offset validator terminates the task instead of retrying.
  @Test
  public void proofM2_validationExceptionTerminatesInsteadOfRetrying() {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    when(config.commitMaxConsecutiveFailures()).thenReturn(5);

    AppendFiles append = mock(AppendFiles.class);
    when(append.validateWith(any())).thenReturn(append);
    when(append.set(any(), any())).thenReturn(append);
    doThrow(new ValidationException("stale offsets")).when(append).commit();

    Table spied = spy(table);
    doReturn(append).when(spied).newAppend();
    doReturn(spied).when(catalog).loadTable(any());

    Coordinator coordinator = newCoordinator(memberWith(0));
    coordinator.start();
    initConsumer();

    UUID commitId = startCommit(coordinator);
    deliverFileAndComplete(commitId, EventTestUtil.createDataFile(), 1L);

    assertThatCode(coordinator::process)
        .as("a stale-offset ValidationException should be retried within the failure budget")
        .doesNotThrowAnyException();
  }

  // ---------------------------------------------------------------- M3
  // Worker flushes files to storage before publishing DataWritten. A publish failure
  // orphans them: the transaction aborts, source offsets roll back, files stay behind.
  @Test
  public void proofM3_workerOrphansFilesWhenPublishFails() {
    DataFile file = EventTestUtil.createDataFile();
    IcebergWriterResult writerResult =
        new IcebergWriterResult(
            TableReference.of("catalog", TABLE_IDENTIFIER, table.uuid()),
            ImmutableList.of(file),
            ImmutableList.of(),
            StructType.of());
    SinkWriterResult results =
        new SinkWriterResult(
            ImmutableList.of(writerResult),
            ImmutableMap.of(new TopicPartition(SRC_TOPIC_NAME, 0), new Offset(1L, null)));

    SinkWriter sinkWriter = mock(SinkWriter.class);
    when(sinkWriter.completeWrite()).thenReturn(results);

    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0)));

    Worker worker = new Worker(config, clientFactory, sinkWriter, context);
    worker.start();
    initConsumer();

    // the control-topic publish fails after the files are already on storage
    producer.commitTransactionException = new RuntimeException("broker unavailable");

    Event start =
        new Event(config.connectGroupId(), new StartCommit(UUID.randomUUID()));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1L, "key", AvroUtil.encode(start)));

    assertThatCode(worker::process).isInstanceOf(RuntimeException.class);

    assertThat(mockingDetails(sinkWriter).getInvocations())
        .as(
            "completeWrite() ran before the failed publish, so "
                + file.location()
                + " is now orphaned with no compensating cleanup")
        .noneMatch(inv -> "completeWrite".equals(inv.getMethod().getName()));
  }
}
