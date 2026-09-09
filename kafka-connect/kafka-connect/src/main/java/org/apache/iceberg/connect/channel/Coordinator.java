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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotAncestryValidator;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Coordinator extends Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String TASK_ID_SNAPSHOT_PROP = "kafka.connect.task-id";
  private static final String VALID_THROUGH_TS_SNAPSHOT_PROP = "kafka.connect.valid-through-ts";
  private static final Duration POLL_DURATION = Duration.ofSeconds(1);

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private volatile int totalPartitionCount;
  private volatile boolean partitionCountVerified;
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;
  private final AtomicLong partialCommitFailures = new AtomicLong();
  private volatile boolean terminated;
  private final String taskId;
  private int consecutiveCommitFailures;

  Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory,
      SinkTaskContext context) {
    // pass consumer group ID to which we commit low watermark offsets
    super("coordinator", config.connectGroupId() + "-coord", config, clientFactory, context);

    this.catalog = catalog;
    this.config = config;
    this.totalPartitionCount = totalPartitionCount(members);
    // the constructor's group snapshot is the same source the refresh uses, so it starts trusted;
    // an assignment change withdraws that trust until a stable group confirms the new count
    this.partitionCountVerified = true;
    this.snapshotOffsetsProp =
        String.format(
            "kafka.connect.offsets.%s.%s", config.controlTopic(), config.connectGroupId());
    this.exec =
        new ThreadPoolExecutor(
            config.commitThreads(),
            config.commitThreads(),
            config.keepAliveTimeoutInMs(),
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-committer" + "-%d")
                .build());
    this.commitState = new CommitState(config);
    this.taskId = config.connectorName() + "-" + config.taskId();
  }

  void process() {
    if (commitState.isCommitIntervalReached()) {
      refreshTotalPartitionCount();
      // send out begin commit
      commitState.startNewCommit();
      Event event =
          new Event(config.connectGroupId(), new StartCommit(commitState.currentCommitId()));
      send(event);
      LOG.info("Coordinator {} initiated commit {}", taskId, commitState.currentCommitId());
    }

    consumeAvailable(POLL_DURATION);

    if (commitState.isCommitTimedOut()) {
      commit(true);
    }
  }

  private static int totalPartitionCount(Collection<MemberDescription> members) {
    return members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
  }

  /**
   * Re-reads the source partition count from the consumer group. A coordinator outlives the
   * assignment it was constructed with: a task that keeps the leader partition through a topic
   * expansion is not restarted, so a count captured once would keep a commit ready before every
   * partition has reported and stamp a watermark the table does not satisfy.
   *
   * <p>Only a stable group is used. A description taken mid-rebalance can report a subset of the
   * members, and adopting that lower count would cause exactly the premature commit this guards
   * against.
   *
   * <p>A successful read restores full-commit eligibility after an assignment change withdrew it.
   * While the count is unverified a full commit is not eligible, because its {@code
   * valid-through-ts} asserts that every source partition reported. Such a cycle still commits its
   * data when it times out, as a partial commit without that watermark.
   */
  private void refreshTotalPartitionCount() {
    int updated;
    try {
      ConsumerGroupDescription groupDesc =
          KafkaUtils.consumerGroupDescription(config.connectGroupId(), admin());
      if (groupDesc.state() != ConsumerGroupState.STABLE) {
        LOG.info(
            "Coordinator {} cannot verify the source partition count, group {} is {}."
                + " This cycle can only end in a partial commit.",
            taskId,
            config.connectGroupId(),
            groupDesc.state());
        return;
      }

      updated = totalPartitionCount(groupDesc.members());
    } catch (Exception e) {
      LOG.warn(
          "Coordinator {} could not describe group {}, keeping source partition count {}."
              + " This cycle can only end in a partial commit.",
          taskId,
          config.connectGroupId(),
          totalPartitionCount,
          e);
      return;
    }

    if (updated <= 0) {
      LOG.warn(
          "Coordinator {} read a source partition count of {} from group {}, keeping {}."
              + " This cycle can only end in a partial commit.",
          taskId,
          updated,
          config.connectGroupId(),
          totalPartitionCount);
      return;
    }

    this.partitionCountVerified = true;

    if (updated != totalPartitionCount) {
      LOG.info(
          "Coordinator {} source partition count changed from {} to {}",
          taskId,
          totalPartitionCount,
          updated);
      this.totalPartitionCount = updated;
    }
  }

  /**
   * Marks the source partition count as unverified. A coordinator outlives the assignment it was
   * constructed with, so an assignment change mid-cycle can leave a verified count describing a
   * topology that no longer exists. The current cycle can still commit its data, but only as a
   * partial commit, which stamps no completeness watermark.
   */
  void assignmentChanged() {
    this.partitionCountVerified = false;
  }

  @Override
  protected boolean receive(Envelope envelope) {
    switch (envelope.event().payload().type()) {
      case DATA_WRITTEN:
        commitState.addResponse(envelope);
        return true;
      case DATA_COMPLETE:
        commitState.addReady(envelope);
        if (partitionCountVerified && commitState.isCommitReady(totalPartitionCount)) {
          commit(false);
        }
        return true;
    }
    return false;
  }

  private void commit(boolean partialCommit) {
    try {
      doCommit(partialCommit);
      if (!partialCommit) {
        consecutiveCommitFailures = 0;
      }
    } catch (RuntimeException e) {
      if (partialCommit) {
        partialCommitFailures.incrementAndGet();
        LOG.warn(
            "Partial commit {} failed for task {}, will retry",
            commitState.currentCommitId(),
            taskId,
            e);
        return;
      }

      if (!(e instanceof CommitFailedException)) {
        // CommitStateUnknownException, ValidationException, ForbiddenException,
        // NPE, anything else -- not retryable, terminate immediately
        throw e;
      }

      consecutiveCommitFailures++;
      if (consecutiveCommitFailures >= config.commitMaxConsecutiveFailures()) {
        LOG.error(
            "Commit {} failed for task {} ({} consecutive failures, terminating)",
            commitState.currentCommitId(),
            taskId,
            consecutiveCommitFailures,
            e);
        throw e;
      }
      LOG.warn(
          "Commit {} failed for task {} ({} consecutive failures, will retry)",
          commitState.currentCommitId(),
          taskId,
          consecutiveCommitFailures,
          e);
    } finally {
      commitState.endCurrentCommit();
    }
  }

  private void doCommit(boolean partialCommit) {
    Map<TableReference, List<Envelope>> commitMap = commitState.tableCommitMap();
    OffsetDateTime validThroughTs = commitState.validThroughTs(partialCommit);

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(
            entry ->
                commitToTable(
                    entry.getKey(), entry.getValue(), controlTopicOffsets(), validThroughTs));

    // we should only get here if all tables committed successfully...
    commitConsumerOffsets();
    commitState.clearResponses();

    Event event =
        new Event(
            config.connectGroupId(),
            new CommitComplete(commitState.currentCommitId(), validThroughTs));
    send(event);

    LOG.info(
        "Coordinator {} completed commit {}, committed to {} table(s), valid-through {}",
        taskId,
        commitState.currentCommitId(),
        commitMap.size(),
        validThroughTs);
  }

  private String offsetsToJson(Map<Integer, Long> offsets) {
    try {
      return MAPPER.writeValueAsString(offsets);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void commitToTable(
      TableReference tableReference,
      List<Envelope> envelopeList,
      Map<Integer, Long> controlTopicOffsets,
      OffsetDateTime validThroughTs) {
    TableIdentifier tableIdentifier = tableReference.identifier();
    Table table;
    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found, skipping commit: {}", tableIdentifier, e);
      return;
    }

    if (tableReference.uuid() != null && !tableReference.uuid().equals(table.uuid())) {
      LOG.warn(
          "Skipping commits to table {} due to target table mismatch.  Expected: {} Received: {}",
          tableIdentifier,
          table.uuid(),
          tableReference.uuid());
      return;
    }

    String branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

    // Control topic partition offsets may include a subset of partition ids if there were no
    // records for other partitions.  Merge the updated topic partitions with the last committed
    // offsets.
    Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(table, branch);
    Map<Integer, Long> mergedOffsets =
        Stream.of(committedOffsets, controlTopicOffsets)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::max));
    String offsetsJson = offsetsToJson(mergedOffsets);

    List<DataWritten> payloads =
        envelopeList.stream()
            .filter(
                envelope -> {
                  Long minOffset = committedOffsets.get(envelope.partition());
                  return minOffset == null || envelope.offset() >= minOffset;
                })
            .map(envelope -> (DataWritten) envelope.event().payload())
            .collect(Collectors.toList());

    List<DataFile> dataFiles =
        payloads.stream()
            .filter(payload -> payload.dataFiles() != null)
            .flatMap(payload -> payload.dataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .collect(Collectors.toList());

    List<DeleteFile> deleteFiles =
        payloads.stream()
            .filter(payload -> payload.deleteFiles() != null)
            .flatMap(payload -> payload.deleteFiles().stream())
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .collect(Collectors.toList());

    if (terminated) {
      throw new ConnectException(
          String.format("Coordinator %s is terminated, commit aborted", taskId));
    }

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info(
          "Coordinator {} found nothing to commit to table {}, skipping", taskId, tableIdentifier);
    } else {
      if (deleteFiles.isEmpty()) {
        AppendFiles appendOp =
            table.newAppend().validateWith(offsetValidator(tableIdentifier, committedOffsets));
        if (branch != null) {
          appendOp.toBranch(branch);
        }
        appendOp.set(snapshotOffsetsProp, offsetsJson);
        appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
        appendOp.set(TASK_ID_SNAPSHOT_PROP, taskId);
        if (validThroughTs != null) {
          appendOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
        }
        dataFiles.forEach(appendOp::appendFile);
        appendOp.commit();
      } else {
        RowDelta deltaOp =
            table.newRowDelta().validateWith(offsetValidator(tableIdentifier, committedOffsets));
        if (branch != null) {
          deltaOp.toBranch(branch);
        }
        deltaOp.set(snapshotOffsetsProp, offsetsJson);
        deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
        deltaOp.set(TASK_ID_SNAPSHOT_PROP, taskId);
        if (validThroughTs != null) {
          deltaOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
        }
        dataFiles.forEach(deltaOp::addRows);
        deleteFiles.forEach(deltaOp::addDeletes);
        deltaOp.commit();
      }

      Long snapshotId = latestSnapshot(table, branch).snapshotId();
      Event event =
          new Event(
              config.connectGroupId(),
              new CommitToTable(
                  commitState.currentCommitId(), tableReference, snapshotId, validThroughTs));
      send(event);

      LOG.info(
          "Coordinator {} completed commit to table {}, snapshot {}, commit ID {}, valid-through {}",
          taskId,
          tableIdentifier,
          snapshotId,
          commitState.currentCommitId(),
          validThroughTs);
    }
  }

  private SnapshotAncestryValidator offsetValidator(
      TableIdentifier tableIdentifier, Map<Integer, Long> expectedOffsets) {

    return new SnapshotAncestryValidator() {
      private Map<Integer, Long> lastCommittedOffsets;

      @Override
      public boolean validate(Iterable<Snapshot> baseSnapshots) {
        lastCommittedOffsets = lastCommittedOffsets(baseSnapshots);

        return expectedOffsets.equals(lastCommittedOffsets);
      }

      @Override
      public String errorMessage() {
        return String.format(
            "Cannot commit to %s, stale offsets: Expected: %s Committed: %s",
            tableIdentifier, expectedOffsets, lastCommittedOffsets);
      }
    };
  }

  private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Map<Object, Boolean> seen = Maps.newConcurrentMap();
    return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
  }

  private Snapshot latestSnapshot(Table table, String branch) {
    if (branch == null) {
      return table.currentSnapshot();
    }
    return table.snapshot(branch);
  }

  private Map<Integer, Long> lastCommittedOffsetsForTable(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);

    if (snapshot == null) {
      return Map.of();
    }

    Iterable<Snapshot> branchAncestry =
        SnapshotUtil.ancestorsOf(snapshot.snapshotId(), table::snapshot);
    return lastCommittedOffsets(branchAncestry);
  }

  private Map<Integer, Long> lastCommittedOffsets(Iterable<Snapshot> snapshots) {
    return Streams.stream(snapshots)
        .filter(Objects::nonNull)
        .filter(snapshot -> snapshot.summary().containsKey(snapshotOffsetsProp))
        .map(snapshot -> snapshot.summary().get(snapshotOffsetsProp))
        .map(this::parseOffsets)
        .findFirst()
        .orElseGet(Map::of);
  }

  private Map<Integer, Long> parseOffsets(String value) {
    if (value == null) {
      return Map.of();
    }

    TypeReference<Map<Integer, Long>> typeRef = new TypeReference<>() {};
    try {
      return MAPPER.readValue(value, typeRef);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  long partialCommitFailureCount() {
    return partialCommitFailures.get();
  }

  void terminate() {
    this.terminated = true;

    exec.shutdownNow();

    // wait for coordinator termination, else cause the sink task to fail
    try {
      if (!exec.awaitTermination(1, TimeUnit.MINUTES)) {
        throw new ConnectException("Timed out waiting for coordinator shutdown");
      }
    } catch (InterruptedException e) {
      throw new ConnectException("Interrupted while waiting for coordinator shutdown", e);
    }
  }
}
