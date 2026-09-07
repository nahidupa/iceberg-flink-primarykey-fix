# CommitState After PR Review

**The fix preserves useful in-memory state across a control-topic rebalance and prevents already-consumed records from changing that state again.** It does not discard the current commit and hope Kafka reconstructs it.

This describes the local post-review implementation inspected on 2026-09-07, not the original reset-based proposal in PR #17925. The earlier diagrams are retained as historical artifacts.

Interactive lifecycle: [iceberg-commit-state-after-pr-review.html](iceberg-commit-state-after-pr-review.html)

![CommitState lifecycle with retained rebalance state and separate retry cleanup](iceberg-commit-state-after-pr-review.visual-check.1440x900.light.png)

## What Changed

| Concern | Original PR proposal | Post-review implementation |
| --- | --- | --- |
| Rebalance handling | Invoke a coordinator reset hook | Remove the added listener and reset hook |
| Buffered files | Clear them during revocation | Keep them in the surviving coordinator |
| Active commit | Clear its ID and readiness | Keep the same ID and readiness |
| Replayed control records | Dispatch them again after resetting | Skip them in `Channel.consumeAvailable` |
| Successful cleanup | Clear file responses after committing | Preserve that existing cleanup |

The important distinction is **duplicate delivery versus invalid state**. A consumer rewinding does not make an already-buffered file or a genuine readiness response invalid.

## What the State Contains

| State | Meaning | On a same-instance rebalance | When a commit attempt ends |
| --- | --- | --- | --- |
| `commitBuffer` | `DataWritten` envelopes describing files | Retained | Retained unless `clearResponses()` was reached |
| `readyBuffer` | `DataComplete` events used for timestamp calculation | Retained | Cleared by `endCurrentCommit()` |
| `receivedPartitionCount` | Assignment count from matching-commit readiness events | Retained, not incremented by replay | Reset to zero |
| `currentCommitId` | ID of the active commit round | Retained | Set to `null` |
| `startTime` | Clock used for interval and timeout decisions | Retained | Not reset by `endCurrentCommit()` |
| `controlTopicOffsets` | Per-control-partition next-offset map owned by `Channel` | Retained | Retained independently of round cleanup |

`commitBuffer` stores envelopes and file metadata, not the Parquet contents themselves. The files already exist in storage.

Source: [CommitState.java](../kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CommitState.java#L40).

## The Guard Runs First

The decision in [Channel.java](../kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Channel.java#L124), with debug logging omitted, is:

```java
Long nextOffset = controlTopicOffsets.get(record.partition());
if (nextOffset != null && record.offset() < nextOffset) {
  return;
}

controlTopicOffsets.put(record.partition(), record.offset() + 1);
```

This `return` exits the callback for **one record**, not the entire polling loop. A skipped record does not update the map, get decoded, or reach `Coordinator.receive`.

For example, after consuming offset `2`, the stored next offset is `3`:

- Replayed offset `1` or `2`: skip it; the map stays at `3`.
- New offset `3`: accept it; the map becomes `4`.
- Previously unseen partition: accept its first observed record because it has no stored boundary.

This is why the comparison is `<`, not `<=`. The stored value is the **next** offset, not the last consumed offset. Partition boundaries are independent. Group filtering still happens after offset tracking for accepted records, as before.

## One Commit Round

1. **Start when due.** `process()` checks the existing interval rule. `startNewCommit()` creates the ID and records the start time, and the coordinator publishes `StartCommit`.
2. **Collect accepted events.** `DataWritten` adds file responses. `DataComplete` adds readiness metadata and increments the assignment count only when its commit ID matches the active round.
3. **Keep collecting through a rebalance.** In the surviving instance, the ID, files, readiness and offset map remain. Replayed records stop at the channel guard; genuinely new records can still complete the round.
4. **Attempt a commit.** Reaching the expected assignment count triggers a full commit. Exceeding the timeout triggers a partial commit with what has arrived. A partial commit has no valid-through timestamp.
5. **End the round.** `commit()` always invokes `endCurrentCommit()` in `finally`. This clears readiness and the active ID, whether the attempt succeeded or threw.

The top rail in the diagram represents this logical progression; the adjacent phase labels already describe the main-path transitions. **"Round ended" does not mean "commit succeeded."** The next round is subject to the existing interval rule, not an immediate-restart promise.

Source: [Coordinator.java](../kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java#L121).

## Two Cleanup Boundaries

In the normal successful path, `doCommit()` executes these steps in order:

1. Complete the per-table commit work.
2. Commit the coordinator's Kafka control-consumer offsets.
3. Call `clearResponses()` to clear the file buffer.
4. Publish `CommitComplete`.
5. Return through `commit()`'s `finally`, which ends the current round.

The table operations and Kafka offset commit are **not one shared atomic transaction**. Existing table snapshot-offset filtering remains part of recovery.

If an attempt fails **before step 3**, the file buffer remains available to a later round when the retry policy permits continuation. Readiness and the old ID are still cleared. Not every error is retryable: the existing exception classification and failure limit can terminate the task.

A failure publishing `CommitComplete` is later than the buffer clear. It would be inaccurate to say that *every* failed `doCommit()` leaves the file buffer intact.

Source: [Coordinator.java](../kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java#L154).

## Why Removing Reset Matters

Consider a fresh coordinator group that has not committed a control offset yet:

1. The worker publishes a file response and advances its source offsets transactionally.
2. The coordinator consumes that response and buffers file X.
3. A rebalance occurs before the coordinator's first successful commit.
4. With the default `auto.offset.reset=latest`, reassignment without a committed offset can start at the log end. X's response need not be replayed.

The old reset discarded X's only in-memory response and relied on step 4 replaying it. The revised implementation keeps that response, so a surviving coordinator can still commit X, including through a timeout commit, **without receiving X again**.

Keeping the file buffer also matters with multiple control partitions: a timeout may occur before replay from every partition completes, and a retained partition might not rewind at all.

**Do not combine the new guard with clearing the buffer on rebalance.** The guard would reject the old records needed to reconstruct the discarded state.

Sources: [KafkaClientFactory.java](../kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/KafkaClientFactory.java#L53) and [Channel.java](../kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Channel.java#L79).

## Evidence and Limits

The current regression tests directly inspect the recovered files and snapshots:

- `retainsBufferedFilesWhenRebalanceResetsToLatest` verifies a buffered file commits without being replayed after reassignment to the log end.
- `retainsFilesDuringPartialControlReplay` verifies both files survive incomplete replay across two control partitions, including retaining one assignment.
- `commitsReplayedFilesExactlyOnce` verifies replay does not complete the readiness count early and genuine completion persists both expected files.

Source: [TestCoordinator.java](../kafka-connect/kafka-connect/src/test/java/org/apache/iceberg/connect/channel/TestCoordinator.java#L143). The test bodies were reviewed for these documents; Java tests were not rerun during this documentation-only task. The earlier implementation verification is recorded in [planning.md](../planning.md).

This is an **in-memory, same-instance replay guard**, not a durable recovery checkpoint. It does not establish correctness for process replacement without committed control offsets, topic-retention loss, conflicting coordinators, or the same logical event republished at a different Kafka offset. It does not repair existing duplicate table entries.

For the numeric replay example, continue with [iceberg-16282-duplicate-files-fix-after-pr-review.md](iceberg-16282-duplicate-files-fix-after-pr-review.md).

## Artifact Evidence

Specification: [iceberg-commit-state-after-pr-review.lifecycle.json](iceberg-commit-state-after-pr-review.lifecycle.json)

Browser report: [iceberg-commit-state-after-pr-review.visual-check.json](iceberg-commit-state-after-pr-review.visual-check.json)

Screenshot contact sheet: [iceberg-commit-state-after-pr-review.visual-check.html](iceberg-commit-state-after-pr-review.visual-check.html)

### Delivery Receipt

```text
diagram_type: lifecycle
specification_sha256: 238a5e98f4ac6b0bc06c040b2ed4e170f81996ffba13f62250fbe4cda168097f
specification_bytes: 2662
artifact_sha256: d341a419631104606052c145d07f225371b87d4b31ca74ac025163878849221a
artifact_bytes: 708901
validation: 9/9 showcase, 0 errors, 0 warnings
browser_evidence: passed
visual_review: passed
correction_rounds: 2
```

Chrome measured no page overflow at 1440x900, 1600x1000, 1920x1080 and 2048x1320. An image-capable assistant inspected the artifact-bound light/dark screenshots at 1440x900 and 2048x1320: text fits, the return paths are distinguishable, the recovery heading is clear, and the diagram plus cards remain visible and vertically balanced.

This is screenshot-based perceptual review of READ / Still mode, not human sign-off or exhaustive interaction/export testing. The automated report correctly retains `visualReview: pending`; the separate review statement above does not rewrite that machine receipt. Mobile containment was not measured.
