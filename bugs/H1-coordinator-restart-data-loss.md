# H1 — Coordinator replaced before the group's first commit loses buffered files

Archived investigation from September 8, 2026. Source locations and validation
below describe the investigation revision, not the current upstream branch.
This folder is published only on the fork's investigation-notes branch.

| | |
|---|---|
| **Severity** | High |
| **Class** | Silent data loss |
| **Status** | Fix proposed in [PR #18006](https://github.com/apache/iceberg/pull/18006) |
| **Present in** | `main`, and in PR #17925 both before and after the skip guard |
| **Evidence level** | **Proven defect** — corrected reproduction in the fix PR |

## Summary

A coordinator buffers `DataWritten` events in memory. If it is replaced before its consumer group
has ever committed an offset, the replacement resumes at the **log end** and never re-reads those
events. The files are orphaned in object storage, and the source records that produced them were
already committed by the worker's transaction, so they are never re-delivered.

## Affected code

`KafkaClientFactory.java:55` — the control-topic consumer defaults to `latest`:

```java
consumerProps.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
```

`Coordinator.java:211` — the **only** call site that gives the `-coord` group an offset, reached
only after every table commits successfully:

```java
// we should only get here if all tables committed successfully...
commitConsumerOffsets();
commitState.clearResponses();
```

`Worker.java:111` → `Channel.send()` — source offsets are committed in the *same transaction* that
publishes `DataWritten`, so once published the records will not be re-consumed:

```java
recordList.forEach(producer::send);
if (!sourceOffsets.isEmpty()) {
  producer.sendOffsetsToTransaction(offsetsToCommit, KafkaUtils.consumerGroupMetadata(context));
}
producer.commitTransaction();
```

## Mechanism

1. Coordinator C1 is elected, subscribes to the control topic. The `<group>-coord` group has never
   committed an offset, so C1 starts at the log end.
2. A worker writes file X, publishes `DataWritten`, and transactionally commits its source offsets.
3. C1 consumes the event into `commitBuffer`. No `DataComplete` quorum yet, so no commit fires and
   `commitConsumerOffsets()` is never reached.
4. A rebalance moves the leader partition. `stopCoordinator()` discards C1 **and its buffer**.
   `Channel.stop()` does not commit offsets.
5. Coordinator C2 starts. The group *still* has no committed offset, so `auto.offset.reset=latest`
   puts it at the current log end — past the `DataWritten` for file X.
6. File X is never registered. Its source records are already consumed.

## Impact

Silent omission: the file is written to object storage but never registered in any snapshot, with
no exception or warning. The object is not deleted, so manual recovery remains possible — but
nothing recovers automatically.

The first successful group checkpoint closes the startup window for the partitions it covers.
The window can reopen per partition if its checkpoint is missing or out of range, including after
offset deletion or retention expiry. For a subscribed consumer group, an active but idle group does
not lose its offsets merely because seven days pass; expiration depends on Kafka's group and
subscription retention conditions, such as the group becoming empty.

## Proof

**Corrected 2026-09-08.** The original `proofH1_…` in `TestBugProofs.java` was **unsound**: it
queued the `DataWritten` record only on the *first* coordinator's `MockConsumer`, so the
replacement could never have read it whatever the reset strategy. It failed for the wrong reason —
the same fixture flaw independent review found in the unpublished M3 worker-orphaned-files audit.

A probe confirms the harness *can* express this faithfully:

```
PROBE strategy=earliest position=1 recordsReturned=1
PROBE strategy=latest   position=1 recordsReturned=0
```

`MockConsumer` does honour the reset strategy, so queuing the record on **both** consumers and
letting the strategy decide visibility is a sound model.

The corrected reproduction is
`TestCoordinatorOffsetReset.replacementCoordinatorRecoversFilesBufferedByItsPredecessor` in the fix
PR. Verified against the real wiring — each `MockConsumer` is built from the strategy production
actually requested:

```
before fix:  coordinatorConsumerReadsFromEarliest()                        FAILED
             replacementCoordinatorRecoversFilesBufferedByItsPredecessor() FAILED
             3 tests completed, 2 failed
after fix:   BUILD SUCCESSFUL
```

The existing harness uses `OffsetResetStrategy.EARLIEST` while production uses `latest`. **That
difference is why this never surfaced in the existing suite.**

## Relationship to #16282 / PR #17925

This is the same *class* of bug as #16282: files that exist in storage but are never registered.

@wombatu-kun raised a narrower version of it against the `CommitState.reset()` that PR #17925
originally proposed, asking whether the window was intentional. Removing `reset()` did **not**
remove the window — the proof runs against the current tree, with the skip guard in place and no
`reset()` anywhere. The cause is coordinator *restart*, not buffer *clearing*.

## Suggested fixes

| Option | Effect | Cost |
|---|---|---|
| Commit control-topic offsets once at coordinator start | Unsafe if the checkpoint advances past files not yet committed to Iceberg | Does not by itself establish delivery |
| Default the `-coord` consumer to `earliest` | Replacement re-reads retained history; a retained per-table boundary filters what was already committed | Implemented in PR #18006; subject to the retention limitation below |
| Persist `commitBuffer` outside the coordinator | Fully removes the dependency on replay | Large |

The second is the smallest change and composes with the existing floor, but needs discussion —
`putIfAbsent` means operators can already override it via `iceberg.kafka.auto.offset.reset`.

## Snapshot-expiration follow-up

On 2026-09-08, a local regression against `3eea23a` plus test-only changes reproduced duplicate
live-file registration. This is a separate recovery limitation, not a failure to recover the
uncommitted file in H1. No missing-boundary policy or startup memory bound is added in this PR.

The regression performed real metadata operations against the in-memory catalog:

1. Commit file X through the coordinator, producing snapshot S1 with its offset boundary.
2. Append a distinct file through the ordinary table API, producing S2 without the boundary.
3. Expire S1 and verify that it is no longer available while X is still live through S2.
4. Start an independent coordinator consumer without a Kafka checkpoint and replay X's announcement.
5. Scan the current table and assert that X and the external file each occur exactly once.

The retained-boundary case passed. The expired-boundary case failed with:

```text
Expecting actual:
   ["path/to/file.parquet", "path/to/file.parquet", "external.parquet"]
to contain exactly in any order:
   ["path/to/file.parquet", "external.parquet"]
but the following elements were unexpected:
   ["path/to/file.parquet"]
```

The standalone [reproduction patch](snapshot-expiration-replay.patch) adds the expiration sequence
to the no-pending-files variant of the replay regression at `3eea23a`. It is intentionally not
applied to the passing PR test suite. The test is absent from this notes branch; the patch requires
that historical H1 revision and may need adaptation on newer revisions. From that revision's
repository root, apply it only when working on the follow-up:

```sh
git apply --check bugs/snapshot-expiration-replay.patch
git apply bugs/snapshot-expiration-replay.patch
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test \
   --tests 'org.apache.iceberg.connect.channel.TestCoordinatorOffsetReset.replayedResponsesAlreadyCoveredBySnapshotOffsetsAreNotCommittedTwice' \
   --console=plain
git apply --reverse bugs/snapshot-expiration-replay.patch
```

Until a recovery policy is implemented, safe historical replay depends on a reachable snapshot
boundary for already-committed announcements. Rejecting all empty boundaries would also reject
legitimate first commits, so that is not an unconditional fix. Startup buffering remains a separate
operational risk for large histories, even when the boundary exists.
