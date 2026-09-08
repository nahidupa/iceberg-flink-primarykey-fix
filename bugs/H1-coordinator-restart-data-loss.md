# H1 — Coordinator replaced before the group's first commit loses buffered files

| | |
|---|---|
| **Severity** | High |
| **Class** | Silent data loss |
| **Status** | Unreported |
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

Bounded to the window before the group's **first ever** successful commit, which is exactly
connector startup, when Connect rebalances most. The window **reopens** whenever the group's
offsets expire (`offsets.retention.minutes`, default 7 days) during an idle period.

## Proof

**Corrected 2026-09-08.** The original `proofH1_…` in `TestBugProofs.java` was **unsound**: it
queued the `DataWritten` record only on the *first* coordinator's `MockConsumer`, so the
replacement could never have read it whatever the reset strategy. It failed for the wrong reason —
the same fixture flaw independent review found in [M3](M3-worker-orphaned-files.md).

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
| Commit control-topic offsets once at coordinator start | Group always has a floor; replacements resume correctly | Small; changes when offsets are first written |
| Default the `-coord` consumer to `earliest` | Replacement re-reads from the beginning; the per-table floor filters what was already committed | One line, but changes recovery semantics on a large control topic |
| Persist `commitBuffer` outside the coordinator | Fully removes the dependency on replay | Large |

The second is the smallest change and composes with the existing floor, but needs discussion —
`putIfAbsent` means operators can already override it via `iceberg.kafka.auto.offset.reset`.
