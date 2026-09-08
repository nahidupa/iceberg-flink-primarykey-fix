# M3 — Worker orphans data files when the control-topic publish fails

| | |
|---|---|
| **Severity** | Medium (unconfirmed) |
| **Class** | Storage leak |
| **Status** | Unreported |
| **Present in** | `main` and PR #17925 |
| **Evidence level** | **Proof INVALID — analysis only** |

## Summary

`Worker.receive` flushes writers to object storage **before** publishing `DataWritten`. If the
publish transaction fails, the transaction aborts and the source offsets roll back, so the records
are re-delivered and the files are written again. The first set is never referenced and never
cleaned up.

## Affected code

`Worker.java:74` — files hit storage first:

```java
SinkWriterResult results = sinkWriter.completeWrite();
```

`Worker.java:111` — publish happens afterwards:

```java
send(events, results.sourceOffsets());
```

`Channel.java:96-116` — on failure the transaction aborts, so the source offsets are not committed:

```java
producer.beginTransaction();
try {
  recordList.forEach(producer::send);
  if (!sourceOffsets.isEmpty()) {
    producer.sendOffsetsToTransaction(offsetsToCommit, KafkaUtils.consumerGroupMetadata(context));
  }
  producer.commitTransaction();
} catch (Exception e) {
  producer.abortTransaction();
  throw e;
}
```

There is no compensating delete for the files already written.

## Mechanism

1. `StartCommit` arrives. `completeWrite()` writes file X to object storage.
2. `send(...)` fails — broker unavailable, transaction timeout, fencing.
3. The transaction aborts. Source offsets are not committed.
4. Connect re-delivers the records; the worker writes file X' with the same content.
5. File X is orphaned: not in any manifest, not referenced by any snapshot, never deleted.

A secondary effect: `controlTopicOffsets.put()` already advanced for that `StartCommit` record
(`Channel.java:135`, before `receive()` is called at `:141`), so the worker will not re-handle that
particular `StartCommit` even if it were re-delivered.

## Impact

No correctness problem — orphaned files are invisible to readers. The cost is unbounded storage
growth under repeated publish failures, and the files are not reachable by
`remove_orphan_files` until they age past its default retention.

## Proof — WITHDRAWN

`proofM3_workerOrphansFilesWhenPublishFails` **does not test what it claims**, and its failure must
not be cited as evidence. Identified by independent review, 2026-09-08, and confirmed:

1. `Channel.send()` calls `producer.sendOffsetsToTransaction(offsets, KafkaUtils.consumerGroupMetadata(context))`
   **before** `commitTransaction()` (`Channel.java:101-104`).
2. `consumerGroupMetadata` reaches
   `DynFields.builder().hiddenImpl("org.apache.kafka.connect.runtime.WorkerSinkTaskContext", "consumer")`
   (`KafkaUtils.java:89`). Against the plain `mock(SinkTaskContext.class)` the test supplies, that
   throws `ConnectException`.
3. The test asserts only `isInstanceOf(RuntimeException.class)`, which happily accepts that
   fixture error. **The injected `commitTransactionException` is never reached.**
4. The final assertion merely forbids `completeWrite()` from being invoked, so it would keep
   failing even after a correct cleanup fix.
5. No physical file is created or checked, so nothing demonstrates an actual orphan.

A valid proof needs a stubbed consumer-group metadata path (or a real `WorkerSinkTaskContext`), a
narrow assertion on the injected exception, and a filesystem check that the written file survives.

Until then this finding rests on code reading only: the ordering at `Worker.java:74` (write) and
`Worker.java:111` (publish) is real, and there is no compensating delete — but the end-to-end
consequence is **not** demonstrated.

## Suggested fixes

| Option | Effect | Cost |
|---|---|---|
| Delete the just-written files on publish failure | Removes the leak directly | Small, but the delete can itself fail |
| Document that `remove_orphan_files` maintenance is required | Zero code | Operational burden |
| Two-phase write with a manifest of pending files | Fully recoverable | Large |

Iceberg already documents orphan-file maintenance in `docs/docs/maintenance.md`, and orphaned
files from failed writes are an accepted part of that model across engines. Independent review
notes this weakens the case for treating it as a connector defect at all. **Do not propose
immediate file deletion without a recovery-safety analysis** — a delete that races a successful-
but-unacknowledged transaction would destroy committed data.
