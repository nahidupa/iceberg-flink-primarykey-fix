# H2 — Missing or replaced table silently discards buffered files

| | |
|---|---|
| **Severity** | High |
| **Class** | Silent data loss |
| **Status** | Unreported |
| **Present in** | `main` and PR #17925 |
| **Proof** | `TestBugProofs.proofH2_missingTableSilentlyDiscardsBufferedFiles` |

## Summary

If a table cannot be loaded, or its UUID does not match, `commitToTable` logs a warning and
**returns normally**. `doCommit` treats that as success, advances the control-topic offsets, and
clears `commitBuffer`. Every `DataWritten` for that table is discarded and can never be recovered.

## Affected code

`Coordinator.java:244-256` — both early returns leave no trace and raise nothing:

```java
try {
  table = catalog.loadTable(tableIdentifier);
} catch (NoSuchTableException e) {
  LOG.warn("Table not found, skipping commit: {}", tableIdentifier, e);
  return;                                    // ← treated as success
}

if (tableReference.uuid() != null && !tableReference.uuid().equals(table.uuid())) {
  LOG.warn("Skipping commits to table {} due to target table mismatch. ...");
  return;                                    // ← treated as success
}
```

`Coordinator.java:210-212` — because no exception escaped, `Tasks.foreach` completes and the
buffer is cleared:

```java
// we should only get here if all tables committed successfully...
commitConsumerOffsets();
commitState.clearResponses();
```

## Mechanism

1. A worker writes file X for table T, publishes `DataWritten`, transactionally commits its
   source offsets.
2. T is dropped, recreated, or is temporarily unreachable.
3. The commit cycle fires. `commitToTable(T, …)` hits `NoSuchTableException` and returns.
4. `doCommit` proceeds to `commitConsumerOffsets()` and `clearResponses()`.
5. File X is gone from the buffer, and the control-topic offsets have advanced past its event.
6. When T reappears, nothing re-delivers the event — the offsets already moved.

## Impact

Silent omission for the affected table: the files exist in storage but are never registered.

**Corrected after independent review.** An earlier version of this note claimed recovery was
"impossible even by manual replay". That is false. Committed consumer offsets are checkpoints, not
deletions — an operator can reset the `-coord` group offset and replay the control topic, and the
per-table floor will filter whatever was genuinely committed. The loss is silent and automatic
recovery does not occur, but manual recovery remains possible.

The UUID-mismatch branch is arguably intentional (the target really is a different table), but the
`NoSuchTableException` branch is not — a transient catalog failure is enough to trigger it.

## Proof

```
[file buffered while the table was missing must not be lost]
Expecting actual not to be empty
```

The test drops the table for exactly one commit cycle, restores it, then runs another cycle.
`table.snapshots()` is empty: the file was discarded, not retried.

## Suggested fixes

| Option | Effect | Cost |
|---|---|---|
| Throw instead of returning on `NoSuchTableException` | Buffer is retained | One line, **but see caveat**: `commit()` retries only Iceberg's `CommitFailedException`, so a thrown `NoSuchTableException` would terminate the coordinator immediately rather than retry |
| Track skipped tables and exclude their envelopes from `clearResponses()` | Only the affected table's data is retained | Moderate; needs per-table buffer accounting |
| Keep the early return but do not advance offsets when any table was skipped | Conservative | Moderate |

Separating "skipped" from "committed" is the real fix — `doCommit`'s comment says *"we should only
get here if all tables committed successfully"*, which is not currently true.
