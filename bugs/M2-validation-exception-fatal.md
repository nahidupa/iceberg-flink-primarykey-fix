# M2 — `ValidationException` terminates the task instead of retrying

| | |
|---|---|
| **Severity** | Medium |
| **Class** | Availability (design tradeoff, not clearly a defect) |
| **Status** | Unreported |
| **Present in** | `main` and PR #17925 |
| **Proof** | `TestBugProofs.proofM2_validationExceptionTerminatesInsteadOfRetrying` |

## Summary

A `ValidationException` raised by the snapshot-ancestry offset validator is explicitly classified
as non-retryable, so the coordinator thread exits and the task fails. Retrying would re-read the
floor and re-filter the envelopes, which is very likely to succeed.

**This one is a judgement call, not an unambiguous bug** — see "Is this actually wrong?" below.

## Affected code

`Coordinator.java:171-174`:

```java
if (!(e instanceof CommitFailedException)) {
  // CommitStateUnknownException, ValidationException, ForbiddenException,
  // NPE, anything else -- not retryable, terminate immediately
  throw e;
}
```

`Coordinator.java:357-377` — the validator that raises it:

```java
public boolean validate(Iterable<Snapshot> baseSnapshots) {
  lastCommittedOffsets = lastCommittedOffsets(baseSnapshots);
  return expectedOffsets.equals(lastCommittedOffsets);
}
```

## Mechanism

1. Two coordinators are briefly live (split brain), or a concurrent writer commits between the
   floor read and the CAS.
2. The validator sees a floor different from the one it expected and raises `ValidationException`.
3. `commit()` rethrows immediately; the task fails.

## Impact

Split-brain becomes task death rather than graceful convergence. Because the buffer is retained
and the floor filters on restart, no data is lost or duplicated — the cost is availability and
restart churn.

## Proof

```
[a stale-offset ValidationException should be retried within the failure budget]
Expecting code not to raise a throwable but caught
  "org.apache.iceberg.exceptions.ValidationException: stale offsets
	at org.apache.iceberg.connect.channel.Coordinator.commitToTable(Coordinator.java:320)
	at org.apache.iceberg.connect.channel.Coordinator.lambda$doCommit$1(Coordinator.java:207)
```

The test sets `commitMaxConsecutiveFailures` to 5, so budget was available and unused.

## Is this actually wrong?

Arguments that the current behaviour is correct:

- A stale floor may indicate a genuine split brain, where failing fast is safer than racing.
- Connect restarts the task anyway, which achieves recovery by a different route.
- `SnapshotProducer` already retries internally; reaching this handler means those retries were
  exhausted or inapplicable.

Arguments that it is wrong:

- The validator's own failure mode — another writer moved the floor — is exactly the condition a
  refresh-and-retry resolves.
- It is grouped with genuinely unrecoverable conditions (`NPE`, `ForbiddenException`) that have
  nothing in common with it.

**Recommendation:** treat as a discussion point, not a fix to push. Worth raising with maintainers
alongside [#17376](https://github.com/apache/iceberg/pull/17376), which reworks coordinator
election and may make split brain rare enough that the question is moot.
