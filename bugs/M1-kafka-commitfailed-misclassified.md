# M1 — Kafka's `CommitFailedException` propagates as fatal

| | |
|---|---|
| **Severity** | Medium |
| **Class** | Availability |
| **Status** | Unreported |
| **Present in** | `main` and PR #17925 |
| **Evidence level** | Behaviour proven; "defect" **not** established |

## Summary

`Coordinator` imports **Iceberg's** `CommitFailedException`, but `commitConsumerOffsets()` throws
**Kafka's** same-named exception. The classifier does not match it, so the exception propagates and
kills the coordinator thread — *after* the Iceberg commit has already succeeded.

**Corrected after independent review.** The original write-up called this a misclassification and
proposed adding the exception to the retry list. Two problems with that framing:

1. Kafka [documents its own `CommitFailedException`](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/consumer/CommitFailedException.html)
   as a commit that **cannot generally be retried safely** once partitions have been reassigned.
   Blindly retrying is not obviously correct; an ownership-aware policy is needed.
2. The proof inherits `commitMaxConsecutiveFailures = 1` from `ChannelTestBase:105`, so adding the
   exception to the retry set would **still** fail the assertion. The test proves the exception
   propagates; it does not validate the proposed fix.

What is established: the failure is fatal, it occurs after a successful Iceberg commit, and it is
triggered by ordinary rebalances. Whether that is the right policy is an open design question.

## Affected code

`Coordinator.java:57`:

```java
import org.apache.iceberg.exceptions.CommitFailedException;
```

`Coordinator.java:171-174` — the classifier:

```java
if (!(e instanceof CommitFailedException)) {
  // CommitStateUnknownException, ValidationException, ForbiddenException,
  // NPE, anything else -- not retryable, terminate immediately
  throw e;
}
```

`Channel.java:154-161` — the throw site, reached from `doCommit` at `Coordinator.java:211`:

```java
protected void commitConsumerOffsets() {
  ...
  consumer.commitSync(offsetsToCommit);   // org.apache.kafka.clients.consumer.CommitFailedException
}
```

`consumer.commitSync` can also raise `RebalanceInProgressException`, and
`IllegalStateException` when the retained offsets map covers partitions no longer assigned
(see [L1](L-minor-findings.md)).

## Mechanism

1. A commit cycle fires. Every table commits successfully — snapshots exist.
2. `doCommit` calls `commitConsumerOffsets()`.
3. The control-topic group has rebalanced, so `commitSync` throws Kafka's `CommitFailedException`.
4. `commit()` tests `instanceof` against *Iceberg's* class. No match.
5. The exception is rethrown. `CoordinatorThread` catches it, sets `terminated`, and exits.
6. The sink task fails.

## Impact

Not a data bug — `clearResponses()` never runs, so the buffer is retained, and the per-table floor
filters the replay. But the connector goes down for a condition it is explicitly designed to
tolerate, and the trigger is the same rebalance scenario as #16282.

## Proof

```
[a Kafka offset-commit failure must be retried, not propagated as fatal]
Expecting code not to raise a throwable but caught
  "org.apache.kafka.clients.consumer.CommitFailedException: Commit cannot be completed since the
   group has already rebalanced and assigned the partitions to another member. ...
	at org.apache.kafka.clients.consumer.MockConsumer.commitSync(MockConsumer.java:324)
	at org.apache.iceberg.connect.channel.Channel.commitConsumerOffsets(Channel.java:160)
	at org.apache.iceberg.connect.channel.Coordinator.doCommit(Coordinator.java:211)
	at org.apache.iceberg.connect.channel.Coordinator.commit(Coordinator.java:156)
```

The stack confirms the ordering: the Iceberg commit already completed inside `Tasks.foreach`
before `commitConsumerOffsets` threw.

## Suggested fixes

| Option | Effect | Cost |
|---|---|---|
| Catch Kafka's `CommitFailedException` / `RebalanceInProgressException` around `commitConsumerOffsets()` and log-and-continue | Offsets are re-derived next cycle from the table floor | Small; **preferred** |
| Add them to the retryable set in `commit()` | Uses the existing failure budget | Small, but retrying a commit Kafka calls unretryable needs justification |
| Move `commitConsumerOffsets()` out of the failure path entirely | Cleanest separation of Iceberg and Kafka concerns | Moderate |

The first remains the most defensible: the control-topic offset is an optimisation, not the source
of truth — the authoritative floor lives in the snapshot summary — so failing to advance it should
not be fatal. Note this interacts with [H1](H1-coordinator-restart-data-loss.md): if the offset is
never committed, the H1 window stays open.
