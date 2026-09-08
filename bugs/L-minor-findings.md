# L — Minor findings

Three lower-severity issues found during the same audit. Only L3 has a proof; L1 and L2 are
analysis-only and are labelled as such.

---

## L1 — Offsets committed for partitions no longer assigned

**Severity:** Low · **Status:** analysis only, not independently proven

`Channel.java:154-161` commits the entire retained `controlTopicOffsets` map:

```java
protected void commitConsumerOffsets() {
  Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();
  controlTopicOffsets()
      .forEach((k, v) -> offsetsToCommit.put(new TopicPartition(controlTopic, k), new OffsetAndMetadata(v)));
  consumer.commitSync(offsetsToCommit);
}
```

The map accumulates every partition this channel has ever consumed and is never pruned on
rebalance. After partitions are revoked, `commitSync` is asked to commit offsets the consumer no
longer owns, which Kafka rejects.

This is one of the triggers for [M1](M1-kafka-commitfailed-misclassified.md), where the resulting
exception is misclassified as fatal.

**Suggested fix:** intersect the map with `consumer.assignment()` before committing.

---

## L2 — Overlapping worker assignments inflate the readiness quorum

**Severity:** Low · **Status:** analysis only, not independently proven

`CommitState.java:69`:

```java
receivedPartitionCount += dataComplete.assignments().size();
```

The counter sums *claimed* partitions without deduplicating. During a rebalance two workers can
transiently claim the same source partition; both report, and the quorum is reached without full
coverage.

Transient and self-correcting on the next cycle, but it is the same failure shape as
[H3](H3-stale-total-partition-count.md): a commit declared complete while data is missing, with
`valid-through-ts` computed from an incomplete set.

**Suggested fix:** track the set of reported `TopicPartition`s rather than a running count.

---

## L3 — `isCommitReady` returns true with zero responses

**Severity:** Low · **Status:** unit-level behaviour proven; **not reachable in production**

`CommitState.java:117-124`:

```java
boolean isCommitReady(int expectedPartitionCount) {
  if (!isCommitInProgress()) { return false; }
  if (receivedPartitionCount >= expectedPartitionCount) { ... return true; }
```

With `expectedPartitionCount == 0` the comparison is `0 >= 0`, so a commit is "ready" having heard
from nobody. There is no lower bound on the expected count.

```
[a commit with zero expected AND zero received partitions must not be ready]
Expecting value to be false but was true
```

**Corrected after independent review.** An earlier version claimed this was reachable via an empty
member snapshot. It is not: `CommitterImpl.containsFirstPartition` returns `false` when
`findFirstTopicPartition(members) == null`, so a snapshot with no assignments never elects a
coordinator and no `Coordinator` is constructed. The unit result is real, but the production path
is guarded.

It remains worth a defensive bound, and several `TestCoordinator` tests pass `ImmutableList.of()`
as `members` precisely because the guard is absent at this level.

**Suggested fix:** require `expectedPartitionCount > 0` and `receivedPartitionCount > 0`, or
refuse to elect a coordinator from a member snapshot with no assignments.
