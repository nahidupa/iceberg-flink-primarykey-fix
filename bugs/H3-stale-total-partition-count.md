# H3 — Frozen `totalPartitionCount` commits early and stamps a false watermark

| | |
|---|---|
| **Severity** | High |
| **Class** | Silent correctness / metadata corruption |
| **Status** | Unreported |
| **Present in** | `main` and PR #17925 |
| **Evidence level** | Behaviour proven; **reachability not demonstrated** |

## Summary

`totalPartitionCount` is computed **once in the coordinator's constructor** from a
`describeConsumerGroups` snapshot that is known to be partial during a rebalance. It is never
recomputed. If it is too low, the coordinator declares a commit complete before every source
partition has reported, and stamps `kafka.connect.valid-through-ts` from an incomplete set of
timestamps — publishing a watermark the table does not satisfy.

## Affected code

`Coordinator.java:101-102` — computed once, from a constructor argument:

```java
this.totalPartitionCount =
    members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
```

`CommitterImpl.java:84-87` — where that snapshot comes from, taken during a rebalance:

```java
Collection<MemberDescription> members = groupDesc.members();
if (containsFirstPartition(members, currentAssignedPartitions)) {
  membersWhenWorkerIsCoordinator = members;
```

`CommitState.java:117-124` — the quorum test, with no lower bound:

```java
boolean isCommitReady(int expectedPartitionCount) {
  if (!isCommitInProgress()) { return false; }
  if (receivedPartitionCount >= expectedPartitionCount) { ... return true; }
```

`CommitState.java:161-179` — `validThroughTs` takes `min(timestamp)` over **only the partitions
that reported**, so a short quorum yields a watermark that is too far ahead.

## Mechanism

1. A rebalance is in progress. `describeConsumerGroups` returns members whose assignments are
   partially populated — say one member with one partition, while the topic really has two.
2. This task holds the lowest partition, so it is elected and constructs a `Coordinator` with
   `totalPartitionCount = 1`.
3. The worker for source partition 0 reports. `receivedPartitionCount` reaches 1.
4. `isCommitReady(1)` returns true. A **full** (non-partial) commit fires.
5. `validThroughTs(false)` is computed from partition 0's timestamp alone and written to the
   snapshot summary.
6. Partition 1's data was never included, but the snapshot claims a watermark past it.

The count is also stale after a source topic is expanded — new partitions are never counted.

## Impact

Two distinct harms:

- **Torn commits.** Snapshots are created from a subset of the intended data.
- **A lying watermark.** `kafka.connect.valid-through-ts` is the field downstream consumers use to
  decide when data is complete. Publishing it too far ahead causes consumers to treat incomplete
  data as complete. This fails silently — there is nothing in the table to indicate it.

This can independently produce the "snapshot advertises coverage it does not have" symptom
discussed in #16282.

## Proof

```
[commit must not be declared ready while source partition 1 has not reported]
expected: null
 but was: BaseSnapshot{id=1294959816264153184, ...
   summary={kafka.connect.valid-through-ts=2026-09-08T04:19:59.372301Z,
            kafka.connect.offsets.ctl-topic.cg-connect={"0":3}, ...
            added-data-files=1, added-records=1, ...}}
```

A snapshot was created from one of two partitions, **and it carries a `valid-through-ts`**.

**Scope of this proof, corrected after independent review.** The test supplies
`totalPartitionCount = 1` directly and asserts that one response must not complete the commit. It
therefore demonstrates the *consequence* — an early commit that stamps a watermark — but it does
**not** reproduce the topology change that makes the count stale in the first place. To make this
issue-ready, add a test where a retained leader keeps its coordinator across a source-partition
expansion, and assert the resulting watermark against the true one.

## Related

Existing tests in `TestCoordinator` construct coordinators with `ImmutableList.of()` for `members`,
giving `totalPartitionCount == 0`, which makes `isCommitReady` return `true` on the first
`DataComplete`. Those tests depend on this behaviour, so any fix will need them updated. See also
[L3](L-minor-findings.md).

## Suggested fixes

| Option | Effect | Cost |
|---|---|---|
| Recompute `totalPartitionCount` at the start of each commit cycle | Tracks reality, including topic expansion | Moderate; adds an admin call per cycle, or reuse the members already fetched |
| Derive the count from `context.assignment()` across reported `DataComplete` events | No admin call | Needs a different readiness model |
| Reject `expectedPartitionCount <= 0` and refuse to elect on a partial snapshot | Prevents the worst case | Small, partial mitigation |

Related coordinator-election hardening is in
[#17376](https://github.com/apache/iceberg/pull/17376) and
[#17450](https://github.com/apache/iceberg/pull/17450), but neither addresses the frozen count.
