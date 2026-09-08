# PR Description Draft

## Review Notes (Do Not Paste Into The PR)

- Suggested title: **Kafka Connect: Ignore replayed control-topic records**.
- Code branch: `fix-connect-rebalance-listener`.
- Reviewed code commit: `67007e5fd473027aa626d6b57f318842387685fb`.
- Documentation branch: `docs/connect-replay-review-notes`. Its documentation
  commit is separate from the code branch and should not be merged into this PR.
- Neither branch has been pushed. The PR body embeds a Mermaid diagram that
  GitHub renders directly; it does not depend on a hosted image or branch push.
- Local diagram preview: [Replay sequence](architecture/iceberg-16282-duplicate-files-fix-after-pr-review.html#L1).
- Local image preview: [architecture/iceberg-16282-duplicate-files-fix-after-pr-review.visual-check.1440x900.light.png](architecture/iceberg-16282-duplicate-files-fix-after-pr-review.visual-check.1440x900.light.png).
- Confirm the model/version and human oversight fields in the AI disclosure
  before publishing. Automated checks are not human review.

The proposed PR body begins below. Use a related-issue reference rather than
an automatic closing keyword because the verified recovery scope is narrower
than all scenarios in the issue.

---

## Summary

Related to [#16282](https://github.com/apache/iceberg/issues/16282).

Prevent a surviving Kafka Connect channel from applying a control-topic
record more than once after consumer rewind. Replayed records can otherwise
lower the tracked control offset and count the same `DataComplete` again,
causing a premature commit whose offset summary does not cover its files.

This revision replaces the earlier reset-on-rebalance proposal with a guard
in `Channel.consumeAvailable()`. It retains buffered file responses and
genuine readiness state instead of discarding them and relying on replay.

## Changes

- Skip a record when its offset is below the next offset already tracked for
  its control partition. The guard runs before map mutation, decoding, group
  filtering, and event dispatch.
- Accept a record equal to the next offset, and track partitions independently.
- Remove the PR-added rebalance listener, coordinator reset hook, and
  `CommitState.reset()` method.
- Preserve existing commit cleanup: clear file responses after per-table work
  and the Kafka control-offset commit succeed; end readiness and the active
  round in `finally`.
- Replace reset-specific tests with recovery assertions on files, snapshots,
  offset summaries, and subsequent replay.

The guard, with debug logging omitted, is:

```java
Long nextOffset = controlTopicOffsets.get(record.partition());
if (nextOffset != null && record.offset() < nextOffset) {
  return;
}

controlTopicOffsets.put(record.partition(), record.offset() + 1);
```

`return` skips one record's callback, not the polling loop. Using `<` rather
than `<=` preserves the next eligible record.

## Why The Previous Listener Approach Could Lose Data

`ConsumerRebalanceListener` itself is not the problem. The previous proposal
used its revocation callback to discard the coordinator's buffered state:

```text
ConsumerRebalanceListener.onPartitionsRevoked(...)
  -> Coordinator.onControlPartitionsRevoked(...)
  -> CommitState.reset()
     -> clearResponses(): discard buffered file responses
     -> endCurrentCommit(): clear readiness and the active commit ID
```

This assumed that Kafka would replay every discarded `DataWritten` response.
The callback did not establish that guarantee, and it left the channel's
`controlTopicOffsets` map intact.

### No Committed Offset: Reassignment Can Skip The Buffered Response

1. A worker writes file X, then publishes `DataWritten(X)` and advances its
  source offsets in the same Kafka transaction.
2. The coordinator consumes that response and buffers X, but has not yet
  committed it to Iceberg. Its group has no committed control offset because
  control offsets are committed only after successful per-table work.
3. A rebalance invokes the listener. `reset()` discards X's buffered response.
4. The surviving consumer is reassigned without a committed control offset.
  With the default `auto.offset.reset=latest`, it can resume at the log end,
  past X's response, without delivering it again.
5. The coordinator has no response from which to commit X. The worker's source
  offsets have already advanced, so normal source consumption need not
  regenerate it either.

**The result is a file omitted from normal table delivery.** The Parquet object
and Kafka response may still exist; this is not physical deletion. The reset
removed the surviving coordinator's reference to uncommitted work. Retaining
that response allows the revised implementation to commit X without replay,
as verified by `retainsBufferedFilesWhenRebalanceResetsToLatest`.

### Partial Replay: Progress Can Outlive The Files It Represents

This second case can happen even when committed control offsets exist:

1. The coordinator buffers files X and Y from two different control partitions.
2. The callback clears both responses but retains both tracked next offsets.
3. Only X's partition replays before a timeout commit, or Y's partition remains
  assigned and does not rewind. Y's response is not reconstructed in time.
4. The commit can omit Y while recording its partition's retained progress in
  the table summary and Kafka group checkpoint. A later replay of Y can then
  fall below the table's already-committed boundary and be filtered out.

The global reset ignores which partitions were actually revoked. A replay
guarantee for one partition would not recover the files discarded from another.
`retainsFilesDuringPartialControlReplay` checks both full revocation and a
retained-assignment variation, and verifies that both files survive.

The revised guard preserves files, readiness, and tracked progress together,
then rejects duplicate deliveries before they mutate that state. Combining
the guard with buffer clearing would be unsafe: it would suppress the replay
needed to reconstruct the state that was just discarded.

## Replay Example

```mermaid
sequenceDiagram
  participant Kafka as Control topic (partition 0)
  participant Channel
  participant Coordinator as Coordinator / CommitState
  participant Table as Iceberg table

  Note over Kafka,Coordinator: Commit A; two source assignments expected
  Kafka->>Channel: 1: DataWritten(X); 2: DataComplete(P0, A)
  Channel->>Coordinator: Deliver once: buffer X; ready 1/2
  Note over Channel,Coordinator: Channel nextOffset = 3; X retained

  Note over Kafka,Channel: Reassignment resumes from committed offset 1
  Kafka->>Channel: Replay offsets 1 and 2
  Note over Channel,Coordinator: Both below 3: skip; ready stays 1/2

  Kafka->>Channel: 3: DataWritten(Y); 4: DataComplete(P1, A)
  Channel->>Coordinator: Deliver once: buffer Y; ready 2/2
  Note over Channel,Coordinator: Channel nextOffset = 5; X and Y buffered
  Coordinator->>Table: Append X and Y once; summary {"0":5}
  Table-->>Coordinator: Table commit succeeds
  Coordinator->>Kafka: Commit consumer-group offset 5
  Coordinator->>Coordinator: clearResponses()
  Coordinator->>Kafka: Publish CommitComplete(A)
  Coordinator->>Coordinator: finally: endCurrentCommit()

  Note over Kafka,Channel: Test probe: seek back to offset 1
  Kafka->>Channel: Replay offsets 1 through 4
  Note over Channel,Coordinator: All below 5: skip; no extra snapshot
```

The example uses two source partitions, P0 and P1, whose responses arrive on
one control partition. Commit A remains active throughout the rebalance:

1. Control offsets `1` and `2` report file X and P0's completion. The channel
   stores next offset `3`; readiness is `1/2`.
2. Reassignment replays `1` and `2`. Both are skipped; X remains buffered,
   next offset stays `3`, and readiness remains `1/2`.
3. New offsets `3` and `4` report file Y and P1's completion. Readiness reaches
   `2/2`, and the coordinator commits X and Y once each with summary `{"0":5}`.
4. After table work succeeds, control offset `5` is committed, file responses
   are cleared, and `CommitComplete` is published. `finally` ends the round.
5. A deliberate test seek back to `1` cannot append those files again because
   offsets `1` through `4` remain below the channel's retained boundary `5`.

The diagram's offset-commit arrow represents the consumer-group API, not a
control-topic record. Worker publishing, round setup, and the per-table
`CommitToTable` notification are omitted. The final seek is a regression probe,
not an expected rewind behind the now-committed offset. A file legitimately
retained across successive snapshots is not a duplicate; adding its location
twice to one snapshot's live file set is the problematic outcome.

## Regression Coverage

- `retainsBufferedFilesWhenRebalanceResetsToLatest`: a buffered file still
  commits with no committed offset and no replay after reassignment.
- `commitsReplayedFilesExactlyOnce`: replay does not complete readiness early;
  genuine completion commits both distinct files with the expected snapshot
  ID and offset summary; later replay creates no extra snapshot.
- `retainsFilesDuringPartialControlReplay`: both files survive incomplete
  replay across two control partitions, including retaining one assignment.
  Snapshot and committed Kafka offsets are checked.
- `ignoresReplayedOffsetsAndAcceptsNextOffset`: replay is not dispatched, the
  map does not regress, and the next-offset boundary is inclusive for acceptance.
- `tracksPartitionsIndependentlyAndFiltersOtherGroups`: partition progress
  is independent and existing connect-group filtering remains intact.

## Validation

```sh
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test \
  :iceberg-kafka-connect:iceberg-kafka-connect:spotlessCheck \
  :iceberg-kafka-connect:iceberg-kafka-connect:checkstyleMain \
  :iceberg-kafka-connect:iceberg-kafka-connect:checkstyleTest --console=plain
```

The connector suite previously completed with **139 tests across 19 suites,
zero failures, errors, or skips**. The command was checked again before this
commit on 2026-09-07 and completed successfully; Gradle reported the tests,
Spotless, and both Checkstyle tasks as `UP-TO-DATE`, reusing those results.
The staged Java diff also passed `git diff --cached --check`.

Earlier mutation checks showed the no-replay regression failing with the reset
implementation and duplicate dispatch/premature readiness when the guard was
removed. Restoring the chosen implementation returned the tests to green.

These tests use `MockConsumer` and the existing in-memory catalog, with
assignment and rewind positions modeled explicitly. No live-broker recovery
test was run.

## Scope And Related Work

This is a same-instance, in-memory replay guard keyed by control partition and
offset. It does not add a durable checkpoint, fence competing coordinators,
change consumer defaults, repair existing duplicate entries, or deduplicate
logical events republished at new offsets. Recovery after process replacement
without durable offsets and topic-retention loss remain outside this evidence.

Existing snapshot-offset filtering and within-batch file deduplication remain
in place. Table commits and Kafka offset commits are not one atomic operation.

The approach overlaps [#17713](https://github.com/apache/iceberg/pull/17713).
The maximum-only update in [#17933](https://github.com/apache/iceberg/pull/17933) prevents
map regression but still dispatches replayed readiness events. The review
discussion also identifies overlapping guards in #17376 and #17450; their
broader coordinator changes are not part of this revision. The additional
recovery tests can accompany the consolidated fix selected by maintainers.

---

## AI Disclosure

- Model: [unknown - human to fill in]
- Platform/Tool: GitHub Copilot
- Human Oversight: [unknown - human to fill in]
- Prompt Summary: Review PR #17925's reset-based recovery design, implement a
  replay guard with recovery regressions, and draft the revised PR description
  and diagrams for human review.
