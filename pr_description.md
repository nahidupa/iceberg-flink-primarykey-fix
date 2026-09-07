# PR Description Draft

## Review Notes (Do Not Paste Into The PR)

- Suggested title: **Kafka Connect: Ignore replayed control-topic records**.
- Code branch: `fix-connect-rebalance-listener`.
- Reviewed code commit: `67007e5fd473027aa626d6b57f318842387685fb`.
- Documentation branch: `docs/connect-replay-review-notes`. Its documentation
  commit is separate from the code branch and should not be merged into this PR.
- Neither branch has been pushed. The hosted diagram below will become
  available after the documentation branch is pushed to the fork.
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

## Why Not Reset On Rebalance?

Clearing buffered files assumes every discarded response will be delivered
again. Before the coordinator group's first offset commit, reassignment with
the default `auto.offset.reset=latest` can resume at the log end without
replaying a buffered response. Workers may already have advanced their source
offsets in the Kafka transaction that published those responses.

With multiple control partitions, a timeout can also occur before all replay
completes, or a retained partition may not rewind. Discarding file responses
while retaining the channel's offset map can then omit files while recording
progress for them.

Retaining state and suppressing duplicate delivery address these cases
together. Combining the new guard with buffer clearing would suppress the
replay needed to reconstruct that discarded state.

## Replay Example

![Control-record replay rejected before offset and readiness updates][replay-diagram]

[replay-diagram]: https://raw.githubusercontent.com/nahidupa/iceberg-flink-primarykey-fix/docs/connect-replay-review-notes/architecture/iceberg-16282-duplicate-files-fix-after-pr-review.visual-check.1440x900.light.png

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
control-topic record. The final seek is a regression probe, not an expected
rewind behind the now-committed offset. A file legitimately retained across
successive snapshots is not a duplicate; adding its location twice to one
snapshot's live file set is the problematic outcome.

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
