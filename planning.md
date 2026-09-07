# PR #17925 - Implementation and Review Response Plan

**PR:** https://github.com/apache/iceberg/pull/17925 (open; remote unchanged)
**Branch:** `nahidupa:fix-connect-rebalance-listener` @ `5f31cb5ba` plus uncommitted implementation
**Reviewer:** @wombatu-kun, 2026-09-07
**Status:** replay-guard implementation completed locally on 2026-09-07; not staged, committed, or pushed

## Current Implementation

The user authorized implementation after the review. The local change adopts the
replay-guard approach discussed in #17713, rather than the destructive reset or
the narrower readiness-only reset alternative.

   skips records below the per-partition next offset already handled, before
   updating offsets or dispatching events. The next offset itself remains eligible.
   timing promises. Buffered files, readiness, timestamps, and the active commit ID
   now survive a same-instance rebalance. No scheduling or offset-reset default changed.
   are flushed during revocation.
   Removed the long test-method Javadoc rather than describing an obsolete mechanism.

## Verification

Executed against the final local Java changes:

```bash
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test \
   :iceberg-kafka-connect:iceberg-kafka-connect:spotlessCheck \
   :iceberg-kafka-connect:iceberg-kafka-connect:checkstyleMain \
   :iceberg-kafka-connect:iceberg-kafka-connect:checkstyleTest
```

Result: **139 tests in 19 suites, 0 failures, 0 errors, 0 skipped**. Spotless and
both Checkstyle gates passed. The focused channel/coordinator/state suites contain
22 tests. `git diff --check` passed for tracked changes.

Regression evidence:

   reset implementation with zero snapshots, then passed with buffer preservation.
   It explicitly checks absent committed offsets and the post-rebalance log-end
   position; the buffered record is not replayed.
   two distinct file locations in one snapshot, the emitted snapshot ID, the offset
   summary, and no extra snapshot after another replay/commit cycle.
   existing committed offsets, both full revocation and retaining one partition,
   a timeout commit before replay completes, file contents, and committed offsets.
   checks partial replay, monotonic offsets, the next-offset boundary, independent
   partition tracking, and filtering of other connector groups.
   channel replay test to fail: early commit and duplicate event dispatch. The guard
   was restored and the tests rerun successfully before the full-suite run.

Limits: Kafka positions/rebalances are modeled with Kafka 3.9.2 `MockConsumer`;
no real-broker reproduction was run. This does not establish recovery after process
replacement without durable control offsets, topic retention loss, or split-brain
coordinator election. VS Code reports unresolved shaded-Guava imports and other
diagnostics on unchanged code despite successful Gradle compilation; editor
classpath configuration was not changed.

## Review Responses (Draft)

These are local notes for human review, not posted replies or resolved threads.

1. Item 1: Removed `reset()` and its Javadoc. The PR's old immediate-restart claim
    must also be removed: preserving `startTime` never guaranteed the next cycle.
2. Item 2: Accepted the loss concern and removed buffer clearing on rebalance.
    The LATEST/no-committed-offset regression now demonstrates retention. A simple
    `consumer.committed()` guard is not sufficient for retained partitions or
    incomplete replay with a surviving offset map.
3. Item 3: Removed the long Javadoc and obsolete reset commentary from the test.
4. Item 4: Added positive snapshot/file assertions and a subsequent replay cycle;
    counting `CommitToTable` alone is not the acceptance criterion.
5. Item 5: The implementation now overlaps #17713's replay guard. #17933 keeps
    offsets monotonic but still dispatches replayed records. The #17713 discussion
    identifies the same guard in #17376/#17450. Agree one change vehicle with the
    maintainers and preserve the additional recovery tests; do not present this as
    a separate, novel fix or claim comprehensive #16282 recovery.

## Before Publishing

1. Agree whether to contribute the tests to #17713 or reshape this PR. No PR has
    been closed, superseded, edited, or commented on by this implementation session.
2. If retaining this PR, proposed title: **Kafka Connect: Ignore replayed control-topic records**.
    Replace the reset narrative and diagrams, describe the overlap, and use a
    non-closing reference to #16282 unless the maintainers agree the proven scope.
    Existing local architecture assets were intentionally left untouched.
3. Update the AI disclosure to reflect AI-assisted implementation and the actual
    human review status. Do not reuse a disclosure saying all source was human-written.
4. Inspect the current branch, remote, and worktree. Stage only the intended Java
    changes, including the new channel test. Inspect the staged diff and its whitespace
    check. Keep this plan and unrelated local artifacts out of the source PR.
5. Commit/amend and push only after authorization. Preserve the required
    `Generated-by: GitHub Copilot` trailer; use `--force-with-lease` only for an
    agreed amendment after verifying the remote branch. Do not assume old test
    counts or a clean index in a later session.

## Historical Plan (Superseded)

The original assessment below is preserved for context, not execution. Its deferred
decision, comment-only scope, 19-test expectation, parent Spotless command, replay
guarantees, and option B coverage claim are superseded by the implementation and
verification above. The new regression is specific to a surviving coordinator:
process replacement already loses in-memory state without this PR.


## 1. Review inventory

| # | Location | Severity | Nature | State |
|---|---|---|---|---|
| 1 | `CommitState.java:111` | Trivial | Javadoc: drop rationale ¶ + `we` pronoun | **Ready** |
| 2 | `CommitState.java:116` | **Blocker** | Dropping `commitBuffer` can lose data | **Deferred — author deciding** |
| 3 | `TestCoordinator.java:184` | Trivial | Javadoc on test method breaks house style | **Ready** |
| 4 | `TestCoordinator.java:265` | Moderate | No positive "lands exactly once" assertion | **Blocked on 2** |
| 5 | Issue-level comment | Moderate | Position vs #17713 / #17376 / #17450 / #17933 | **Blocked on 2** |


## 2. Decisions taken



## 3. Coupling analysis

| Item | Coupled to 2? | Reasoning |
|---|---|---|
| 1 | No | ¶2 concerns `startTime`; item 2 concerns ¶1's replay claim. Dropping ¶2 is correct under every outcome of item 2. |
| 3 | No | Pure test style. Independent of `reset()` semantics. |
| 4 | Yes | "File lands exactly once after replay" presumes the buffer *was* cleared. If item 2 stops clearing it, the assertion changes shape. |
| 5 | Yes | The stated differentiator vs #17713 is "the reset also clears the buffer" — exactly what item 2 may remove. |


## 4. Item 1 — `CommitState.reset()` javadoc

**Comment:** *"The second paragraph is implementation rationale rather than the method's contract, and `That is what we want` uses a personal pronoun, which AGENTS.md rules out in comments. Drop it - the reasoning belongs in the PR description."*

**Assessment:** correct on both counts. `AGENTS.md` states *"No personal pronouns in comments"* and *"Javadoc describes the function or purpose of a class or method, not the implementation."*

**Change** — delete 4 lines:

```java
   * this coordinator was assembling; the underlying events remain on the control topic and are
   * re-read by whichever coordinator takes over.
   */
```

The `startTime` rationale already appears in the PR description.

**Residual risk:** ¶1 still asserts *"the underlying events remain on the control topic and are re-read by whichever coordinator takes over."* Item 2 argues that is false in the pre-first-commit window. If item 2 adopts "stop clearing `commitBuffer`", this sentence needs rewording — a second, additive touch to the same block. Accepted, since the alternative is leaving a reviewer comment unaddressed while item 2 stays open.


## 5. Item 3 — test method javadoc

**Comment:** *"No test method under `org.apache.iceberg.connect` carries a Javadoc block; the house style here is a one-to-three line `//` comment placed at the step it explains. Trim this to the line that says what the test drives and leave the mechanism to the PR description."*

**Verified:** `rg -c '^\s+/\*\*'` across `.../connect/channel/*.java` returns exactly one match — the block added by this PR. House style confirmed at `TestCoordinator.java` lines 147, 167, 204.

**Change** — replace the 16-line javadoc with:

```java
  // Replays the DataWritten/DataComplete pair that a rebalance rewind re-delivers, and asserts the
  // stale pair is not counted toward the in-flight commit's readiness.
  @Test
  public void testControlPartitionsRevokedRewindDoesNotDoubleCount() {
```

The mechanism it currently spells out is already carried by inline step comments at lines 218, 246 and 249.

**Optional:** the closing comment at lines 259–262 runs to 4 lines, above the stated 1–3 line norm. Not flagged by the reviewer. Trim only if desired.


## 6. Item 2 — deferred, evidence preserved

**Comment:** *"Dropping `commitBuffer` assumes every discarded `DataWritten` is re-read, but `createConsumer` leaves `auto.offset.reset` at `latest` and the `-coord` group only gets a committed offset inside `doCommit`, so a revoke before that group's first successful commit rewinds to the log end instead. Those files already had their source offsets committed by the worker's transaction - is that window intentional?"*

**Verified as correct.** Evidence chain:

1. `KafkaClientFactory.java:55` — `consumerProps.putIfAbsent(AUTO_OFFSET_RESET_CONFIG, "latest")`
2. `commitConsumerOffsets()` has exactly one call site, `Coordinator.java:228`, reached only after every table commits successfully
3. Until the `<group>-coord` group's first successful commit it therefore has **no committed offset**
4. `Channel.send()` publishes `DataWritten` **and** `producer.sendOffsetsToTransaction(sourceOffsets)` in one transaction — once committed, the worker never re-reads those source records

**Consequence:** revoke before the first successful commit → `reset()` drops `commitBuffer` → reassignment finds no committed offset → seeks to log end → buffered `DataWritten` records are never re-read. Files orphaned in object storage; source records already consumed. **Silent data loss, introduced by this PR** — pre-patch the buffer survived and was eventually committed. The window is widest during a fresh deployment, exactly when Connect rebalances most.

**Additional finding:** `clearResponses()` may not be load-bearing. Replayed envelopes re-added to a surviving buffer are collapsed by `distinctByKey(ContentFile::location)` within the batch, and stale-envelope carry-over is already the designed behaviour of `addResponse`. If that holds, removing `clearResponses()` from `reset()` closes the data-loss window at no cost — but this PR would then no longer fully close #16282, since the floor-regression half belongs to #17933.

**Options when resumed:**

| Option | Effect | Cost |
|---|---|---|
| A. `reset()` = `endCurrentCommit()` only | Window removed; premature-commit fix retained | `Closes #16282` → `Part of`; floor half deferred to #17933 |
| B. Guard `clearResponses()` on `consumer.committed()` | Full #16282 coverage retained | Added branching; harder test |
| C. Ask reviewer to consolidate into #17713 | Avoids duplicated effort | Cedes control of the fix |
| D. Defend current behaviour | No code change | Weak — default is `latest`; not recommended |


## 7. Item 4 — parked

**Comment:** *"Both new tests assert only that nothing was committed early, so nothing covers that the replayed `DataWritten` still reaches the table exactly once after the rebalance. Deliver both source partitions' `DataComplete` for the new commit id and assert the file lands in a single snapshot."*

Legitimate gap: current assertions are purely negative. Add a third phase delivering both partitions' `DataComplete` under the new commit id, then assert exactly one `CommitToTable`. Shape depends on item 2.


## 8. Item 5 — parked

**Comment (abridged):** #17713 fixes the same eager-rebalance replay from the other side; #17376 and #17450 carry a byte-equivalent guard; #17933 makes `controlTopicOffsets` monotonic. Reviewer asks the description to state the relationship.

Diffs read:

| PR | Approach | Clears buffer? |
|---|---|---|
| #17713 | Skip already-consumed records in `consumeAvailable` | No |
| #17933 | `controlTopicOffsets.merge(..., Long::max)` | No |
| #17376 / #17450 | Coordinator hardening / election rework | No |
| **#17925 (this)** | Reset in-flight `CommitState` on revoke | **Yes** |

The reviewer's differentiator and item 2's objection are the same fact seen from opposite sides. Resolve item 2 before writing this section.


## 9. Execution steps (items 1 + 3 only)

1. Edit `CommitState.java` — delete javadoc ¶2
2. Edit `TestCoordinator.java` — javadoc → 2-line `//`
3. `./gradlew :iceberg-kafka-connect:spotlessCheck`
4. `./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test --tests "...TestCoordinator" --tests "...TestCommitState"` — expect **19 tests, 0 failures**
5. `git commit --amend` (keeps one commit; message unchanged, no behaviour change)
6. `git push fork fix-connect-rebalance-listener --force-with-lease`
7. Reply to both resolved review threads

No change to `Closes #16282` or the PR description — that is item 5.


## 10. Open questions

1. Item 2 direction — A, B, C, or D above.
2. Should a holding reply be posted to @wombatu-kun on item 2 acknowledging the catch, or stay silent until decided?
3. Trim the 4-line comment at `TestCoordinator.java:259-262` to the 1–3 line norm? Not reviewer-flagged.


## 11. Housekeeping

`planning.md`, `blog.md`, `architecture/`, `hero.png`, `hero2.png`, `top.png` and `.playwright-mcp/` are untracked in the Iceberg checkout. None are staged. Per `AGENTS.md` ("One concern per PR") they must stay out of the diff.
