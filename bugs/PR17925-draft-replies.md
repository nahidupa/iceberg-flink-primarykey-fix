# PR #17925 - posted replies and original drafts

Historical September 9, 2026 record, archived on the fork-only notes branch.
Statements about the current branch, readiness algorithm, test names, and review
status below apply to that date. Original drafts contain superseded claims and
are not current PR guidance or authorization to post additional replies.

**Posted on 2026-09-09:** nine inline replies and one conversation summary,
using the reviewed wording. Draft items 2 and 11 were combined in their shared
thread. The original draft bodies below were not posted verbatim.

No threads were resolved. The PR title, description, and code were not changed
as part of posting these replies.

| Draft item | Posted reply |
| --- | --- |
| 1: Javadoc rationale | [Reply](https://github.com/apache/iceberg/pull/17925#discussion_r3967730104) |
| 2 and 11: Recovery, consolidation, snapshot expiration | [Combined reply](https://github.com/apache/iceberg/pull/17925#discussion_r3967730497) |
| 3: Test Javadoc | [Reply](https://github.com/apache/iceberg/pull/17925#discussion_r3967730883) |
| 4: Positive file assertion and coverage limits | [Reply](https://github.com/apache/iceberg/pull/17925#discussion_r3967733540) |
| 5: Consolidation and validation summary | [Summary](https://github.com/apache/iceberg/pull/17925#issuecomment-5600930892) |
| 6: Rebase and readiness scope | [Reply](https://github.com/apache/iceberg/pull/17925#discussion_r3967734008) |
| 7: Merged TestChannel | [Reply](https://github.com/apache/iceberg/pull/17925#discussion_r3967734389) |
| 8: Old test did not reach the guard | [Reply](https://github.com/apache/iceberg/pull/17925#discussion_r3967736498) |
| 9: Test helper duplication | [Reply](https://github.com/apache/iceberg/pull/17925#discussion_r3967736923) |
| 10: Negative-control verification | [Reply](https://github.com/apache/iceberg/pull/17925#discussion_r3967737331) |

## Original Drafts (Not Posted Verbatim)

Branch is now `2e9d2e927` off current `main`. Description already rewritten and live.

Five of these threads point at code that no longer exists. I've written them to say
so explicitly rather than resolve them silently, because silently closing a
reviewer's thread reads as ignoring it.

---

## 1. `CommitState.java:111` — javadoc rationale + `we` pronoun
**id `3948388801`** · wombatu-kun 2026-09-07

> The second paragraph is implementation rationale rather than the method's contract, and `That is what we want` uses a personal pronoun...

**Draft reply:**

You were right, and the method that carried it is gone. `CommitState.reset()` was
withdrawn along with the whole reset-on-rebalance approach after your next comment,
so both the pronoun and the rationale paragraph went with it.

The point stuck though: the new code keeps its rationale in the comment on
`addReady` describing *why* a partition can be reported twice, and the reasoning
about alternatives sits in the PR description.

---

## 2. `CommitState.java:116` — is the data-loss window intentional?
**id `3948388808`** · wombatu-kun 2026-09-07

> Dropping `commitBuffer` assumes every discarded `DataWritten` is re-read... is that window intentional?

**Draft reply:**

Not intentional, and the approach is gone. This was the most valuable comment on
the PR — it invalidated the premise, not just the wording.

`reset()` is removed entirely. Nothing in this PR clears buffered responses now.
The PR also no longer touches `Channel`, so there is no replay assumption left to
be wrong about.

For completeness on the recovery half you identified: a coordinator that is
*replaced* still cannot recover its predecessor's buffer when the `-coord` group
has no committed offset, because `auto.offset.reset` is `latest`. That is #18006,
kept separate from this one.

---

## 3. `TestCoordinator.java:184` — javadoc on a test method
**id `3948388814`** · wombatu-kun 2026-09-07

> No test method under `org.apache.iceberg.connect` carries a Javadoc block...

**Draft reply:**

Correct, and that test no longer exists — the branch was rebuilt on current `main`
and the reset-era tests went with it.

The convention is applied in the replacements: the new cases in `TestCommitState`
and `TestCoordinator` use short `//` comments at the step they explain, and the
mechanism lives in the PR description.

---

## 4. `TestCoordinator.java:265` — nothing proves the file lands exactly once
**id `3948388819`** · wombatu-kun 2026-09-07

> Both new tests assert only that nothing was committed early... Deliver both source partitions' `DataComplete` for the new commit id and assert the file lands in a single snapshot.

**Draft reply:**

Added, in the shape you asked for.
`TestCoordinator.testReplayedDataCompleteStillCommitsTheFileExactlyOnce` uses a real
two-partition member list, replays both the `DataWritten` and the `DataComplete`,
asserts no snapshot exists yet, then lets the second partition report and asserts:

```java
assertThat(table.snapshots()).hasSize(1);
assertThat(...addedDataFiles()).extracting(DataFile::location)
    .containsExactly(dataFile.location());
```

So the negative and the positive are both covered: the replay does not complete the
quorum, and the file still reaches the table exactly once afterwards.

---

## 5. Issue-level — consolidation across #17713 / #17376 / #17450 / #17933
**issue comment** · wombatu-kun 2026-09-07

> ...as far as I can tell the reset is the only one of the four that also clears a coordinator's buffer after it loses its partitions, which a skip guard leaves intact.

**Draft reply:**

Consolidated. This PR no longer carries a skip guard, so the overlap with #17713 is
gone rather than merely disclosed.

What is left is the one thing none of the four addressed: `CommitState.addReady`
summed `assignments().size()`, so a redelivered `DataComplete` — or two workers
transiently claiming the same partition — satisfied the quorum before every
partition had reported. Readiness now tracks the set of reported partitions.

That is deliberately orthogonal: #17933 keeps the offsets monotonic, #17713 skips
already-consumed records, and this makes readiness immune to a response being
counted twice regardless of whether it was skipped.

---

## 6. `Channel.java:129` — rebase; lead with the double-count
**id `3964288760`** · wombatu-kun 2026-09-09

> `main` now merges this offset with `Long::max`... lead with the replayed `DataComplete` double-count in `CommitState.addReady`, which the merge does not address.

**Draft reply:**

Done, and this is now the whole PR.

To be precise about the mechanics: `git rebase` conflicted on the first commit —
the one that added `reset()`, which a later commit in the same branch reverted — so
rather than replay contradictory history onto a changed `main`, I rebuilt the branch
from current `main` as a single commit. The previous four commits are preserved on a
local ref. The branch is now level with `main` and `Channel.java` is untouched.

Your framing of the remaining delta was the useful part. The set-based readiness is
also strictly stronger than skipping: a replay that is *not* behind the tracked
position is dispatched normally and would still be counted twice, and skipping does
nothing for two workers claiming one partition during a rebalance.

---

## 7. `TestChannel.java:42` — collides with the merged `TestChannel`
**id `3964288766`** · wombatu-kun 2026-09-09

> A `TestChannel` already exists at this path on `main`... this guard makes that five. Fold these cases into that class and change the assertion deliberately.

**Draft reply:**

No longer applicable, and thank you for catching it before it became a problem.

Since the skip guard is deferred to #17713, this PR adds no `TestChannel` and
`Channel.java` is unchanged. The merged class and its `hasSize(7)` assertion are
untouched here. Whichever PR carries the skip inherits that deliberate change, as
you described.

---

## 8. `TestCoordinator.java:141` — the test never reaches the guard
**id `3964288771`** · wombatu-kun 2026-09-09

> No record is re-delivered here - `rebalance` clears the mock's buffer... Was the intent to pin buffer retention rather than the skip?

**Draft reply:**

Correct on both counts, and the test is gone with the rebuild.

Your reading of the intent was right: it was pinning buffer retention, not the skip,
and with no rebalance listener left there was nothing for it to protect. The
replacement tests re-deliver records explicitly and were checked by reverting the
fix to confirm they fail.

---

## 9. `TestCoordinator.java:228` — reuse `dataWrittenEvent`
**id `3964288778`** · wombatu-kun 2026-09-09

> `dataWrittenEvent` in this class builds exactly this event... Use it here and in `retainsBufferedFilesWhenRebalanceResetsToLatest`.

**Draft reply:**

Both of those tests were removed in the rebuild, so the duplication is gone. The
helper was one this PR had added; it is not on `main`, and the current tests build
their events inline in the style of the surrounding class.

---

## 10. `TestCoordinator.java:370` — both partial-replay cases pass with the guard removed
**id `3964288781`** · wombatu-kun 2026-09-09

> ...the replay is exactly one offset behind, so the unguarded put rewrites the same value, and `distinctByKey` collapses the duplicate envelope.

**Draft reply:**

Verified before acting on it, and you were right: one record per partition at offset
1, replayed at offset 1, so with `merge(..., Long::max)` the map never regresses and
`distinctByKey` absorbs the envelope. Neither assertion could fail either way.

Those tests are gone. More usefully, this is the argument for the change the PR now
makes: an offset-based guard only helps when the replay is behind the tracked
position, whereas readiness that counts partitions is correct regardless.

Every new test here was checked by reverting the fix — the reported-partition `Set`
back to a `List` — and confirming exactly the three new cases fail and nothing else.

---

## 11. `CommitState.java:116` — consolidate + rebase; expired-snapshot floor
**id `3964297068`** · wombatu-kun 2026-09-09

> Consolidate, and rebase first... On the durable side, the only cross-restart floor is the `kafka.connect.offsets` summary that `lastCommittedOffsetsForTable` finds by walking snapshot ancestry, so expiring that snapshot drops it - separate from both #18006 and this guard.

**Draft reply:**

Both done: rebuilt on current `main`, and consolidated by dropping the skip in
favour of #17713.

On the durable floor — agreed, and it is out of scope here. `lastCommittedOffsets`
returns an empty map when no ancestor carries the property, and `SnapshotProducer`
does not propagate custom summary properties, so a snapshot written by another
engine does not carry it forward. Once every snapshot that did carry it has been
expired, the floor is gone. That is recorded as a known limitation on #18006 rather
than treated as something `earliest` solves, and it is untouched by this PR.

---

## Notes for you before I send

- **Thread 5 is an issue comment**, not a review comment, so it posts to the
  conversation tab rather than inline.
- Threads **1, 3, 8, 9, 10** all say some version of "that code is gone". That is
  honest, but five of them in a row may read as dismissive. I can compress them into
  one summary comment plus short pointers if you prefer.
- I have **not** claimed any thread is resolved. Marking threads resolved is yours
  to do.
- Nothing here claims a real-broker test was run.
