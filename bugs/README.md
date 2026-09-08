# Iceberg Kafka Connect — Audit Findings

Independent audit of `kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/`,
carried out while reviewing [PR #17925](https://github.com/apache/iceberg/pull/17925) and
[issue #16282](https://github.com/apache/iceberg/issues/16282).

Each finding below carries an explicit **evidence level**. A failing test proves that code
behaves a certain way; it does **not** by itself prove that behaviour is a defect, because the
assertion encodes an expectation that may be wrong. Findings are labelled accordingly.

These findings were independently reviewed by a second model on 2026-09-08. That review found
three genuine errors in the original write-up (M3's fixture, M1's framing, L3's reachability).
Those corrections are folded in below and attributed.

- Audited revision: working tree on branch `fix-connect-rebalance-listener`, base `5f31cb5ba`,
  **including** the uncommitted `consumeAvailable` skip guard.
- Date: 2026-09-07
- Harness: [`TestBugProofs.java`](TestBugProofs.java) — drop into
  `kafka-connect/kafka-connect/src/test/java/org/apache/iceberg/connect/channel/` and run:

```bash
./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test \
  --tests "org.apache.iceberg.connect.channel.TestBugProofs"
```

Expected result: **7 tests, 7 failures.** A failure demonstrates the described behaviour; see
each finding's *evidence level* for whether that behaviour is a defect.

Two proofs here are **known-unsound** and must not be cited:

- `proofM3_…` — fails on a fixture error, not the behaviour it claims. See [M3](M3-worker-orphaned-files.md).
- `proofH1_…` — queued its record on only one of two consumers, so it failed for the wrong reason.
  H1 itself is real; the corrected reproduction is in the fix PR. See [H1](H1-coordinator-restart-data-loss.md).

Both are retained so the flaws stay reproducible.

## Findings

| # | Severity | Evidence level | Title |
|---|---|---|---|
| [H1](H1-coordinator-restart-data-loss.md) | **High** | **Proven defect** | Coordinator replaced before the group's first commit skips already-published file responses |
| [H2](H2-missing-table-silent-discard.md) | **High** | Behaviour proven; impact narrowed | Missing table discards buffered responses and advances offsets |
| [H3](H3-stale-total-partition-count.md) | **High** | Behaviour proven; reachability **not** demonstrated | Frozen `totalPartitionCount` commits early and stamps a watermark |
| [M1](M1-kafka-commitfailed-misclassified.md) | Medium | Behaviour proven; "defect" **not** established | Kafka's `CommitFailedException` propagates as fatal |
| [M2](M2-validation-exception-fatal.md) | Medium | Design discussion, not a defect | `ValidationException` terminates instead of retrying |
| [M3](M3-worker-orphaned-files.md) | Medium | **Proof invalid** — analysis only | Worker can orphan data files when the publish fails |
| [L](L-minor-findings.md) | Low | L1/L2 analysis only; L3 unit-level only | Three smaller issues |

Only **H1** is put forward as an issue-ready, independently reproducible defect.

## Relationship to #16282 and PR #17925

The skip guard in `Channel.consumeAvailable` (Channel.java:120-133) correctly closes the
offset-regression and readiness-double-count mechanism reported in #16282.

**It does not close the data-loss window.** H1 shows that window is caused by *coordinator
restart*, not by the `CommitState.reset()` that PR #17925 originally added and has since removed.
Removing `reset()` did not remove the window — it exists in unmodified `main` too.

H3 can independently produce the symptom described in the issue thread: a snapshot that advertises
coverage it does not actually have.

## Scope note

None of these are in the PR diff. Per `AGENTS.md` ("One concern per PR") they belong in separate
issues or PRs. This folder is documentation only — no production code was changed to produce it.
