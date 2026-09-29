<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# H3: Retained coordinator uses a stale partition count

Archived September 8, 2026 investigation. The test and validation described here
are historical; they do not describe the later parked fix or a new H3 PR.

## Verdict

Reproduced at the connector callback/unit-test level on updated `main`, revision
`af86b8399ca794bfe482eae85ba7e79ab0c49adc`, without the H2 fix or any production
changes. A retained coordinator can complete a cycle after only two of three
source partitions report and publish an overstated `kafka.connect.valid-through-ts`.

The reproduction establishes premature completion and an incorrect completeness
watermark. It does not establish permanent connector-side file loss: the omitted
file is committed in the next cycle. A downstream consumer that treats the
early watermark as a completeness guarantee could skip or finalize data too
soon, but that downstream loss is not exercised by this test.

## Reproduction

The historical test is `TestCoordinatorPartitionExpansion`, preserved in local
H3 investigation history but not published on this notes branch. It calls real
`CommitterImpl.open()` election/startup logic and uses a real `Coordinator`,
`CommitState`, and in-memory Iceberg catalog. It never directly assigns an
artificial quorum count or reflects into the coordinator.

1. Start with complete group metadata for two source partitions: the leader
   owns partition 0 and another worker owns partition 1. The coordinator is
   constructed and started with an accurate count of two.
2. Model topic expansion by supplying complete updated group metadata: the
   leader now owns partitions 0 and 2; the other worker still owns partition 1.
3. Call `CommitterImpl.open()` for the new assignment while retaining the leader.
   Test both an added-only callback `{2}` and a full-assignment callback `{0,2}`.
   Assert that only one coordinator thread was constructed and started, and
   that it was not terminated.
4. Begin a commit after the expanded assignment has been delivered. The leader
   announces its file and a `DataComplete` for partitions 0 and 2, both with
   timestamp `12:00Z`. Partition 1 has not reported yet.
5. Observe a full snapshot containing only the leader's file, a snapshot
   watermark and `CommitComplete` timestamp of `12:00Z`, and a coordinator
   control-topic checkpoint advancing from 1 to 3.
6. Deliver the other worker's file and completion for the old cycle, timestamped
   `11:00Z`. Confirm it has not yet entered a snapshot and the checkpoint is 3.
7. Complete the next cycle with current responses from all three partitions.
   The late file enters a second snapshot, the published timestamp is now
   `11:00Z`, and the checkpoint advances to 7.

The timestamps are deterministic completion metadata supplied by the fixture.
The correct minimum across all three partitions is `11:00Z`, not `12:00Z`.
Both snapshot summaries and completion events are asserted, as are the exact
added file locations and Kafka checkpoint offsets.

## Discriminating Control

Construct a fresh coordinator directly from the complete three-partition group
snapshot, then deliver the same two leader responses. It produces no snapshot
or completion event and keeps its checkpoint at 1. Once partition 1 reports,
it creates one snapshot containing both files, publishes `11:00Z`, and advances
the checkpoint to 5.

This distinguishes a retained stale count from an invalid fixture or a generally
broken readiness calculation. No empty or partial group description is needed.

## Root Cause

- [Coordinator construction][count] calculates the final `totalPartitionCount`
  once from the initial group members.
- [Assignment handling][open] checks whether the callback includes the leader
  partition. For an added-only `{2}` callback it leaves the existing coordinator
  untouched. For `{0,2}`, the [startup guard][guard] also keeps the existing
  coordinator, even though the committer has read fresh member metadata.
- [Readiness accounting][ready] sums reported assignment counts. The leader's
  two assignments satisfy the obsolete expected count of two.
- [Watermark calculation][watermark] takes the minimum of reported timestamps
  for a full commit. The unreported partition's lower timestamp is absent.

## Validation

The initial safety probe asserted that two of three partitions must not create
a snapshot. The retained-coordinator case failed with a snapshot whose summary
included `kafka.connect.valid-through-ts=2026-09-08T12:00Z`; the fresh-coordinator
control passed.

The saved tests now explicitly characterize the defect instead of leaving an
intentional failing test in the worktree. Their passing result does not mean
H3 is fixed. A future fix should make the retained cases satisfy the fresh
coordinator's waiting behavior and remove the premature-watermark expectations.

- H3 class: three passing cases (two retained callback shapes and one control).
- Neighboring classes: 12 `TestCoordinator` and four `TestCommitterImpl` tests.
- Total: 19 tests, zero failures, errors, or skips.
- Connector `spotlessCheck` and `checkstyleTest` passed.
- Gradle compilation passed. The editor still reports its existing relocated
  Guava classpath issue for the required `Lists` import.

On the historical investigation checkout containing the test, run:

```sh
module=:iceberg-kafka-connect:iceberg-kafka-connect
./gradlew -DsparkVersions= -DflinkVersions= -DkafkaVersions=3 \
  "$module:test" \
  --tests org.apache.iceberg.connect.channel.TestCoordinatorPartitionExpansion
```

## Limits

Kafka clients, group descriptions, and coordinator thread scheduling are mocked.
The actual coordinator is captured during thread construction and driven
synchronously. No live broker, topic expansion, or Connect cluster was run.
The test verifies the connector's behavior for retained-leader callbacks; it
does not establish which deployed assignors produce that sequence. Rebalances
that revoke the leader and recreate its coordinator from complete metadata are
outside this failing scenario, as illustrated by the fresh-coordinator control.

The original audit's separate partial-group-metadata-at-startup allegation is
not established here. Snapshot expiration, restart recovery, and downstream
consumer data loss are also outside this reproduction.

No production fix, commit, push, or new PR was made for H3. H2 remains separate
in [PR #18012](https://github.com/apache/iceberg/pull/18012).

[count]: https://github.com/apache/iceberg/blob/af86b8399ca794bfe482eae85ba7e79ab0c49adc/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Coordinator.java#L101
[open]: https://github.com/apache/iceberg/blob/af86b8399ca794bfe482eae85ba7e79ab0c49adc/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CommitterImpl.java#L141
[guard]: https://github.com/apache/iceberg/blob/af86b8399ca794bfe482eae85ba7e79ab0c49adc/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CommitterImpl.java#L218
[ready]: https://github.com/apache/iceberg/blob/af86b8399ca794bfe482eae85ba7e79ab0c49adc/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CommitState.java#L117
[watermark]: https://github.com/apache/iceberg/blob/af86b8399ca794bfe482eae85ba7e79ab0c49adc/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/CommitState.java#L146
