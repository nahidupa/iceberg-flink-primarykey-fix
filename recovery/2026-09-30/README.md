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

# Resume Iceberg Work on Another Laptop

This is a fork-only preservation snapshot dated September 30, 2026. It does
not depend on files remaining on the original laptop. Do not merge this
recovery branch into an upstream code PR.

## Start Working

```sh
git clone https://github.com/nahidupa/iceberg-flink-primarykey-fix.git iceberg
cd iceberg
git remote add upstream https://github.com/apache/iceberg.git
git switch fix-connect-assignment-freshness
```

The normal working branches remain separate:

| Work | Branch | Upstream PR |
| --- | --- | --- |
| H1: coordinator restart recovery | `fix-connect-coordinator-offset-reset` | [PR18006](https://github.com/apache/iceberg/pull/18006) |
| H2: table lookup failures | `fix-connect-table-lookup-failures` | [PR18012](https://github.com/apache/iceberg/pull/18012) |
| H3: assignment freshness | `fix-connect-assignment-freshness` | [PR18322](https://github.com/apache/iceberg/pull/18322) |
| Approved readiness fix, unchanged | `fix-connect-rebalance-listener` | [PR17925](https://github.com/apache/iceberg/pull/17925) |
| Bug investigation notes | `docs-connect-bug-investigations` | Fork only |
| Earlier replay notes and diagrams | `docs-connect-replay-review-notes` | Fork only |

H2 remains a draft PR. Keep the three bug fixes independent. Do not modify
PR17925 without renewed authorization.

## Recover an Older Branch

All 18 original local branch heads have exact dated copies under
`archive/laptop-reset-20260930/heads/`. This includes the parked H3 work,
pre-rebase backups, and the five compatibility branches.

For example, to resume the tested combined compatibility tree:

```sh
git switch -c resume-combined origin/archive/laptop-reset-20260930/heads/verify/connect-all-pr17925-20260929
```

[manifest.json](manifest.json) records the original branch names, exact
object IDs, tag locations, recovered commits, and file checksums. All 64
original local tags exist either under their original fork names or under
`archive/laptop-reset-20260930/tags/`. Seven missing tags were archived;
existing tags were not replaced.

## Recover the Stashed PR Description

The original stash is preserved as a Git commit with its original parents,
including its index state. These commands restore its working state on a
new branch without changing any PR branch:

```sh
git switch -c recovered-pr-description 'origin/archive/laptop-reset-20260930/stash-0^1'
git stash apply --index origin/archive/laptop-reset-20260930/stash-0
```

Alternatively, register it in the new clone's stash list without applying it:

```sh
git stash store -m "Recovered PR-description draft" "$(git rev-parse origin/archive/laptop-reset-20260930/stash-0)"
```

## Recover Historical Staging Objects

Five historical commits that were no longer referenced locally have their
own branches under `archive/laptop-reset-20260930/recovered/`.

The historical object pack also preserves earlier staged file and tree
objects that had no remaining branch. It is only needed for investigating
older drafts, not for normal work on H1, H2, or H3. Import it into the new
clone's Git database without changing the checked-out files:

```sh
git show origin/archive/laptop-reset-20260930/workspace-notes:recovery/2026-09-30/historical-objects.pack | git index-pack --stdin
```

The manifest lists each preserved object ID and type. After importing the
pack, use `git show <object-id>` or `git cat-file -p <object-id>` to inspect
an older object. Objects are historical evidence, not additional validated
fixes.

## Project Settings and Evidence

Reviewed project-local IDE settings and agent hook files are stored in
[workspace-settings.tar.gz](workspace-settings.tar.gz) as 175 inactive archival
copies. Some contain old absolute paths. Inspect the archive in a separate
directory before restoring selected preferences on a new laptop;
regenerating IDE import metadata is usually preferable.

The `published-pr-descriptions` directory preserves the local copies of the
three published PR descriptions. Their validation reports describe the
September 29 runs, not current GitHub CI status.

The final combined compatibility head is
`25771fca3aa119c40a52423a67f0346d20f1aa6f`. Its connector check passed 190
tests across 24 suites with no failures, errors, or skips, alongside
Spotless, Checkstyle, and class-uniqueness checks. Local validation used
Corretto 21.0.3; it did not substitute for JDK17 CI or live-broker testing.

## Scope and Exclusions

This snapshot covers this Iceberg repository and its six original linked
worktrees. Their tracked working states were clean when captured. The
original `bugs` and `.vscode` directories were empty at capture time; the
investigation notes are preserved on the separate notes branch.

Build outputs, dependency caches, downloaded Gradle wrapper binaries,
generated Eclipse project/classpath files, and macOS metadata are not saved.
They can be regenerated. Credentials, SSH keys, machine-wide configuration,
editor chat history, and unrelated repositories are outside this snapshot.
Set up GitHub authentication and the required JDK on the new laptop.

Do not copy the old worktree Git pointer files to another machine. Clone
from GitHub and create new worktrees there when needed.
