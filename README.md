# Pinned-version export console

A chunked export engine for a source that keeps receiving corrections and
deletions while the export runs. Every part of one archive is derived from a
single registered data version, a resume never re-reads a newer version, and
retention cleanup can never delete files that someone can still resume or
download.

Built on Node's built-in `node:sqlite` — no external dependencies.

```bash
node src/server.js            # http://127.0.0.1:7317
PORT=8080 DATA_DIR=/tmp/exp node src/server.js
node --test test/pipeline.test.js test/http.test.js
```

The dashboard at `/` starts exports, edits the live source, splits/merges
unexported windows, re-issues exported ones, pauses/resumes, verifies individual
parts, downloads the archive as a ZIP, measures pin costs, and runs retention.

## The problem this solves

The source is append-only versioned. A row's state is the newest entry in
`changes` at or below a version. Export workers pull windows of ids; the source
may be corrected or rows deleted at any moment. If worker N read version 40
while worker N+1 read version 97, the archive would mix two states and a
correction could appear in some parts and not others.

The fix is a **pin registration**: the job records one version and one
partition of the id space. Only that version is ever read, for every window,
including windows re-derived after a pause, a crash, or a re-issue.

## Two pin strategies, both correct, different costs

| | `snapshot` | `replay` |
|---|---|---|
| What is kept | a private SQLite copy of the pinned rows | nothing but the version number |
| Disk held | full duplicated row set, for the job's life | only the change-log history it needs |
| Pin cost | one-time copy (`ATTACH` + `INSERT … SELECT` inside one read transaction) | none |
| Resume cost | zero rebuild; read the copy | re-derives every pending window from the log |
| Log pruning | unconstrained | cannot prune below the oldest live replay pin |

Neither is "better": the console measures both on a sample of real windows and
shows the measured bytes and milliseconds side by side. Every figure in the
"What a pin really costs" panel comes from an actual build and an actual replay
on the current data — nothing is assumed.

## Invariants

1. **One version per archive.** `jobs.pinned_version` is written before the
   first window is claimed. `ReplayReader` binds `:version` into a single SQL
   query; `SnapshotReader` reads a file materialised inside one read
   transaction. Both assert `row.seq <= pinned_version` during verification.
2. **Committed parts are immutable.** A committed window's bytes, hash and part
   name are recorded; nothing rewrites them except an explicit re-issue, which
   deletes the file and derives it again *from the same pin*.
3. **Part names are never reused.** `jobs.part_seq` allocates
   `part-NNNNNN.jsonl`; renumbering windows after a split never renames a file.
4. **A torn file is never adopted.** Every part streams to `.tmp`, is fsynced,
   renamed, the directory is fsynced, and only then is the commit record
   written. Recovery resets `writing` windows and removes their partial files.
5. **Cleanup cannot break a recoverable export.** Collection takes an exclusive
   `gc` lease, re-checks every other lease, and refuses if the job has a live
   export/download/revise lease, an in-process worker, or is paused rather than
   expired. Downloads hold a `download` lease and extend retention.
6. **Bounded memory.** Writers hold ~256 KiB of buffered output; the ZIP is
   streamed part by part. A 30k-row / ~27 MB export runs with < 120 MiB heap
   growth (asserted in the tests).

## Layout

```
src/source.js     versioned source: rows + append-only change log, replay SQL, pruning
src/state.js      job/window/lease/verification registry (SQLite)
src/pin.js        build/remove the materialised snapshot (single read transaction)
src/export.js     streaming part writer, snapshot & replay readers, worker engine
src/verify.js     re-read a part from disk and check every recorded fact
src/zip.js        store-method ZIP streamer, incremental CRC-32
src/cost.js       real measurement of both pin strategies
src/revise.js     split / merge / re-issue / retarget
src/gc.js         retention planning and collection under a lease
src/workspace.js  composition root + crash recovery
src/server.js     REST API, ZIP download, static dashboard
public/           vanilla ES-module dashboard
test/             pipeline (15) and HTTP (8) tests, all against real SQLite
```

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/overview` | source stats, storage, jobs, GC plan, events, last probe |
| POST | `/api/source/generate` | regenerate the fictional dataset (blocked by live pins) |
| POST | `/api/source/mutate` | append corrections/deletions/inserts |
| POST | `/api/source/prune` | prune superseded log entries, never below the oldest replay pin |
| POST | `/api/jobs` | create a job (`strategy`, `windowWidth`, `workers`, `retentionSec`, `autoStart`) |
| GET | `/api/jobs/:id` | job + windows + leases |
| POST | `/api/jobs/:id/start\|pause\|resume\|cancel` | lifecycle |
| POST | `/api/jobs/:id/revise` | `split` / `merge` / `reissue` / `retarget` |
| POST | `/api/jobs/:id/verify` | `quick` or `deep`, optional `indices` |
| GET | `/api/jobs/:id/manifest` | manifest with each part's range and checksums |
| GET | `/api/jobs/:id/download` | streamed ZIP (manifest + all committed parts) |
| GET | `/api/jobs/:id/parts/:idx` | one part with `x-part-sha256` / `x-part-rows` |
| POST | `/api/jobs/:id/collect` | force collection (refused while protected) |
| POST | `/api/gc/plan`, `/api/gc/sweep` | retention view and sweep |
| POST | `/api/cost/probe` | measure both strategies on the current data |

## Verification

`verify` re-reads each part **from disk** and checks size, SHA-256, CRC-32, row
count, first/last id, JSON shape, ascending ids, that the id range matches the
window, and that every `seq` is ≤ the pinned version. Deep mode additionally
compares every row against the pinned reader, so a tampered or stale part fails
rather than merely looking well-formed.
