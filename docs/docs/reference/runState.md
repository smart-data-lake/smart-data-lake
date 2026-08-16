---
id: runState
title: Run State & Recovery
---

If SDLB is started with `--state-path`, it writes a *state file* for every run.
The state file records what each Action did, and it is what enables SDLB to recover a failed run on the next start.
An application name (`-n` / `--name`) is mandatory when a state path is set.

## Directory layout

```
<state-path>/
├── current/
│   └── <appName>.<runId>.<attemptId>.json
├── succeeded/
│   └── <appName>.<runId>.<attemptId>.json
└── index.json
```

* `current/` holds the state file of the run in progress, and of runs which did not finish successfully.
* `succeeded/` holds the state files of finished runs. A run is moved here as soon as it succeeds, together with the
  state files of its earlier, failed attempts.
* `index.json` is an optional one-line-per-run index, see below.

The `<runId>` is incremented by one for every new run. The `<attemptId>` is 1 for a normal run and incremented by one
for every recovery of the same run. Note that `.` separates the file name parts, so the application name must not
contain a dot or whitespace.

## When is a run recovered?

On startup SDLB reads the state file with the highest `(runId, attemptId)`, from either directory, and decides:

* If **every** Action of that run is `SUCCEEDED`, `SKIPPED` or `STREAMING`, and the state is final, the run is
  considered **finished**. SDLB starts a new run with `runId + 1`, carrying over the incremental state of the
  DataObjects (`dataObjectsState`).
* Otherwise the run is to be **recovered**: If it is in `succeeded/` directory, SDLB issues logs warning and creates an new run.
  If it is in `current/` directory, SDLB starts a new attempt with the same `runId` and `attemptId + 1`. Actions
  that are `SUCCEEDED` or `SKIPPED` are not executed again — their results and metrics are carried over into the new
  attempt. Everything else (`FAILED`, `CANCELLED`, `PENDING`, …) is executed again.

Because the state file of an attempt always contains the carried-over information of the previous attempts, the
latest state file of a run is a complete picture of the whole run.

When recovering, the command line parameters must match the ones of the run being recovered — otherwise SDLB aborts
with an assertion error rather than silently continuing a run with a different configuration.

## Accepting a failed run

Sometimes a failed run must not be retried, e.g. because the failure was analysed and accepted, or because the
missing data will never arrive. Instead of editing the state file by hand, **move it from `current/` to `succeeded/`**:

```bash
mv <state-path>/current/<appName>.<runId>.<attemptId>.json <state-path>/succeeded/
```

A state file in `succeeded/` is accepted: on the next start SDLB does not recover it, but starts a new run with
`runId + 1`. The incremental DataObject state of the accepted run is still carried over. SDLB logs a warning naming
the Actions that never completed, so it stays visible in the log why they were not retried:

```
WARN  run runId=5 attemptId=2 has unfinished actions (a2, a3), but its state file is accepted
      -> not recovering it and starting a new run instead
```

## Index file

Setting the SDL parameter `hadoopFileStateStoreIndexAppend = true` makes SDLB append one compact JSON line per
finished run to `<state-path>/index.json`, containing run id, attempt id, feed selector, start/end times, final
state, the state and output DataObjects per Action, the SDLB and application versions, and the relative path of the
state file. This is used by the local UI, see [Metadata](../getting-started/part-3/metadata.md).

## State file format version

State files carry a `runStateFormatVersion`. SDLB migrates older state files to the current format when reading them,
so an upgrade does not invalidate a pending recovery. Reading a state file written by a *newer* SDLB version fails
with an explicit error. Existing state files can also be migrated in place with the `StateMigrator` tool
(`io.smartdatalake.meta.state.StateMigrator -s <state-path>`).
