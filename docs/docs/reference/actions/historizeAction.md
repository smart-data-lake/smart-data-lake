---
id: historizeAction
title: HistorizeAction
---

HistorizeAction copies data from one input to one output DataObject and keeps **all versions of every record**, identified by the primary key of the output table.
It creates a technical history by adding validity columns to each record, a pattern also known as *Slowly Changing Dimension Type 2*.
Every time a record changes in the source, the current version is closed and a new version is inserted. Records which are no longer delivered by the source are closed as well.

If you only need the latest state of every record, use the cheaper [DeduplicateAction](deduplicateAction.md).

## Requirements

* The output DataObject must be a transactional table supporting SQL merge, e.g. DeltaLakeTableDataObject, IcebergTableDataObject, JdbcTableDataObject or SparkConnectTableDataObject.
* The primary key must be defined on the output table (`table.primaryKey`). It defines what "the same record" means.
* The input data must be unique across that primary key. By default `dropDuplicates(primary key)` is applied, which is non-deterministic and can make attributes flip between runs. Set `checkInputUnique = true` to fail fast with details about the duplicate records instead.

## Technical columns

HistorizeAction adds the following technical columns to the output:

| Column | Description |
| ------ | ----------- |
| `dl_ts_captured` | Timestamp from which this version is valid (valid-from). |
| `dl_ts_delimited` | Timestamp until which this version is valid (valid-to). The current version carries the upper horizon timestamp `9999-12-31 00:00:00`. |
| `dl_hash` | Hash over the compared columns, used to detect changes without transferring all data. Added by incremental historization. |
| `dl_dummy` | Helper column needed to work around a limitation of the SQL merge statement. Added by CDC historization instead of `dl_hash`. |

The names of `dl_ts_captured` and `dl_ts_delimited` can be changed globally with the SDLB parameters `capturedColumnName` and `delimitedColumnName`, the upper horizon with `historizationUpperHorizonTimestamp`.

The current version of the data is therefore selected with:

```sql
select * from int_airports where dl_ts_delimited = '9999-12-31 00:00:00'
```

## Basic example

```
actions {
  historize-airports {
    type = HistorizeAction
    inputId = stg-airports
    outputId = int-airports
  }
}

dataObjects {
  int-airports {
    type = DeltaLakeTableDataObject
    path = "~{id}"
    table {
      db = "default"
      name = "int_airports"
      primaryKey = [ident]
    }
  }
}
```

Assume the first run at `2024-03-01 04:00:00` delivers two airports, and the second run at `2024-03-02 04:00:00` delivers a new name for `LSZB` and no longer delivers `LSGG`.
The resulting history is:

| ident | name | dl_ts_captured | dl_ts_delimited |
| ----- | ---- | -------------- | --------------- |
| LSZB | Bern Belp | 2024-03-01 04:00:00 | 2024-03-02 03:59:59.999 |
| LSZB | Bern Belp Airport | 2024-03-02 04:00:00 | 9999-12-31 00:00:00 |
| LSGG | Geneva Airport | 2024-03-01 04:00:00 | 2024-03-02 03:59:59.999 |

`LSZB` got a second version, the first one closed just before the new one starts. `LSGG` has no current version anymore, which is how a delete is represented in the history.

## The time axis

By default a history with **closed intervals** is created: both `dl_ts_captured` and `dl_ts_delimited` are inclusive.
`timeAxisUnit` defines the offset between the valid-to of the previous version and the valid-from of the new one. It defaults to `1ms`, which explains the `03:59:59.999` above. The format is `x(ns|us|ms|s|m|h|d)`:

```
    timeAxisUnit = 1d
```

Setting `timeAxisUnit = 0` creates a history with **half-open intervals**, where valid-from is inclusive and valid-to is exclusive. The previous version is then closed with exactly the timestamp at which the new version starts:

| ident | name | dl_ts_captured | dl_ts_delimited |
| ----- | ---- | -------------- | --------------- |
| LSZB | Bern Belp | 2024-03-01 04:00:00 | 2024-03-02 04:00:00 |
| LSZB | Bern Belp Airport | 2024-03-02 04:00:00 | 9999-12-31 00:00:00 |

Half-open intervals make queries easier (`where ts >= dl_ts_captured and ts < dl_ts_delimited`) and avoid gaps, so prefer them for new pipelines.

## Change detection

A new version is created only if one of the compared columns changed. By default all columns of the input are compared. Restrict this with exactly one of:

* `historizeBlacklist` - compare all columns except the listed ones. Useful for technical columns which change on every load, e.g. a load timestamp.
* `historizeWhitelist` - compare only the listed ones.

```
actions {
  historize-airports {
    type = HistorizeAction
    inputId = stg-airports
    outputId = int-airports
    historizeBlacklist = [dl_ts_load]
  }
}
```

Note that blacklisted columns are still written to the output, they are only ignored when deciding whether something changed.

## Incremental historization

Since SDLB 3.0 incremental historization is the default and full historization is removed.
Instead of rewriting the whole output table, only changed records are written using a merge statement. It still needs to join new data with existing data to detect changes, but the `dl_hash` column keeps the amount of data to read and transfer small:
only the primary key, the validity columns and the hash of the current versions are read.

Reading the existing data can be limited further with `mergeModeAdditionalJoinPredicate`, e.g. if it is sufficient to consider the last 7 days.
Use the table alias `existing` to reference columns of the existing table data:

```
    mergeModeAdditionalJoinPredicate = "existing.dl_ts_captured > current_date - interval 7 days"
```

:::caution
Limiting the existing data means records outside the predicate are treated as not existing, which creates duplicate versions if such a record is delivered again. Use it only if you know the source cannot deliver older records.
:::

If you still have a table from a legacy full historization, migration happens automatically: the missing `dl_hash` column is detected and added to the existing data on the next run.

## Change Data Capture (CDC) historization

If the input already tells which records were inserted, updated and deleted, the join with the existing data can be avoided completely, which is optimal from a performance perspective.
HistorizeAction then makes **no change detection on its own** and creates a new version for every insert and update it receives.

### With SDLB's standard CDC columns

If the input delivers change events using SDLB's standard CDC columns `_change_type`, `_commit_timestamp` and `_change_ordinal`, as DebeziumCdcDataObject does, CDC historization is enabled without any further configuration:

```
actions {
  historize {
    type = HistorizeAction
    inputId = ext-debezium-airports
    outputId = int-airports
    executionMode {
      type = DataObjectStateIncrementalMode
    }
  }
}
```

HistorizeAction detects the column `_change_type` in the input schema and then:

* closes the current version of a record for change events of type `delete`,
* ignores change events of type `update_preimage`, as the value before the update is already stored in the history,
* historizes only the last change event if a batch contains several change events for the same primary key, ordered by `_change_ordinal`, or `_commit_timestamp` if the ordinal is missing,
* starts the validity of a new version at `_commit_timestamp`, e.g. the history follows the time axis of the source database rather than the time SDLB happened to run. Set `mergeModeCDCTimestampAutoDetect = false` to use the run's reference timestamp instead,
* does not write the CDC metadata columns to the output table.

Set `mergeModeCDCAutoDetect = false` to switch this behaviour off and treat the CDC columns as ordinary data columns.

:::info
Intermediate states of a record within the same batch are not historized: if a record is changed twice before SDLB reads the events, only the last state becomes a version. Read more often to get a finer grained history.
:::

### With a custom CDC column

If your source provides its own CDC information, name the column and the value marking a deleted record:

```
actions {
  historize-airports {
    type = HistorizeAction
    inputId = stg-airports-cdc
    outputId = int-airports
    mergeModeCDCColumn = operation
    mergeModeCDCDeletedValue = "D"
  }
}
```

Both parameters must be set together. `historizeWhitelist` cannot be combined with CDC historization, as no columns are compared.

## Following the time axis of the source system

By default the validity of a new version starts at the reference timestamp of the run, so the history reflects the schedule of the pipeline rather than the source system.

Following the time axis of the source system is not limited to change data. If a regular input has a column with the timestamp of the last change of a record, e.g. `last_updated`, set `sourceTimestampColumn = last_updated` on HistorizeAction to start the validity of new versions at that timestamp:

```
actions {
  historize-airports {
    type = HistorizeAction
    inputId = stg-airports
    outputId = int-airports
    sourceTimestampColumn = last_updated
  }
}
```

* The column must exist in the input and be of type timestamp.
* In contrast to CDC there is no auto detection for this: the column must be configured explicitly.
* It is not historized itself, e.g. it is excluded from change detection and not written to the output table. Copy it to a column with another name in a transformer if you want to keep it.
* Records arriving late, e.g. having a source timestamp older than the version they replace, are delayed to the next tick on the time axis after that version was captured, in order to avoid negative validity intervals.
* Records deleted in the source have no source timestamp, so their version is closed relative to the run's reference timestamp.

## Transformations and execution modes

`transformers` are applied **before** the historization, so they see the input data without the technical columns, see [Transformations](../transformations).
Any [ExecutionMode](../executionModes) can be combined with HistorizeAction; `DataObjectStateIncrementalMode` is a natural fit, as only new or changed records need to be read from the source.

The DataFrame created by HistorizeAction is not passed on to the next Action, it is recreated from the output DataObject instead, because the output contains the consolidated history and not just the increment.

## Parameters

The complete list of parameters is available in the [Configuration Schema Viewer](../../../json-schema-viewer). The most relevant ones:

| Parameter | Default | Description |
| --------- | ------- | ----------- |
| `inputId` | - | Input DataObject. |
| `outputId` | - | Output DataObject, a transactional table with defined primary key supporting merge. |
| `transformers` | `[]` | Transformations to apply before historization, in the order of the list. |
| `timeAxisUnit` | `1ms` | Time between ticks on the time axis. Set to `0` for half-open intervals. |
| `historizeBlacklist` | - | Columns to ignore when comparing two records. Cannot be combined with `historizeWhitelist`. |
| `historizeWhitelist` | - | Final list of columns to use when comparing two records. Cannot be combined with `historizeBlacklist`. |
| `checkInputUnique` | `false` | Fail if the input contains duplicate primary keys, instead of silently dropping duplicates. |
| `sourceTimestampColumn` | - | Column holding the timestamp of the last change in the source system, used as valid-from of new versions. |
| `mergeModeAdditionalJoinPredicate` | - | Condition to limit the existing data read for the merge, using table alias `existing`. |
| `mergeModeCDCColumn` | - | Column holding the CDC operation to replay, enabling CDC historization. |
| `mergeModeCDCDeletedValue` | - | Value of `mergeModeCDCColumn` marking a record as deleted. Defaults to `delete` for SDLB's standard CDC columns. |
| `mergeModeCDCAutoDetect` | `true` | Enable CDC historization automatically if the input has a column `_change_type`. |
| `mergeModeCDCTimestampAutoDetect` | `true` | Use `_commit_timestamp` as `sourceTimestampColumn` for SDLB's standard CDC columns. |
| `ignoreOldDeletedColumns` | `false` | Remove no longer existing columns on schema evolution. |
| `ignoreOldDeletedNestedColumns` | `true` | Remove no longer existing columns from nested data types on schema evolution. |

## See also

* [Actions overview](../actions.md)
* [DeduplicateAction](deduplicateAction.md) to keep only the latest version of a record
* [Getting Started: Historical data](../../getting-started/part-2/historical-data.md) for a hands-on walkthrough
* [Change Data Capture (CDC) with SDLB](/blog/sdl-debezium) for the CDC end-to-end story
* [Schema Evolution](../schema.md)
