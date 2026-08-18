---
id: deduplicateAction
title: DeduplicateAction
---

DeduplicateAction copies data from one input to one output DataObject and keeps the **latest version of every record**, identified by the primary key of the output table.
In contrast to a plain [CopyAction](../actions.md), records which are no longer delivered by the source are kept, so the output always contains the complete set of records ever seen, each one in its most recent state.
Use it to build a *current state* table out of a source you can only read partially, e.g. deltas or incremental loads.

If you need the full history of every change instead of only the latest state, use [HistorizeAction](historizeAction.md).

## Requirements

* The output DataObject must be a transactional table supporting SQL merge, e.g. DeltaLakeTableDataObject, IcebergTableDataObject, JdbcTableDataObject or SparkConnectTableDataObject.
* The primary key must be defined on the output table (`table.primaryKey`). It defines what "the same record" means.
* The input data must be unique across that primary key. DeduplicateAction does **not** deduplicate the input itself, because that is an expensive operation and data is usually unique already. If it is not, add a `DeduplicateTransformer` to `transformers`, or you will get errors from the merge statement like
  `DeltaUnsupportedOperationException: [DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE]`.

## Technical columns

DeduplicateAction adds one technical column to the output:

| Column | Description |
| ------ | ----------- |
| `dl_ts_captured` | Timestamp of the last occurrence of the record in the source. |

The column name can be changed globally with the SDLB parameter `capturedColumnName`.

## Basic example

```
actions {
  dedup-airports {
    type = DeduplicateAction
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

Assume the source delivers the airport `LSZB` with an updated name in a later run, and no longer delivers `LSGG`:

| ident | name | dl_ts_captured |
| ----- | ---- | -------------- |
| LSZB | Bern Belp Airport | 2024-03-02 04:00:00 |
| LSGG | Geneva Airport | 2024-03-01 04:00:00 |

`LSZB` was updated to the value and timestamp of the second run, `LSGG` is still there with the timestamp of the first run, although the source does not deliver it anymore.

## Reducing the number of updated records

By default `dl_ts_captured` is updated on **every** execution, even if nothing changed in the source.
With a merge statement this rewrites the whole table on every run. Set `updateCapturedColumnOnlyWhenChanged = true` to update a record only if one of its columns changed:

```
actions {
  dedup-airports {
    type = DeduplicateAction
    inputId = stg-airports
    outputId = int-airports
    updateCapturedColumnOnlyWhenChanged = true
  }
}
```

`dl_ts_captured` then holds the timestamp of the last *change* of the record instead of the last time it was seen.

Reading the existing data for the merge can be limited further with `mergeModeAdditionalJoinPredicate`, e.g. if it is sufficient to consider records captured within the last 7 days.
Use the table alias `existing` to reference columns of the existing table data:

```
    mergeModeAdditionalJoinPredicate = "existing.dl_ts_captured > current_date - interval 7 days"
```

## Following the time axis of the source system

By default `dl_ts_captured` is set to the reference timestamp of the run, so it reflects the schedule of the pipeline rather than the source system.
If the input contains the timestamp of the last change of a record, e.g. `last_updated`, set `sourceTimestampColumn` to use that value instead:

```
actions {
  dedup-airports {
    type = DeduplicateAction
    inputId = stg-airports
    outputId = int-airports
    sourceTimestampColumn = last_updated
  }
}
```

* The column must exist in the data to deduplicate and be of type timestamp. Records where it is null fall back to the reference timestamp of the run.
* There is no auto detection: the column must be configured explicitly.
* The column itself is not written to the output DataObject, as its value is kept in `dl_ts_captured`. Copy it to a column with another name in a transformer if you want to keep it.
* Records arriving late, e.g. having a source timestamp older than the record already stored, are not applied, so that `dl_ts_captured` always holds the latest version according to the source system.
* `updateCapturedColumnOnlyWhenChanged` is normally not needed then, as `dl_ts_captured` is moved forward only if the source system changed the record anyway. What it changes is that existing records are updated only if the source timestamp increased, instead of comparing all columns. This avoids rewriting records which the source system delivers again with an unchanged timestamp, but a change which the source system did not timestamp is not applied.

## Transformations and execution modes

`transformers` are applied **before** the deduplication, so they see the input data without `dl_ts_captured`, see [Transformations](../transformations).
Any [ExecutionMode](../executionModes) can be combined with DeduplicateAction; `DataObjectStateIncrementalMode` is a natural fit, as only new or changed records need to be read from the source.

## Parameters

The complete list of parameters is available in the [Configuration Schema Viewer](../../../json-schema-viewer). The most relevant ones:

| Parameter | Default | Description |
| --------- | ------- | ----------- |
| `inputId` | - | Input DataObject. |
| `outputId` | - | Output DataObject, a transactional table with defined primary key supporting merge. |
| `transformers` | `[]` | Transformations to apply before deduplication, in the order of the list. |
| `updateCapturedColumnOnlyWhenChanged` | `false` | Update `dl_ts_captured` only if the record changed in the source, instead of on every execution. |
| `sourceTimestampColumn` | - | Column holding the timestamp of the last change in the source system, used as value for `dl_ts_captured`. |
| `mergeModeAdditionalJoinPredicate` | - | Condition to limit the existing data read for the merge, using table alias `existing`. |
| `ignoreOldDeletedColumns` | `false` | Remove no longer existing columns on schema evolution. |
| `ignoreOldDeletedNestedColumns` | `true` | Remove no longer existing columns from nested data types on schema evolution. |

## See also

* [Actions overview](../actions.md)
* [HistorizeAction](historizeAction.md) to keep all versions of a record instead of only the latest one
* [Getting Started: Historical data](../../getting-started/part-2/historical-data.md) for a hands-on walkthrough
* [Schema Evolution](../schema.md)
