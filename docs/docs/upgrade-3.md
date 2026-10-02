---
id: upgrade-3
title: Upgrading to SDLB 3.x
---

This guide lists everything you need to check when moving a project from SDLB 2.x to 3.x.
SDLB 3.x is a major release: besides the move to Apache Spark 4, the Spark support was split out of
`sdl-core`, deprecated features were removed and some defaults changed.
Most projects need changes in their Maven build, their `global` configuration section and possibly in custom code.

Work through the sections in order. The [checklist](#checklist) at the end summarizes them.

## 1. Runtime requirements

| | SDLB 2.x | SDLB 3.x |
|---|---|---|
| Java | 8+ | **17+** |
| Scala | 2.12 / 2.13 | **2.13 only** (artifacts `_2.13`) |
| Apache Spark | 3.x | **4.1** |
| Hadoop | 3.3 | 3.4 |

See [Architecture](architecture#versions-and-supported-configuration) for the exact library versions.
The target platform must provide Spark 4.x as well, e.g. a Databricks runtime based on Spark 4.
Spark on Java 17 needs `--add-opens=java.base/...` JVM options when SDLB creates the Spark session itself,
see [Build](reference/build#build-an-sdl-container).

## 2. Maven dependencies

* Change the `sdl-parent` version to `3.x` and all SDLB artifacts to the `_2.13` suffix.
  The `scala-2.12` / `scala-2.13` build profiles do not exist anymore.
* **Add `sdl-spark`** if you use any Spark DataObject, Action or transformer. `sdl-core` no longer contains
  Spark code. The connector modules (`sdl-deltalake`, `sdl-iceberg`, `sdl-kafka`, `sdl-snowflake`, ...) depend on it.
* The modules **`sdl-splunk` and `sdl-jms` were removed**. If you need `SplunkDataObject` or `JmsDataObject`,
  copy their source code from the 2.x branch into your project.
* Optional new modules: `sdl-sparkconnect` (Spark Connect engine) and `sdl-sql` (SQL/ELT engine, needs Python),
  see [Execution Engines](reference/executionEngines).

:::caution
`sdl-sparkconnect` cannot be combined with `sdl-spark` in the same JVM, as Spark classic and Spark Connect
client libraries conflict.
:::

## 3. Starting SDLB

* `LocalSmartDataLakeBuilder` and `SparkSmartDataLakeBuilder` were removed.
  Use `io.smartdatalake.app.DefaultSmartDataLakeBuilder` as main class everywhere, e.g. also in Databricks jobs.
* The command line options `--master` and `--deploy-mode` were removed. Set `master` and `deployMode` on the
  engine connection instead (next section).

## 4. Spark session configuration: from `global` to an engine connection

The Spark session is no longer configured in the `global` section, but by an *engine connection*.
DataFrame Actions use the connection with id `default-engine` unless they set `engineConnectionId`.
Without such a connection, DataFrame Actions fail with `default-engine not found in instance registry`.

| SDLB 2.x | SDLB 3.x |
|---|---|
| `global.sparkOptions` | `connections.default-engine.sparkOptions` |
| `global.enableHive` (default **true**) | `connections.default-engine.enableHive` (default **false**) |
| `global.sparkUDFs` / `global.pythonUDFs` | `connections.default-engine.sparkUDFs` / `pythonUDFs` |
| `global.kryoClasses` | `connections.default-engine.kryoClasses` |
| `--master` / `--deploy-mode` | `connections.default-engine.master` / `deployMode` |
| Hadoop options inside `sparkOptions` (`spark.hadoop.*`) | still possible, or the new `global.hadoopOptions` |

```hocon
connections {
  default-engine {
    type = SparkClassicConnection
    # leave master unset to use the session of the environment, e.g. Databricks
    master = "local[*]"
    enableHive = true   # only if you really use a Hive metastore
    sparkOptions {
      "spark.sql.shuffle.partitions" = "8"
    }
  }
}
```

SDLB now stops a Spark session it created itself at the end of the run. A session provided by the environment
is not stopped.

## 5. Configuration changes

### Renamed (old name still works, but is deprecated)

| SDLB 2.x | SDLB 3.x |
|---|---|
| `DeduplicateAction` | `UpsertAction` – it implements a slowly changing dimension type 1 |
| `JdbcTableConnection` | `JdbcConnection` |

### Renamed or replaced (old name fails)

| SDLB 2.x | SDLB 3.x |
|---|---|
| `<Action>.persist` | `cacheInput` |
| `<Action>.breakDataFrameLineage`, `breakDataFrameOutputLineage` | removed – the DataFrame is no longer passed on by default; `cacheOutput = true` passes it on, see [behaviour changes](#6-behaviour-changes) |
| `<Action>.transformer` (single transformer) | `transformers = [...]` |
| `foreignKeys = [{db = ..., table = ..., columns = ...}]` | `foreignKeys = [{dataObjectId = ..., columns = ...}]` – references the DataObject owning the table |
| `CustomFileAction.transformer = {className = ...}` | `transformer = {type = ScalaClassFileTransformer, className = ...}` (or `ScalaCodeFileTransformer`) |
| `ScalaClassSparkDsTransformer`, `ScalaClassSparkDsNTo1Transformer` | `ScalaClassSparkDfTransformer` / `ScalaClassSparkDfsTransformer` with typed `Dataset[...]` parameters of the [dynamic transform method](reference/transformations#dynamic-transform-methods) |
| `executionMode { type = CustomMode, ... }` | implement your own `ExecutionMode`, see [Execution Modes](reference/executionModes#implement-your-own-execution-mode) |

### Removed

| Removed | Replacement |
|---|---|
| `DeduplicateAction.mergeModeEnable`, `HistorizeAction.mergeModeEnable` | merge is always used: the output DataObject must implement `CanMergeDataFrame`, e.g. `DeltaLakeTableDataObject`, `IcebergTableDataObject`, `JdbcTableDataObject` |
| `HistorizeAction.filterClause` | filter in a transformer or use an execution mode |
| `updateColumnComments`, `syncComments` of table DataObjects | comments are applied at deploy time, see [Catalog metadata](#catalog-metadata-at-deploy-time) |
| `HiveTableDataObject`, `TickTockHiveTableDataObject`, `HiveTableConnection` | `DeltaLakeTableDataObject` or `IcebergTableDataObject` |
| `CustomDfDataObject` / `CustomDfCreator`, `CustomFileDataObject` / `CustomFileCreator` | implement your own DataObject, see [Extending SDLB](reference/extending) |
| `PartitionArchiveCompactionMode` | `PartitionArchiveMode` |
| ACL configuration (`acl`) | manage permissions outside SDLB |
| Atlas export | - |

## 6. Behaviour changes

Review these even if your configuration parses without errors.

* **DataFrames are no longer passed between Actions by default.** A subsequent Action reads its input again
  from the DataObject unless the previous Action sets `cacheOutput = true`. For an Action writing with
  `saveMode = Append` or `Merge` to an **unpartitioned** output, the subsequent Action therefore now reads the
  whole DataObject instead of only the records written in this run. Set `cacheOutput = true` on the writing
  Action to keep the 2.x behaviour. See [Execution Phases](reference/executionPhases).
* **Reference timestamp.** The reference timestamp (e.g. `dl_ts_captured` of Historize/UpsertAction) is now the
  start time of the run and stays the same when a run is recovered. It can be overridden with the SDL parameter
  `referenceTimestamp`.
* **Schema evolution keeps the wider data type.** When a column's data type changes, existing and new data are
  converted to the wider of both types instead of the new type, e.g. an existing `decimal(38,10)` column is kept.
* **Run recovery.** A run is now recovered if *any* Action did not complete, e.g. also if Actions were cancelled.
  A failed run can be accepted by moving its state file to the `succeeded` directory, see [Run State](reference/runState).
* **enableHive** defaults to false now (see above).
* **Snowpark is no longer selected automatically.** In 2.x an Action between `SnowflakeTableDataObject`s ran with
  Snowpark. In 3.x the engine is chosen by the engine connection: set `engineConnectionId` to the `SnowflakeConnection`
  of the DataObjects to keep using Snowpark. Otherwise the Action runs on the default engine, e.g. Spark with the
  Snowflake Spark connector. Actions using `ScalaClassSnowparkDf(s)Transformer` fail without it.
* **Debezium CDC columns** were renamed to their Delta Lake/Iceberg counterparts:
  `__commit_event` → `_change_type` (value `create` → `insert`), `__event_timestamp` → `_commit_timestamp`,
  plus the new `_change_ordinal`. The commit timestamp keeps its milliseconds. `HistorizeAction` detects these
  columns and historizes CDC data without further configuration (`mergeModeCDCAutoDetect`).
  Adapt transformers or downstream consumers referring to the old names.
* **XmlFileDataObject** uses Spark's built-in XML source (`format = xml`); the `spark-xml` library is not needed anymore.
* **Python:** the interpreter for Python transformations, MLflow Actions and the SQL engine can be set with the
  new SDL parameter `pythonPath` (environment variable `SDL_PYTHON_PATH`); it takes precedence over `PYSPARK_PYTHON`.
* **Expressions** referring to runtime information of previous Actions use the new `predecessorActions` attribute,
  see [Transformations](reference/transformations#runtime-information-of-previous-actions).

### Catalog metadata at deploy time

Table and column comments and primary keys are no longer written to the catalog during a run.
Instead, export the schemas with a dry-run on the development environment and apply them on the target
environment with `CatalogSchemaUpdater`:

```bash
sdlb --config config/ --feed-sel '.*' --test dry-run-with-schema-export
java -cp sdlb.jar io.smartdatalake.meta.configexporter.CatalogSchemaUpdater --config config/ --mode plan   # then --mode apply
```

`CatalogSchemaUpdater` also creates missing tables, evolves table schemas, creates primary keys and, with
`table.createAndReplaceForeignKeys = true`, foreign keys. Add it to your deployment pipeline if you relied on
SDLB writing comments or primary keys. See [Schema](reference/schema#managing-tables-in-the-catalog-at-deploy-time).

## 7. Custom code

Custom DataObjects, Actions, transformers and applications embedding SDLB need these changes:

* **Remove `override def factory: FromConfigFactory[...] = ...`** from all classes. Otherwise compilation fails
  with *"method factory overrides nothing"*. The companion object implementing `fromConfig` is still required.
* **Imports.** Many traits moved package, e.g.
  * `io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, CanHandlePartitions, CanMergeDataFrame, CanCreateIncrementalOutput, TableDataObject, TransactionalTableDataObject, Table, ExpectationValidation, SchemaValidation, UserDefinedSchema, HousekeepingMode, ...}` → `io.smartdatalake.workflow.dataobject.generic`
  * `FileRef`, `FileRefDataObject`, `CanCreateInputStream`, `CanCreateOutputStream`, `HasHadoopStandardFilestore` → `io.smartdatalake.workflow.dataobject.file`
  * `CanCreateSparkDataFrame`, `CanWriteSparkDataFrame`, `CanCreateStreamingDataFrame`, `SparkFileDataObject`, `SparkSaveMode` → `io.smartdatalake.workflow.dataobject.spark`
  * `SparkDfTransformer`, `SparkDfsTransformer`, `OptionsSparkDf(s)Transformer` → `io.smartdatalake.workflow.action.spark.transformer`
  * `CustomFileTransformer`, `TransformInfo` → `io.smartdatalake.workflow.action.generic.customlogic`
  * `DefaultExpressionData` → `io.smartdatalake.util.misc`

  An IDE "optimize imports" resolves most of them.
* **1:1 transformers** (`CustomDfTransformer`, `CustomGenericDfTransformer`, ...) implemented in Scala need the
  `override` keyword on `transform`, as it is no longer abstract. You can also declare a
  [dynamic transform method](reference/transformations#dynamic-transform-methods) with typed parameters instead.
* `CustomDsTransformer` and `CustomDsNto1Transformer` were removed: use `CustomDfTransformer` /
  `CustomDfsTransformer` with `Dataset[MyCaseClass]` parameters and return type.
* **`CustomFileTransformer`** moved to `io.smartdatalake.workflow.action.generic.customlogic` and is now a
  config-parsable transformer; implement `transform` (1:1) or `transformToFiles` (1:n).
* **Custom DataFrame Actions** implement `dataFrameInputs` / `dataFrameOutputs` instead of `inputs` / `outputs`.
  Custom 1:1 Actions based on `DataFrameOneToOneActionImpl` just delete their `inputs`/`outputs` overrides.
* `ScriptSubFeed` was renamed `ParameterSubFeed`, `CanReceiveScriptNotification` `CanReceiveParameterNotification`
  (package `io.smartdatalake.workflow.dataobject.generic`).
* `DataFrameSubFeed.filter: Option[String]` was replaced by `filters: Seq[ColumnFilter]`, and SubFeeds carry a
  schema instead of a dummy DataFrame (`isDummy`, `asDummy`, `convertToDummy` were removed).
* `SmartDataLakeBuilder.run`, `startRun` and `startSimulation*` return `RunStatistics` instead of
  `Map[RuntimeEventState, Int]`; use `RunStatistics.currentAttempt` for the previous value.
* Code using the `SparkSession` of `ActionPipelineContext` must get it from the engine connection or the
  SubFeed's DataFrame instead.
* Extension points are no longer `private[smartdatalake]`, so custom classes can live in any package.
  See [Extending SDLB](reference/extending).

## 8. Run state, metrics and exports

* **State files** of 2.x are migrated automatically when read (format version 5 → 7), so pending recoveries
  survive the upgrade. Exception: a failed run containing a `CustomScriptAction` can not be recovered across
  the upgrade – start it again. See [Run State](reference/runState#state-file-format-version).
* **Metrics log:** `FinalMetricsLogWriter` writes the new column `expectations_result`. Add it to existing
  metrics log tables.
* **DataObjectsExporter:** the `table` column contains the plain table name, the primary key moved to the new
  column `primaryKey`. Empty `partitions`/`tags` are exported as null.
* **Schema/statistics/lineage export files**: Old files are compatible. Update the SDLB UI to a version reading the new exports, e.g. column lineage information

## Checklist

1. Java 17, Spark 4.x platform, `_2.13` artifacts, `sdl-parent` 3.x.
2. Add `sdl-spark`; remove `sdl-splunk` / `sdl-jms`.
3. Main class `DefaultSmartDataLakeBuilder`; drop `--master` / `--deploy-mode`.
4. Create a `default-engine` connection; move `global.sparkOptions`, `enableHive`, `sparkUDFs`, `pythonUDFs`, `kryoClasses` there.
5. Replace removed/renamed attributes (`persist`, `breakDataFrameLineage`, `transformer`, `foreignKeys.db/table`, `mergeModeEnable`, `filterClause`, `updateColumnComments`, `syncComments`, `CustomMode`, Hive DataObjects).
6. Check pipelines with Append/Merge to unpartitioned outputs followed by another Action → `cacheOutput = true`?
7. Adapt consumers of Debezium CDC columns.
8. Snowpark Actions: set `engineConnectionId` to the `SnowflakeConnection`.
9. Add `dry-run-with-schema-export` + `CatalogSchemaUpdater` to the deployment if you need comments, primary or foreign keys in the catalog.
10. Custom code: remove `factory`, fix imports, add `override` to 1:1 `transform`, `dataFrameInputs/Outputs`, `RunStatistics`.
11. Add column `expectations_result` to the metrics log table; adapt consumers of the DataObjectsExporter.
12. Run `--test config` and `--test dry-run` on all feeds before the first real run.

For the full list of changes, see the [release notes](https://github.com/smart-data-lake/smart-data-lake/releases).
