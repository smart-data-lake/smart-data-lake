# sdl-sparkconnect

SDLB engine module implementing a `SparkConnectSubFeed` based on the [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) JVM client.
It allows running SDLB as a thin client against a remote Spark Connect server: the SDLB driver only needs
`sdl-core` and `spark-connect-client-jvm` on the classpath - no classic Spark distribution, no Hadoop.

## Key constraints

Two constraints shape the design of this module:

1. **No Hadoop FileSystem API**: A Spark Connect client can only access remote data through `spark.read`/`write`
   and the catalog. All file-based DataObjects (`HadoopFileDataObject`, `SparkFileDataObject`, DeltaLake/Iceberg
   with their path handling) are therefore out of scope. DataObjects in this module follow the pattern of
   `SnowflakeTableDataObject`/`JdbcTableDataObject`: pure Spark API plus catalog calls.
2. **Classic and Connect sessions cannot coexist in the same JVM**: This module therefore depends only on
   `sdl-core` (not `sdl-spark`), there is no `SparkSubFeed` fallback, and no `SchemaConverter`s between the two
   Spark engines are needed - a job runs either fully classic or fully connect.

Note that `spark-connect-client-jvm` is a shaded jar which embeds the shared Spark SQL API (`spark-sql-api`)
and gRPC. Classic `spark-sql`/`spark-core` must not be added to the classpath.

## Architecture

SDLB abstracts DataFrame engines behind the generic interfaces in `sdl-core`
(`io.smartdatalake.workflow.dataframe`): `GenericDataFrame`, `GenericColumn`, `GenericSchema`, `DataFrameFunctions`,
and the `DataFrameSubFeed` / `DataFrameSubFeedCompanion` pair. Each engine (sdl-spark, sdl-snowflake, this module)
provides wrapper classes around its native types plus a SubFeed type used for engine selection.

This module contributes:

| Component | File | Purpose |
|-----------|------|---------|
| `SparkConnectConnection` | `workflow/connection/SparkConnectConnection.scala` | Owns the Spark Connect session (`SparkSession.builder().remote(url)`). Implements `EngineConnection` with `subFeedType = SparkConnectSubFeed`, which drives engine selection in `DataFrameActionImpl`. `activate` tags remote operations with the current action id for traceability on the server. |
| `SparkConnectSubFeed` | `workflow/dataframe/sparkconnect/SparkConnectSubFeed.scala` | The SubFeed transporting DataFrames between Actions, plus its companion implementing `DataFrameSubFeedCompanion`/`DataFrameFunctions` by delegating to the shared `org.apache.spark.sql.functions`. `getSparkSession` resolves the session from the `SparkConnectConnection` in the context (parallel to `SparkSubFeed.getSparkSession`, which stays hard-wired to `SparkClassicConnection`). |
| Wrapper classes | `workflow/dataframe/sparkconnect/SparkConnectDataFrame.scala` | `SparkConnectDataFrame`, `SparkConnectColumn`, `SparkConnectSchema`, `SparkConnectField`, `SparkConnectDataType` (+ Simple/Struct/Array/Map), `SparkConnectRow`, `SparkConnectGroupedDataFrame`, `SparkConnectUnaryUdf` - each wrapping the corresponding shared Spark SQL API type. |
| `DeltaLakeTableSparkConnectEngine` | `workflow/dataobject/DeltaLakeTableSparkConnectEngine.scala` | Spark Connect implementation of the `DeltaLakeTableEngine` SPI of `sdl-core`, so that the engine-agnostic `DeltaLakeTableDataObject` can be used over Spark Connect. Read/write/merge, schema evolution, partition handling, delta operation metrics from `DESCRIBE HISTORY`, and incremental output (`CanCreateIncrementalOutput`) with the delta change data feed (`readChangeFeed`/`startingVersion`). Everything through SQL and the Spark API, without the DeltaTable Java API and without filesystem access. |
| `IcebergTableSparkConnectEngine` | `workflow/dataobject/IcebergTableSparkConnectEngine.scala` | Spark Connect implementation of the `IcebergTableEngine` SPI of `sdl-core`, analogous to the delta engine. Incremental output uses the Iceberg stored procedure `system.create_changelog_view`, snapshot ids are read from the `<table>.snapshots` metadata table instead of the Iceberg Java API. |
| `SparkConnectObservation` | `workflow/dataframe/sparkconnect/SparkConnectObservation.scala` | `DataFrameObservation` based on the standard Spark API `org.apache.spark.sql.Observation` and `Dataset.observe(Observation, ...)`. Needs no `QueryExecutionListener`: the observed metrics are transported back to the client with the response of the query or command executing the plan. Falls back to calculating the metrics with a separate query if the execution reports no metrics, see "Observations and metrics" below. |
| `SparkConnectTableDataObject` | `workflow/dataobject/SparkConnectTableDataObject.scala` | `TransactionalTableDataObject` over the normal Spark Table API: read via `session.read.table` (or `table.query` via `session.sql`), write via `saveAsTable` with SDLSaveMode mapping, existence checks via `session.catalog`, `dropTable`/pre-/post-SQL via `session.sql`. Outside the exec phase DataFrames are limited to 1 row for fast schema propagation. Implements `CanHandlePartitions` (write with `partitionBy`, overwrite given partitions by delete+append, dynamic partition overwrite via `insertInto`, list partitions by select distinct, delete/move partitions with SQL DELETE/UPDATE) and `CanMergeDataFrame` (SDLSaveMode.Merge implemented with the Spark native merge API `Dataset.mergeInto`, analogous to `DeltaLakeTableDataObject.mergeDataFrameByPrimaryKey`). Note that merge and delete/move partitions need a table format supporting row-level operations on the server side, e.g. `format = delta`. |

There is no registration mechanism needed: SDLB discovers SubFeed types, connections and DataObjects by
classpath reflection over the `io.smartdatalake` package (`DataFrameSubFeed.getKnownSubFeedTypes`, `ConfigParser`).

Because Spark 4 unified the client API (classic and connect both implement the shared `spark-sql-api` types
`Dataset`, `Column`, `Row`, `functions`, `types.StructType`, ...), the wrapper implementations are nearly identical
to sdl-spark. They were copied and adapted (Snowpark precedent) rather than sharing a base class, to keep sdl-spark
untouched and the module standalone.

### Engine selection

A `DataFrameActionImpl` intersects the `getSubFeedSupportedTypes`/`writeSubFeedSupportedTypes` of its input/output
DataObjects with the `subFeedType` of the engine connection (action attribute `engineConnectionId`, default
connection id `default-engine`). To run a job over Spark Connect, define a `SparkConnectConnection` with id
`default-engine` (or reference it explicitly per action).

## Example configuration

```hocon
connections {
  "default-engine" {
    type = SparkConnectConnection
    url = "sc://localhost:15002"
  }
}

dataObjects {
  srcTable {
    type = SparkConnectTableDataObject
    connectionId = "default-engine"
    table = {
      db = default
      name = my_source_table
    }
  }
  tgtTable {
    type = SparkConnectTableDataObject
    connectionId = "default-engine"
    table = {
      db = default
      name = my_target_table
    }
  }
}

actions {
  copy {
    type = CopyAction
    inputId = srcTable
    outputId = tgtTable
    metadata.feed = myFeed
  }
}
```

## Supported DataObject features

| DataObject | Features |
|---|---|
| `SparkConnectTableDataObject` | `CanHandlePartitions`, `CanMergeDataFrame`, `CanEvolveSchema`, `ExpectationValidation` (constraints and expectations) |
| `DeltaLakeTableDataObject` (sdl-core) | additionally `CanCreateIncrementalOutput` (delta change data feed) and `CanHandleConstraints` (primary key constraints) |
| `IcebergTableDataObject` (sdl-core) | additionally `CanCreateIncrementalOutput` (Iceberg changelog view) |

Expectations and constraints are engine-agnostic: they are composed of `SqlExpressionColumn`s and standard
`DataFrameFunctions`, and the metrics are observed with `SparkConnectObservation`. The validation expressions
themselves are evaluated client-side by `ScalaExpressionEvaluatorFactory` (the fallback of
`Environment.expressionEvaluatorFactory`), which does not need Spark at all.

## Observations and metrics

Expectations and the row counts of an Action are collected with `DataFrame` observations. On a Spark Connect client
there is no `QueryExecutionListener`, but the standard `org.apache.spark.sql.Observation` API works: the client
registers the observation, and the server returns the observed metrics with the response of the query or command
executing the plan - for `ExecutePlan` requests as well as for commands like a write.

However, not every execution reports metrics. The response then contains an *empty* metrics row, and
`SparkConnectObservation` falls back to calculating the metrics with a separate aggregation query on the cached
DataFrame (this is what `GenericCalculatedObservation` of sdl-core does, and what this module did before). Known cases:

- **writing with a DataSource which creates its own `QueryExecution` on the server** - this includes delta lake and
  Iceberg, i.e. `DeltaLakeTableDataObject` and `IcebergTableDataObject` always use the fallback for their output metrics.
  A plain `saveAsTable` (e.g. `SparkConnectTableDataObject` on the servers default format) does report metrics.
- **plans which are not sent to the server as such**, e.g. when the observed DataFrame is registered as a temporary
  view and referenced by a SQL statement (`SQLDfTransformer`, `SQLDfsTransformer`).
- **the observed DataFrame is not part of the executed plan at all**, e.g. an input which is not used by the
  transformation of an Action.

Because of the fallback, the DataFrame is still cached in the exec phase - `cacheInput`/`cacheOutput` remain the
explicit materialization knobs on top of that.

Metrics of the input observations are collected by the output observation (`linkWithInputObservations`) rather than
combined with `CombinedObservation`, analogous to `SparkObservation` of sdl-spark: an `Observation` only carries the
metrics of its own `CollectMetrics` node, and waiting for an input which is not part of the executed plan would block
until timeout. Input metrics get the input DataObjectId as postfix, e.g. `count#src1`, same as with the classic engine.

## Current limitations

The following features are stubbed with `NotImplementedError` or reduced functionality, following the Snowpark precedent:

- **Streaming**: streaming DataFrames, dummy streaming DataFrames and the streaming lifecycle hooks are not supported yet.
  Observations are not supported for streaming Datasets either.
- **Schema evolution of struct types**: needs custom catalyst expressions which cannot be created on the client side
  (simple type casts and array/map recursion work).
- **Write metrics of `SparkConnectTableDataObject`**: there is no `QueryExecutionListener` on the connect client, so
  `writeDataFrame` returns an empty MetricsMap, i.e. there is no `records_written`/`bytes_written`. The number of
  written rows is still reported as `count` metric from the observation.
  The delta and iceberg engines do return operation metrics (`DESCRIBE HISTORY` / snapshot summary).
- **Observed metrics are not reported for every execution**, see "Observations and metrics" above. The metrics are
  correct in any case, but for delta lake and Iceberg writes they need a second query on the cached DataFrame.
- **No filter push-down for input row counts**: the `!pushDownTolerant` marker that SDLB adds to input observation names
  is evaluated by the catalyst rule `PushPredicateThroughTolerantCollectMetricsRule`, which is injected by
  `SDLSparkExtension` of sdl-spark. That is a *server-side* session extension and normally not installed on a Spark
  Connect server, so input counts are taken above the filter instead of at the source.
- **`hash` function**: implemented as `hash(to_json(struct(...), ignoreNullFields=false))` to keep null-awareness without
  the custom `NullAwareMurmur3HashExpr` catalyst expression. Hash values are therefore *not* comparable to hash values
  written by the classic Spark engine.
- **Column introspection**: `GenericColumn.exprSql` is not available (columns are unresolved plan expressions on the
  client side). This also means that the expectations implemented in sdl-spark (`CountExpectation`,
  `AvgCountPerPartitionExpectation`) cannot be used. Use the generic `SQLExpectation`, 
  e.g. `SQLExpectation(name = "count", aggExpression = "count(*)", expectation = Some("> 0"))`.
- **Merge and partition deletion need a v2 table format**: SDLSaveMode.Merge, `deletePartitions` and `movePartitions`
  use row-level operations (MERGE/DELETE/UPDATE) which are not supported for plain parquet tables - use e.g. `format = delta`.
  Schema evolution on merge is not supported yet.

## Testing

Config parsing, reflection discovery and client-local schema operations are tested without any server.

All tests needing a server (`SparkConnectDataObjectTest`, `SparkConnectPipelineTest` and the action behaviour tests)
use `io.smartdatalake.testutils.sparkconnect.SparkConnectTestUtil`, which resolves the server once per JVM:

1. env variable `SPARK_CONNECT_URL` - externally managed server, never started/stopped by tests
2. a server already listening on `sc://localhost:15002` - used as-is
3. the module script `start-spark-connect.sh` (found in the working directory or subdirectory sdl-sparkconnect) -
   a local server with delta lake support is started, downloading a Spark distribution first if needed,
   and stopped by a JVM shutdown hook. This is the default when executing `mvn test`.
4. env variable `SPARK_HOME` - a local server is started with `sbin/start-connect-server.sh`
   (without delta lake support) and stopped by a JVM shutdown hook
5. otherwise these tests are cancelled (not failed)

So a plain `mvn test` starts the Spark Connect server automatically. Note that the first execution downloads
a Spark distribution (~400MB) into the module directory. Server availability is always checked with a fast tcp
probe first, because the gRPC client blocks with a long retry policy on an unreachable server.

The `DeduplicateActionBehaviour` and `HistorizeActionBehaviour` test suites of sdl-core are executed with
`SparkConnectTableDataObject`s in `SparkConnectDeduplicateActionTest` and `SparkConnectHistorizeActionTest`,
covering DeduplicateAction and HistorizeAction with merge mode, CDC and schema evolution over Spark Connect.

The CI snapshot build caches the Spark distribution and runs `start-spark-connect.sh` before the Maven build,
so these tests execute in CI (the tests then detect the already running server).
