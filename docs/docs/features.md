---
id: features
title: Features
---

The following list gives an overview of the features of Smart Data Lake Builder (SDLB) 3.x.
SDLB is actively developed, see the [release notes](https://github.com/smart-data-lake/smart-data-lake/releases)
for what is new, and [Upgrading to SDLB 3.x](upgrade-3) if you come from SDLB 2.x.

## Declarative approach, file based metadata
* Easy to version with a VCS for DevOps
* Flexible structure by splitting over multiple files and subdirectories
* Easy to generate from third party metadata (e.g. source system table catalog) to automate transformation of large number of DataObjects
* Support to handle multiple environments
* [Configuration Schema Viewer](/json-schema-viewer) with descriptions and HOCON examples for every configuration type and attribute

## Support for [complex workflows](reference/dag) & [streaming](reference/streaming)
* Fork, join, parallel execution, multiple start- & end-nodes possible
* Execution state tracking and [Recovery of failed runs](reference/runState)
* Switch a workflow between batch or streaming execution by using just a command line switch

## [Multi-Engine](reference/executionEngines)
Choose the engine per Action through an engine connection, and combine engines in the same data pipeline:
* Spark (DataFrames), Spark session inside the SDLB process
* Spark Connect (DataFrames) on a remote Spark Connect server, without a Spark session in the SDLB process
* Snowflake Snowpark (DataFrames)
* SQL (ELT): transformations are rendered as one SQL statement by [SQLGlot](https://github.com/tobymao/sqlglot) and executed in the database (Postgres, Snowflake, SQL Server, Oracle, Databricks, DuckDB, ...)
* Plain Scala: lightweight engine without Spark, e.g. for unit tests and small pipelines
* File (Input&OutputStream)
* Parameter: coordinate work outside SDLB, e.g. scripts and ML training runs

## Connectivity
* Spark: diverse connectors (HadoopFS, DeltaLake, Iceberg, JDBC, Kafka, Snowflake, BigQuery, Webservice) and formats (CSV, JSON, XML, Avro, Parquet, Excel, Access …)
* Change data capture from databases with [Debezium](/blog/sdl-debezium), historized without further configuration
* File: SFTP, Local, Webservice
* Support for getting secrets from different secret providers
* Support for SQL update & merge (Jdbc, DeltaLake, Iceberg)
* Database views and materialized views (`JdbcViewDataObject`), keeping grants and replaced only if their query changed
* Support for integration of [Airbyte sources](https://docs.airbyte.com/category/sources)
* Easy to extend by implementing predefined Scala traits, see [Extending SDLB](reference/extending)

## Generic [Actions](reference/actions)
* DataFrame based: Copy, [Historization](reference/actions/historizeAction) (SCD2, using merge statement), [Upsert](reference/actions/upsertAction) (SCD1, using merge statement)
* Historization and upsert following the time axis of the source system (`sourceTimestampColumn`), incl. handling of late arriving records
* Historization of CDC data (Debezium, Delta Lake change data feed) out of the box
* File based: FileTransfer, CustomFileAction with file transformations
* Script based: CustomScriptAction
* Machine learning: train and apply models tracked by [MLflow](reference/actions/mlflow)
* Easy to extend by implementing predefined Scala traits

## Customizable [Transformations](reference/transformations)
* DataFrame Transformations:
    * Chain predefined standard transformations (e.g. filter, row level data validation and more) and custom transformations within the same action
    * Custom Transformation Languages: SQL, Scala (Class, compile from config, notebook), Python
    * Flexible transform method signatures: typed Datasets, options and primitive parameters are mapped automatically
    * SQL transformations can be written in Spark SQL and executed on another database by the SQL engine
    * Many input DataFrames to many outputs DataFrames (but only one output recommended normally, in order to define dependencies as detailed as possible for the lineage)
    * Use runtime information of previous Actions (metrics, partition values, state) in options and expressions
    * Add metadata to each transformation to explain your data pipeline.
* File Transformations:
    * Language: Scala (Class or compile from config)
    * One to one, or one input file to many output files, with mapping of partition values

## Early Validation
Execution in 3 phases before execution
* Load Config: validate configuration
* Prepare: validate connections
* Init: validate DataFrame lineage of all engines (missing columns in transformations of later actions will stop the execution)

Errors in constraint and expectation definitions name the failing definition.
See [execution phases](reference/executionPhases) for details.

## [Execution Modes](reference/executionModes)
Select data to process, e.g.
* Process all data
* Partition parameters: give partition values to process for start nodes as parameter
* Partition Diff: search missing partitions and use as parameter
* Incremental: use stateful input DataObject, or compare sortable column between source and target and load the difference
* File incremental move: process new files and archive or delete them afterwards
* Column filters, optionally propagated to the following Actions
* Spark Streaming: asynchronous incremental processing by using Spark Structured Streaming
* Spark Streaming Once: synchronous incremental processing by using Spark Structured Streaming with Trigger=Once mode
* Implement your own execution mode

## [Schema Management](reference/schema)
* Automatic evolution of data schemas (new column, removed column, changed datatype widened without loss of data)
* Support for changes in complex datatypes (e.g. new column in array of struct)
* Automatic adaption of DataObjects with fixed schema (Jdbc, DeltaLake, Iceberg, SQL engine)
* Manage tables at deploy time with `CatalogSchemaUpdater`: create missing tables, evolve schemas, apply comments, primary and foreign keys, deploy views - with a plan mode reporting the changes first
* Export schemas of all DataObjects with a dry-run, to develop without access to data

## Metrics
* Number of rows read/written per DataObject
* Execution duration per Action
* Arbitrary custom metrics defined by aggregation expressions
* Predefined metric for transfer rate, completness and ensuring unique constraints.
* Result of every expectation (ok/warn/error) in the metrics log
* Run summary including the Actions of previous attempts
* StateListener interface to get notified about progress & metrics

## Data Catalog
* Report all DataObjects attributes (incl. primary and foreign keys if defined) for visualisation of data catalog in BI tool
* Metadata support for categorizing Actions and DataObjects
* Custom metadata attributes
* Column descriptions from Markdown files, `schemaMin` or the ScalaDoc of case classes, applied as column comments to the catalog

## [Lineage](reference/columnLineage)
* Browse lineage of DataObjects and Actions in the UI
* Column level lineage for Spark, plain-Scala and the SQL engine, exported in the [OpenLineage](https://openlineage.io/) column lineage format with a dry-run
* Trace a column through the data pipeline in the UI

## [Data Quality](reference/dataQuality)
* Metadata support for primary & foreign keys
* Check & report primary key violations by executing primary key checker action
* Define and validate row-level Constraints before writing DataObject
* Define and evaluate Expectations when writing DataObject, trigger warning or error, collect result as custom metric
* Future: Report data quality (foreign key matching & expectations) by executing data quality reporter action

## [Testing](reference/testing)
* Support for CI
    * Config validation
    * Custom transformation unit tests
    * Data pipeline simulation (acceptance tests)
* Support for Deployment
    * Dry-run, optionally exporting schemas and column lineage

## Performance
* Execute multiple Spark jobs in parallel within the same Spark Session to save resources
* Explicit caching of input and output DataFrames (`cacheInput`, `cacheOutput`), released automatically at the end of the run
* Push down transformations into the database with the SQL engine

## [Housekeeping](/blog/sdl-housekeeping)
* Delete, or archive & compact partitions according to configurable expressions, for file, DeltaLake, Iceberg (deletion only) and Jdbc DataObjects
* Extend with custom housekeeping logic

## [Agents](reference/agents) (experimental)
* Execute Actions on a remote SDLB instance, e.g. on-premise, controlled by a main instance in the cloud

## [User Interface](/blog/sdl-uidemo)
* Configuration viewer with catalog, search and lineage view, grouped by feed, subject area and layer
* Column lineage and entity relationship diagram of DataObjects and their keys
* Comprehensive workflow visualization: run graph with action states and metrics, timeline, partition values processed
* Documentation from metadata and code approach - all configuration elements can be described in the metadata, and are enriched with documentation from code where possible.
* Runs locally (SQLite & filesystem) or as cloud service

see also [UI Demo](https://ui-demo.smartdatalake.ch/) visualizing [Getting Started](getting-started/setup) data pipeline.
