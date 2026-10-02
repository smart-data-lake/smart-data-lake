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
* Support to handle multiple environments, see also [Configure for different environments](getting-started/part-2/environments), [Hocon Variables](reference/hoconVariables)
* [Configuration Schema Viewer](/json-schema-viewer) with descriptions and HOCON examples for every configuration type and attribute, see also [Schema Viewer Usage](reference/schema-viewer-navigation)

see also [Hocon Configurations](reference/hoconOverview)

## Support for complex workflows & streaming
* Fork, join, parallel execution, multiple start- & end-nodes possible
* Execution state tracking and [Recovery of failed runs](reference/runState)
* Switch a workflow between batch or streaming execution by using just a command line switch, see also [Command Line](reference/commandLine)

see also [DAG](reference/dag), [Streaming](reference/streaming)

## Multi-Engine
Choose the engine per Action through an engine connection, and combine engines in the same data pipeline:
* Spark (DataFrames), Spark session inside the SDLB process
* Spark Connect (DataFrames) on a remote Spark Connect server, without a Spark session in the SDLB process, see also [Spark Connect Engine](reference/executionEngines#spark-connect-engine)
* Snowflake Snowpark (DataFrames), see also [Snowpark blog post](/blog/sdl-snowpark)
* SQL (ELT): transformations are rendered as one SQL statement by [SQLGlot](https://github.com/tobymao/sqlglot) and executed in the database (Postgres, Snowflake, SQL Server, Oracle, Databricks, DuckDB, ...), see also [SQL Engine](reference/executionEngines#sql-engine)
* Plain Scala: lightweight engine without Spark, e.g. for unit tests and small pipelines
* File (Input&OutputStream)
* Parameter: coordinate work outside SDLB, e.g. scripts and ML training runs

see also [Execution Engines](reference/executionEngines)

## Connectivity
* Spark: diverse connectors (HadoopFS, DeltaLake, Iceberg, JDBC, Kafka, Snowflake, BigQuery, Webservice) and formats (CSV, JSON, XML, Avro, Parquet, Excel, Access …), see also [Delta Lake format](getting-started/part-2/delta-lake-format), [Custom Webservice](getting-started/part-3/custom-webservice), [Databricks blog post](/blog/sdl-databricks)
* Change data capture from databases with [Debezium](/blog/sdl-debezium), historized without further configuration
* File: SFTP, Local, Webservice
* Support for getting secrets from different secret providers, see also [Hocon Secrets](reference/hoconSecrets)
* Support for SQL update & merge (Jdbc, DeltaLake, Iceberg)
* Database views and materialized views (`JdbcViewDataObject`), keeping grants and replaced only if their query changed, see also [SQL Engine](reference/executionEngines#sql-engine)
* Support for integration of [Airbyte sources](https://docs.airbyte.com/category/sources), see also [Airbyte blog post](/blog/sdl-airbyte)
* Easy to extend by implementing predefined Scala traits, see [Extending SDLB](reference/extending)

see also [Data Objects](reference/dataObjects)

## Generic Actions
* DataFrame based: Copy, [Historization](reference/actions/historizeAction) (SCD2, using merge statement), [Upsert](reference/actions/upsertAction) (SCD1, using merge statement), see also [CustomDataFrameAction](reference/actions/customDataFrameAction), [Keeping historical data](getting-started/part-2/historical-data), [Historization blog post](/blog/sdl-hist)
* Historization and upsert following the time axis of the source system (`sourceTimestampColumn`), incl. handling of late arriving records, see also [Following the time axis of the source system](reference/actions/historizeAction#following-the-time-axis-of-the-source-system)
* Historization of CDC data (Debezium, Delta Lake change data feed) out of the box, see also [Change Data Capture (CDC) historization](reference/actions/historizeAction#change-data-capture-cdc-historization)
* File based: FileTransfer, CustomFileAction with file transformations
* Script based: CustomScriptAction
* Machine learning: train and apply models tracked by [MLflow](reference/actions/mlflow)
* Easy to extend by implementing predefined Scala traits, see also [Own Action](reference/extending#own-action)

see also [Actions](reference/actions)

## Customizable Transformations
* DataFrame Transformations:
    * Chain predefined standard transformations (e.g. filter, row level data validation and more) and custom transformations within the same action, see also [Own transformer](reference/extending#own-transformer)
    * Custom Transformation Languages: SQL, Scala (Class, compile from config, notebook), Python, see also [Use in Notebooks](reference/notebookCatalog)
    * Flexible transform method signatures: typed Datasets, options and primitive parameters are mapped automatically
    * SQL transformations can be written in Spark SQL and executed on another database by the SQL engine
    * Many input DataFrames to many outputs DataFrames (but only one output recommended normally, in order to define dependencies as detailed as possible for the lineage)
    * Use runtime information of previous Actions (metrics, partition values, state) in options and expressions
    * Add metadata to each transformation to explain your data pipeline.
* File Transformations:
    * Language: Scala (Class or compile from config)
    * One to one, or one input file to many output files, with mapping of partition values

see also [Transformations](reference/transformations)

## Early Validation
Execution in 3 phases before execution
* Load Config: validate configuration, see also [Config validation](reference/testing#config-validation)
* Prepare: validate connections
* Init: validate DataFrame lineage of all engines (missing columns in transformations of later actions will stop the execution)

Errors in constraint and expectation definitions name the failing definition.

see also [Execution Phases](reference/executionPhases)

## Execution Modes
Select data to process, e.g.
* Process all data
* Partition parameters: give partition values to process for start nodes as parameter, see also [Partitions](reference/dataObjects#partitions)
* Partition Diff: search missing partitions and use as parameter
* Incremental: use stateful input DataObject, or compare sortable column between source and target and load the difference, see also [Incremental Mode](getting-started/part-3/incremental-mode)
* File incremental move: process new files and archive or delete them afterwards
* Column filters, optionally propagated to the following Actions
* Spark Streaming: asynchronous incremental processing by using Spark Structured Streaming, see also [Streaming](reference/streaming)
* Spark Streaming Once: synchronous incremental processing by using Spark Structured Streaming with Trigger=Once mode
* Implement your own execution mode, see also [Own ExecutionMode](reference/extending#own-executionmode)

see also [Execution Modes](reference/executionModes)

## Schema Management
* Automatic evolution of data schemas (new column, removed column, changed datatype widened without loss of data), see also [Schema Evolution](reference/schema#schema-evolution)
* Support for changes in complex datatypes (e.g. new column in array of struct)
* Automatic adaption of DataObjects with fixed schema (Jdbc, DeltaLake, Iceberg, SQL engine)
* Manage tables at deploy time with `CatalogSchemaUpdater`: create missing tables, evolve schemas, apply comments, primary and foreign keys, deploy views - with a plan mode reporting the changes first, see also [Managing tables in the catalog at deploy time](reference/schema#managing-tables-in-the-catalog-at-deploy-time)
* Export schemas of all DataObjects with a dry-run, to develop without access to data, see also [Dry run](reference/testing#dry-run)

see also [Schema](reference/schema)

## Metrics
* Number of rows read/written per DataObject
* Execution duration per Action
* Arbitrary custom metrics defined by aggregation expressions, see also [Expectations](reference/dataQuality#expectations-on-dataobjects)
* Predefined metric for transfer rate, completness and ensuring unique constraints.
* Result of every expectation (ok/warn/error) in the metrics log
* Run summary including the Actions of previous attempts, see also [Run State & Recovery](reference/runState)
* StateListener interface to get notified about progress & metrics

see also [Metrics](reference/dataQuality#metrics), [metricsFailCondition](reference/actions#metricsfailcondition)

## Data Catalog
* Report all DataObjects attributes (incl. primary and foreign keys if defined) for visualisation of data catalog in BI tool
* Metadata support for categorizing Actions and DataObjects, see also [Metadata](reference/hoconOverview#metadata)
* Custom metadata attributes
* Column descriptions from Markdown files, `schemaMin` or the ScalaDoc of case classes, applied as column comments to the catalog, see also [Column descriptions from ScalaDoc](reference/schema#column-descriptions-from-scaladoc)

see also [Metadata and SDLB UI](getting-started/part-3/metadata), [Use in Notebooks](reference/notebookCatalog)

## Lineage
* Browse lineage of DataObjects and Actions in the UI
* Column level lineage for Spark, plain-Scala and the SQL engine, exported in the [OpenLineage](https://openlineage.io/) column lineage format with a dry-run
* Trace a column through the data pipeline in the UI

see also [Column Lineage](reference/columnLineage)

## Data Quality
* Metadata support for primary & foreign keys
* Check & report primary key violations by executing primary key checker action
* Define and validate row-level [Constraints](reference/dataQuality#constraints) before writing DataObject
* Define and evaluate [Expectations](reference/dataQuality#expectations-on-dataobjects) when writing DataObject, trigger warning or error, collect result as custom metric
* Future: Report data quality (foreign key matching & expectations) by executing data quality reporter action

see also [Data Quality](reference/dataQuality)

## Testing
* Support for CI
    * [Config validation](reference/testing#config-validation)
    * Custom transformation unit tests, see also [Custom transformation logic unit tests](reference/testing#custom-transformation-logic-unit-tests), [Testing your extension](reference/extending#testing-your-extension)
    * Data pipeline simulation (acceptance tests), see also [Simulation of dataframe data pipeline](reference/testing#simulation-of-dataframe-data-pipeline)
* Support for Deployment, see also [Deployment options](reference/deploymentOptions)
    * Dry-run, optionally exporting schemas and column lineage, see also [Dry run](reference/testing#dry-run)

see also [Testing](reference/testing)

## Performance
* Execute multiple Spark jobs in parallel within the same Spark Session to save resources, see also [DAG](reference/dag), [Command Line](reference/commandLine) (`--parallelism`)
* Explicit caching of input and output DataFrames (`cacheInput`, `cacheOutput`), released automatically at the end of the run, see also [CustomDataFrameAction parameters](reference/actions/customDataFrameAction#parameters)
* Push down transformations into the database with the SQL engine, see also [SQL Engine](reference/executionEngines#sql-engine)

## Housekeeping
* Delete, or archive & compact partitions according to configurable expressions, for file, DeltaLake, Iceberg (deletion only) and Jdbc DataObjects, see also [HousekeepingMode](reference/dataObjects#housekeepingmode)
* Extend with custom housekeeping logic

see also [Housekeeping](reference/housekeeping), [Housekeeping blog post](/blog/sdl-housekeeping)

## Agents (experimental)
* Execute Actions on a remote SDLB instance, e.g. on-premise, controlled by a main instance in the cloud

see also [Agents](reference/agents)

## User Interface
* Configuration viewer with catalog, search and lineage view, grouped by feed, subject area and layer
* Column lineage and entity relationship diagram of DataObjects and their keys, see also [Column Lineage](reference/columnLineage)
* Comprehensive workflow visualization: run graph with action states and metrics, timeline, partition values processed
* Documentation from metadata and code approach - all configuration elements can be described in the metadata, and are enriched with documentation from code where possible, see also [Metadata and SDLB UI](getting-started/part-3/metadata#sdlb-ui)
* Runs locally (SQLite & filesystem) or as cloud service

see also [UI blog post](/blog/sdl-uidemo), and the [UI Demo](https://ui-demo.smartdatalake.ch/) visualizing the [Getting Started](getting-started/setup) data pipeline.
