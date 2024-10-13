---
id: features
title: Features
---

Smart Data Lake Builder is still under heavy development so new features are added all the time.
The following list will give you a rough overview of current and planned features.
More details on the roadmap will follow shortly.

## Declarative approach, file based metadata
* Easy to version with a VCS for DevOps
* Flexible structure by splitting over multiple files and subdirectories
* Easy to generate from third party metadata (e.g. source system table catalog) to automate transformation of large number of DataObjects
* Support to handle multiple environments

## Support for [complex workflows](reference/dag) & [streaming](reference/streaming)
* Fork, join, parallel execution, multiple start- & end-nodes possible
* Recovery of failed runs
* Switch a workflow between batch or streaming execution by using just a command line switch

## Multi-Engine
* Spark (DataFrames)
* Snowflake (DataFrames)
* File (Input&OutputStream)
* Future: SQL, Kafka Streams, Flink, …

## Connectivity
* Spark: diverse connectors (HadoopFS, Hive, DeltaLake, JDBC, Kafka, Splunk, Webservice, JMS) and formats (CSV, JSON, XML, Avro, Parquet, Excel, Access …)
* File: SFTP, Local, Webservice
* Easy to extend by implementing predefined scala traits
* Support for getting secrets from different secret providers
* Support for SQL update & merge (Jdbc, DeltaLake)
* Support for integration of [Airbyte sources](https://docs.airbyte.com/category/sources)

## Generic Transformations
* Spark based: Copy, Historization, Deduplication (incl. incremental update/merge mode for streaming)
* File based: FileTransfer
* Easy to extend by implementing predefined scala traits
* Future: applying MLFlow machine learning models

## Customizable [Transformations](reference/transformations)
* Spark Transformations:
    * Chain predefined standard transformations (e.g. filter, row level data validation and more) and custom transformations within the same action
    * Custom Transformation Languages: SQL, Scala (Class, compile from config), Python
    * Many input DataFrames to many outputs DataFrames (but only one output recommended normally, in order to define dependencies as detailed as possible for the lineage)
    * Add metadata to each transformation to explain your data pipeline.
* File Transformations:
    * Language: Scala
    * Only one to one (one InputStream to one OutputStream)

## Early Validation
Execution in 3 phases before execution
* Load Config: validate configuration
* Prepare: validate connections
* Init: validate Spark DataFrame Lineage (missing columns in transformations of later actions will stop the execution)

see [execution phases](reference/executionPhases) for details

## [Execution Modes](reference/executionModes)
Select data to process, e.g.
* Process all data
* Partition parameters: give partition values to process for start nodes as parameter
* Partition Diff: search missing partitions and use as parameter
* Incremental: use stateful input DataObject, or compare sortable column between source and target and load the difference
* Spark Streaming: asynchronous incremental processing by using Spark Structured Streaming
* Spark Streaming Once: synchronous incremental processing by using Spark Structured Streaming with Trigger=Once mode

## [Schema Evolution](reference/schema#schema-evolution)
* Automatic evolution of data schemas (new column, removed column, changed datatype)
* Support for changes in complex datatypes (e.g. new column in array of struct)
* Automatic adaption of DataObjects with fixed schema (Jdbc, DeltaLake)

## Metrics
* Number of rows read/written per DataObject
* Execution duration per Action
* Arbitrary custom metrics defined by aggregation expressions
* Predefined metric for transfer rate, completness and ensuring unique constraints.
* StateListener interface to get notified about progress & metrics

## Data Catalog
* Report all DataObjects attributes (incl. foreign keys if defined) for visualisation of data catalog in BI tool
* Metadata support for categorizing Actions and DataObjects
* Custom metadata attributes

## Lineage
* Browse lineage of DataObjects and Actions in the UI
* 

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
    * Spark data pipeline simulation (acceptance tests)
* Support for Deployment
    * Dry-run

## Spark Performance
* Execute multiple Spark jobs in parallel within the same Spark Session to save resources
* Automatically cache and release intermediate results (DataFrames)

## [Housekeeping](/blog/sdl-housekeeping)
* Delete, or archive & compact partitions according to configurable expressions
* Extend with custom housekeeping logic

## [User Interface](/blog/sdl-uidemo)
* Configuration viewer with catalog and lineage view
* Comprehensive workflow visualization
* Documentation from metadata and code approach - all configuration elements can be described in the metadata, and are enriched with documentation from code where possible.

see also [UI Demo](https://ui-demo.smartdatalake.ch/) visualizing [Getting Started](getting-started/setup) data pipeline.

