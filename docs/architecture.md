---
id: architecture
title: Architecture
---

Smart Data Lake Builder (SDLB) is basically a Java application which is started on the [command line](reference/commandLine.md).
It can run in many environments and platforms like a Databricks cluster, Azure Synapse, Google Dataproc, and also on your local machine, see [Getting Started](getting-started/setup).

Find below an overview of requirements, versions and supported configurations.

## Basic Requirements
- Needs Java 8+ to run
- Uses Hadoop Java library to read local and remote files (S3, ADLS, HDFS, ...)
- Is programmed in Scala
- Uses Maven 3+ as build system

## Versions and supported configuration
SDLB currently maintains the following two major versions, which are published as Maven artifacts on maven central:

|SDL Version|Java/Scala/Hadoop Version|File Engine|Spark Engine|Snowflake/Snowpark Engine|Comments|
| --------- | ----------------------- | --------- | ---------- | ----------------------- | ------ |
|1.x, branch master/develop-spark2|Java 8, Scala 2.11 & 2.12|Hadoop 2.7.x|Spark 2.4.x|not supported|Delta lake has limited functionality in Spark 2.x|
|2.x, branch master/develop-spark3|Java 8+, Scala 2.12|Hadoop 3.3.x (2.7.x)|Spark 3.2.x (3.1.x)|Snowpark 1.2.x|Delta lake, spark-snowflake and spark-extensions need specific library versions matching the corresponding spark minor version|

Configurations using alternative versions mentioned in parentheses can be build manually by setting corresponding maven profiles.

It's possible to customize dependencies and make Smart Data Lake Builder work with other version combinations, but this needs manual tuning of dependencies in your own maven project.

In general, Java library versions are held as close as possible to the ones used in the corresponding Spark version.

## Release Notes

See SDBL Release Notes including breaking changes on [Github](https://github.com/smart-data-lake/smart-data-lake/releases)

## Logging
By default, SDLB uses the logging libraries included in the corresponding Spark version. This is Log4j 1.2.x for Spark 2.4.x up to Spark 3.2.x.
Starting from Spark 3.3.x it will use Log4j 2.x, see [SPARK-6305](https://issues.apache.org/jira/browse/SPARK-6305).

You can customize logging dependencies manually by creating your own maven project.
