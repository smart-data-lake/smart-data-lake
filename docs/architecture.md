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
SDLB is published as Maven artifacts on Maven Central. 
The SDLB versions are build with specific versions of Apache Spark and other libraries.
This page gives an overview of the respective versions.

### SDLB Version 1.X
SDLB version 1.X used Apache Spark 2.X. 
This branch of SDLB is no longer maintained.
To profit from the latest development, please upgrade to a more recent version of SDLB.

### SDLB Version 2.X

The following table gives an overview of dependency versions that are delivered with each major branch of SDLB.

| SDL Version | Java/Scala/Hadoop Version | Hadoop Version | Spark Engine     | Log4j  | Snowflake/Snowpark Engine | Delta Lake  | Iceberg |
|-------------|---------------------------|----------------|------------------|--------|---------------------------|-------------|---------|
| 2.7.X       | Java 8+, Scala 2.12/2.13  | 3.3.6          | 3.5.1            | 2.20.0 | 2.15.0 / 1.12.1           | 3.2.0       | 1.5.2   |
| 2.6.X       | Java 8+, Scala 2.12/2.13  | 3.3.6          | 3.4.3            | 2.20.0 | 2.12.0 / 1.9.0            | 2.4.0       | 1.3.1   |
| 2.5.X       | Java 8+, Scala 2.12       | 3.3.2          | 3.3.2            | 2.17.2 | 2.11.0 / 1.6.2            | 2.2.0       | 1.1.0   |
| 2.4.X       | Java 8+, Scala 2.12       | 3.3.1          | 3.2.2            | 1.2.17 | 2.10.0 / 1.2.0            | 2.0.0       | -       |
| 2.3.X       | Java 8+, Scala 2.12       | 3.3.1          | 3.2.2            | 1.2.17 | 2.10.0 / 1.2.0            | 2.0.0       | -       |
| 2.2.X       | Java 8+, Scala 2.12       | 3.3.1          | 3.2.1            | 1.2.17 | 2.9.2 / 0.11.0            | 1.1.0       | -       |
| 2.1.X       | Java 8+, Scala 2.12       | 2.7.4          | 3.1.1            | 1.2.17 | 2.8.4                     | 1.0.0       | -       |


It's possible to customize dependencies and make Smart Data Lake Builder work with other version combinations, but this needs manual tuning of dependencies in your own maven project.

In general, Java library versions are held as close as possible to the ones used in the corresponding Spark version.

### Release Notes

See SDBL Release Notes including breaking changes on [Github](https://github.com/smart-data-lake/smart-data-lake/releases)

## Context

<img src={require('./structurizr/diagramExports/container-001.png').default} onClick={(ev) => window.open(ev.target.src, '_blank')} />
Legend: <img width="60%" style={{verticalAlign: 'top'}} src={require('./structurizr/diagramExports/container-001-legend.png').default} onClick={(ev) => window.open(ev.target.src, '_blank')} />

## Components of an SDLB Job

<img src={require('./structurizr/diagramExports/component-001.png').default} onClick={(ev) => window.open(ev.target.src, '_blank')} />
Legend: <img width="60%" style={{verticalAlign: 'top'}} src={require('./structurizr/diagramExports/component-001-legend.png').default} onClick={(ev) => window.open(ev.target.src, '_blank')} />

## Cross cutting concerns

### Logging
By default, SDLB uses the logging libraries included in the corresponding Spark version. This is Log4j 1.2.x for Spark 2.4.x up to Spark 3.2.x.
Starting from Spark 3.3.x it will use Log4j 2.x, see [SPARK-6305](https://issues.apache.org/jira/browse/SPARK-6305).

You can customize logging dependencies manually by creating your own maven project.
