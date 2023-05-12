---
id: dataObjects
title: Data Objects
---

DataObjects are the core element of Smart data Lake Builder. These objects define data entities and how they can be accessed by properties including location, type and others.

Furthermore, a section with metadata can be used. 

All available DataObjects and their parameters are listed in the [Schema Viewer](http://smartdatalake.ch/json-schema-viewer/index.html).

## Object Types
Smart Data Lake Builder supports beside a list of file types also database objects and general connectors. 

Examples: 

* file based: CSV, JSON, XML, Parquet, ...
* database based: JDBC, DeltaLake, Hive ...
* connectors: Airbyte, sFTP, Webservice, ...

If necessary, additional formats can be implemented by providing an own DataObject implementation (Java/Scala), or implementing an Airbyte connector (Python).

## Schema
SDLB requires a schema for data object that can create DataFrames, if they are used as starting point of a DAG. The schema can be
- specified manually with an option in the dataObject
- specified by adding a sample file or
- inferred from data (sampling).
The latter enables fast development, but should be avoided in production. 
By default SDLB will verify that new data fits the existing schema, otherwise an error will be thrown. If desired schema evolution can be enabled using **allowSchemaEvolution** on several DataObjects, e.g. JdbcTableDataObject and DeltaLakeTableObject. Then, old rows will get null in new columns and new rows get null in old (not existing anymore) columns. 

See also details in [Schema](schema.md)

<!-- TODO

## Partitions
including sparkRepartition

## HousekeepingMode
-->