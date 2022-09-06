---
id: dataObjects
title: Data Objects
---

DataObjects are the core element of Smart data Lake Builder. These objects define data entities and how they can be accessed by properties including location, type and others.

Furthermore, a section with metadata can be used. 

For a list of features please see the [Schema Viewer](http://smartdatalake.ch/json-schema-viewer/index.html).

## Object Types
Smart Data Lake Builder supports beside a list of file types, database objects and general connectors. 

Examples: 

* file based: CSV, JSON, XML, Parquet, ...
* database based: JDBC, DeltaLake, ...
* connectors: Airbyte, sFTP

## Schema
SDLB required a schema for data object that can create DataFrames, if they are used as starting point of a DAG. The schema can be
- specified manually with an option in the dataObject
- specified by adding a sample file or
- inferred from data (sampling).
The latter enables fast development, but should be avoided in production. 
By default SDLB will verify that new data fits the existing schema, otherwise an error will be thrown. If desired schema evolution can be enabled using **allowSchemaEvolution** on several DataObjects, e.g. JdbcTableDataObject and DeltaLakeTableObject. Then, old rows will get null in new columns and new rows get null in old (not existing anymore) columns. 



<!-- TODO

## Partitions
including sparkRepartition

## HousekeepingMode
-->
