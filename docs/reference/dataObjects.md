---
id: dataObjects
title: Data Objects
---

:::warning
This page is under review and currently not visible in the menu.
:::

DataObjects are the core element of the smart data lake. These objects define the properties, where and how the data is stored. 

For a list of features please see the [Schema Viewer](http://smartdatalake.ch/json-schema-viewer/index.html).

## Object Types
Smart Data Lake Builder supports beside a list of file types, database objects and general connectors. 

Examples: 

* file based: CSV, JSON, XML, Parquet, ...
* database based: JDBC, DeltaLake, ...
* connectors: Airbyte, sFTP

## Schema
SDLB required a schema for each data object. This can be specified manually, with an option in the dataObject, delivered by the connector or infered from data (sampling). The latter enables fast development, but should be avoided in production. 
By default SDLB will verify that new data have fit to the existing schema, otherwise an error will be thrown. If wanted schema evolution can be enabled using **allowSchemaEvolution**. Then, old row will get null in new columns and new rows get null in old (not existing anymore) columns. 



<!-- TODO

## Partitions
including sparkRepartition

## HousekeepingMode
-->
