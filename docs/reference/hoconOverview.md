---
id: hoconOverview
title: Hocon Configurations
---

## Overview
Data piplines in SmartDataLakeBuilder are defined and stored in the [Human-Optimized Config Object Notation (HOCON)](https://github.com/lightbend/config/blob/master/HOCON.md) file format, which is a superset of JSON.

Data pipelines are defined with four sections:

* [**global options**](#global-options) variables e.g. Spark settings
* [**connections**](#connections): defining the settings for external sources or targets
* [**data objects**](#data-objects): including type, format, location etc., and 
* [**actions**](#actions), which describes how to get from one dataObject to another, including transformations 

The settings are structured in a hierarchy. 

The [**Configuration Schema Viewer**](../../JsonSchemaViewer) presents all available options in each category with all the available parameters for each element. 

Furthermore, there are the following general (Hocon) features:

* [handling of secrets](hoconSecrets)
* [handling of variables](hoconVariables)

The pipeline definition can be separated in multiple files and directories, specified with the SDLB option `-c, --config <file1>[,<file2>...]`. This could also be a list of directories or a mixture of directories and files. All configuration files (`*.conf`) within the specified directories and its subdirectories are taken into account. 

> Note: Also `.json` is an accepted file extension. Files with other extensions are disregarded. 

## Global Options
Options listed in the **global** section are used by all executions. These include Spark options, UDFs, [secretProviders](hoconSecrets) and more.

As an example:
```
global {
  spark-options {
    "spark.sql.shuffle.partitions" = 2
    "spark.databricks.delta.snapshotPartitions" = 2
  }
  synchronousStreamingTriggerIntervalSec = 2
}
```

## Connections
Some Data Objects need a connection, e.g. JdbcTableDataObject, as they need to know how to connect to a source or target, e.g. a database.
Instead of defining the connection information for every data object, you can conveniently define it in one place and just use the reference in the data objects.
The possible parameters depend on the connection type. Please note the page [hocon secrets](hoconSecrets)  on usernames and password handling.

As an example:

```
connections {
  MyTestSql {
    type = JdbcTableConnection
    url = "jdbc:sqlserver://mssqlserver:1433;encrypt=true;trustServerCertificate=true;database=testdb"
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    authMode {
      type = BasicAuthMode
      userVariable = "ENV#MSSQLUSER"
      passwordVariable = "ENV#MSSQLPW"
    }
  }
}
```

Here the connection to an MS SQL server is defined using JDBC protocol. Besides driver and location, the authentication is handled. 

All available connections and available parameters are listed in the [Configuration Schema Viewer](https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=1).

## Data Objects
Data objects are connectors, specifying the type and location of data. SDLB supports various formats, including: 

* file formats including CSV, XML, JSON, parquet and more
* databases like SQL via JDBC, DeltaLakeTables, HiveTables, AccessTables and more
* Airbyte connectors to a long list of services

All available data objects and available parameters are listed in the [Configuration Schema Viewer](https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=2).


Within the SDLB the data objects are organized in a hierarchical manner, as many attributes are shared between them. 
For example, `HiveTables` and `TransactionalSparkTables` share common attributes modeled in the generic `TableDataObject`, as shown in the following overview: 
![data object hierarchy](../images/dataobject_hierarchy.png)
(Note: this list of DataObjects is **not** exhaustive).

If necessary, additional formats could be implemented based on these generic dataObjects.

## Actions
As the name suggest, these objects specify the process from one dataObject to another dataObject. These could also include multiple sources or targets, depending on the action type. 
The basic action is `CopyAction` where data is transferred from one dataObject into another dataObject, by default without any additional operation. On top of the copy, an arbitrary transformation (see below) can be added as well as additional settings like `executionMode`. 

Furthermore, there are actions which provide elaborated features, e.g. historization, or deduplication. 

For a list of all available actions, please consult the [Configuration Schema Viewer](https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=3).

### Transformer
Transformations can be described in SQL, Scala or Python, directly in the configuration file (see example below) or in a separate source code file. Furthermore, the action could also consist of a list of transformations (see `transformers`). 



## Example

```
global {
  spark-options {
    "spark.sql.shuffle.partitions" = 2
    "spark.databricks.delta.snapshotPartitions" = 2
  }
}

dataObjects {

  ext-airports {
    type = WebserviceFileDataObject
    url = "https://ourairports.com/data/airports.csv"
    followRedirects = true
    readTimeoutMs=200000
  }

  stg-airports {
    type = CsvFileDataObject
    path = "~{id}"
  }

  int-airports {
    type = DeltaLakeTableDataObject
    path = "~{id}"
    table {
      db = "default"
      name = "int_airports"
      primaryKey = [ident]
    }
  }
}

actions {
  download-airports {
    type = FileTransferAction
    inputId = ext-airports
    outputId = stg-airports
    metadata {
      feed = download
    }
  }

  historize-airports {
    type = HistorizeAction
    inputId = stg-airports
    outputId = int-airports
    transformers = [{
      type = SQLDfTransformer
      code = "select ident, name, latitude_deg, longitude_deg from stg_airports"
    }]
    metadata {
      feed = compute
    }
  }
}
```



