---
id: hoconOverview
title: Hocon Configurations
---

## Overview
Data piplines in SmartDataLakeBuilder are configured in the [Human-Optimized Config Object Notation (HOCON)](https://github.com/lightbend/config/blob/master/HOCON.md) file format, which is a superset of JSON. Beside being less picky about syntax (semicolons, quotation marks) it supports advanced features as substitutions, merging config from different files and inheritance.

Data pipelines are defined with four sections:

* [**global options**](#global-options) variables e.g. Spark settings
* [**connections**](#connections): defining the settings for external sources or targets
* [**data objects**](#data-objects): including type, format, location etc., and 
* [**actions**](#actions), which describes how to get from one DataObject to another, including transformations 

The settings are structured in a hierarchy. 

The [**Configuration Schema Viewer**](../../json-schema-viewer) presents all available options in each category with all the available parameters for each element. 

Furthermore, there are the following general (Hocon) features:

* [handling of secrets](hoconSecrets)
* [handling of variables](hoconVariables)

The pipeline definition can be separated in multiple files and directories, specified with the SDLB option `-c, --config <file1>[,<file2>...]`. This could also be a list of directories or a mixture of directories and files. All configuration files (`*.conf`) within the specified directories and its subdirectories are taken into account. 

> Note: Also `.properties` and `.json` are accepted file extensions. Files with other extensions are disregarded. 

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
DataObjects are the core element of Smart data Lake Builder. This could be a file type, a database object, a general connector, or a custom defined type. It includes properties about type, location, etc.

See [Data Objects](dataObjects.md)

## Actions
Actions describe the dependencies between two or more data objects and may include one or more transformer.

See [Actions](actions.md)

## Metadata
The SDLB configuration, described in these Hocon files, includes all information about the data objects, connections between them, and transformations. It can be considered as a data catalog. Thus, we suggest to include sufficient Metadata in there, this may include beside a **description** also, *layer*, *tags*, etc. 

A Metadata section is available in all elements of all categories beside of global. See example in the `stg-airports` below. 
Best practice would be to add Metadata to all elements to ensure good documentation for further usage. 

### Description files
Alternatively, the description can be provided in a Markdown files. The `.md` files must be located in the SDLB directory `description/<elementType>/<elementName>.md`,  whereas *elementType* can be actions, dataObjects or connections. 
See [SDLB Viewer](https://github.com/smart-data-lake/sdl-visualization) for more details. 

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
    metadata {
      name = "Staging file of Airport location data"
      description = "contains beside GPS coordiantes, elevation, continent, country, region"
      layer = "staging"
      subjectArea = "airports"
      tags = ["aviation", "airport", "location"]
    }
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



