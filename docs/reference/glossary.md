---
id: glossary
title: Glossary
---

:::warning
This page is under review and currently not visible in the menu.
:::

Term|Description
---|---
Action|Defines how data is moved / transformed from one data object to another. Please refer to the [API](https://smartdatalake.ch/docs/site/scaladocs/io/smartdatalake/workflow/action/index.html) to get an overview. It is possible and often necessary to define custom transformations which can be done by a SQL statement or Scala Code.
DAG|Directed Acyclic Graph: Internally, Smart Data Lake Builder builds a DAG with data objects as vertices and actions as edges to resolve the dependencies between the actions. With this, execution can be optimized as it knows which feeds can run in parallel and what the pre-conditions for each feed are.
    DataObject|Represents a physical data object like a single file (i.e. CSV, Excel, JSON), a directory containing multiple files, a database table (in Hive, JDBC etc) or any other data source or target. A list can be found in the [API docs](https://smartdatalake.ch/docs/site/scaladocs/io/smartdatalake/workflow/dataobject/DataObject.html).
Feed|Multiple actions can be combined in a feed by metadata. This greatly helps to group common actions together and makes selection of the actions to execute easier.
Global Options|The settings in the global section of the configuration file are available globally, meaning they will be used for all execution. This is mainly used for spark options.
