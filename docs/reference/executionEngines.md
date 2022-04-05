---
id: executionEngines
title: Execution Engines
---

An execution engine is a technology/library used by SDLB to transform data. SDLB supports different execution engines and is able to combine different execution engines in the same data pipeline / job.
The data structure used to transport data between DataObjects and Actions is called a SubFeed.
Each Execution Engine has Subfeeds, Actions and Dataobjects associated with it. 


Currently SDLB supports the following execution engines:

|Category|Execution Engine|SubFeed Name|Description|Supported Actions|Supported DataObjects|
| ------ | -------------- | ---------- | --------- | --------------- | ------------------- |
|Java-Byte-Stream|File Engine|FileSubFeed|Transfer Byte-Streams without further knowledge about their content|FileTransferAction, CustomFileAction|all HadoopFileDataObjects, WebserviceFileDataObject, SFtpFileDataObject|
|Generic DataFrame API|Spark Engine|SparkSubFeed|Transform data with Spark DataFrame API|CopyAction, CustomDataFrameAction, DeduplicateAction, HistorizeAction|all Hadoop/SparkFileDataObject, AccessTableDataObject, AirbyteDataObject, CustomDfDataObject, DeltaLakeTableDataObject, HiveTableDataObject, JdbcTableDataObject, JmsDataObject, KafkaTopicDataObject, SnowflakeTableDataObject, SplunkDataObject, TickTockHiveTableDataObject|
|Generic DataFrame API|Snowflake/Snowpark Engine|SnowparkSubFeed|Transform data within Snowflake with Snowpark DataFrame API|SnowflakeTableDataObject|
|Script|Script Engine|ScriptSubFeed|Coordinate script task execution and notify DataObjects about script results|No public implementation for now|all DataObjects

### Connecting different execution engines

In order to build a data pipeline using different execution engines, you need a DataObject that supports both execution engines as interface, so that one execution engine can write the data in the DataObject and the other one can read from it.
- from FileSubFeed to SparkSubFeed (and vice-versa): any Hadoop/SparkFileDataObject like ParquetFileDataObject
- from SparkSubFeed to SnowparkSubFeed (and vice-versa): SnowflakeTableDataObject
- from ScriptSubFeed to any (and vice-versa): every DataObject is suitable

### Schema propagation

Note that a schema can only be propagated within a data pipeline for consecutive actions running with an execution engine of category "Generic DataFrame API". Whenever such an Action has an input from a different category, the schema is read again from the DataObject.

SDLB is able to convert schemas between different execution engines of category "Generic DataFrame API", e.g. Spark and Snowpark.

### Determining execution engine to use in "Generic DataFrame API" Actions

A "Generic DataFrame API" Action can run with different execution engines like Spark or Snowpark. It determines the execution engine to use in Init-phase by checking the supported types of inputs, outputs and transformations. The first common type is chosen. If there is no common type an exception is thrown.
To check which execution engine was chosen, look for logs like the following:

      INFO  CustomDataFrameAction - (Action~...) selected subFeedType SparkSubFeed
