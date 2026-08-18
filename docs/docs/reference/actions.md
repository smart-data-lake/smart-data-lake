---
id: actions
title: Actions
---

Actions describe dependencies between input and output DataObjects and necessary transformation to connect them.

Some Actions allow only one input and one output, e.g. CopyAction, others can cope with several inputs and outputs, e.g. CustomDataFrameActions. As a best practice implement *n:m* Actions only if you have a good reason, otherwise stick to **1:1**, *1:n* and *n:1* Actions in order to know exact dependencies from metadata.

## Available Actions

The following Actions are shipped with SDLB. The *SubFeed types* column lists the [Execution Engines](executionEngines) an Action can run with, see [below](#choosing-the-subfeed-type) how the engine is chosen.
All parameters of every Action are listed in the [Configuration Schema Viewer](../../json-schema-viewer).

| Action | Inputs:Outputs | SubFeed types | Description                                                                                                                                     |
| ------ | -------------- | ------------- |-------------------------------------------------------------------------------------------------------------------------------------------------|
| CopyAction | 1:1 | SparkSubFeed, SparkConnectSubFeed, SnowparkSubFeed | Read the input into a DataFrame, optionally transform it, and write it to the output. The standard 1:1 Action.                                  |
| [CustomDataFrameAction](actions/customDataFrameAction.md) | n:m | SparkSubFeed, SparkConnectSubFeed, SnowparkSubFeed | Transform many inputs into many outputs with DataFrames. Use it for joins and unions (n:1), but also fan-outs (1:n).                            |
| [DeduplicateAction](actions/deduplicateAction.md) | 1:1 | SparkSubFeed, SparkConnectSubFeed | Keep the latest version of every record, also after it was deleted in the source. Adds column `dl_ts_captured`.                                 |
| [HistorizeAction](actions/historizeAction.md) | 1:1 | SparkSubFeed, SparkConnectSubFeed | Build a technical history (Slowly Changing Dimension Type 2) with validity columns `dl_ts_captured` and `dl_ts_delimited`.                      |
| FileTransferAction | 1:1 | FileSubFeed | Transfer files between SFtp, Hadoop, local filesystem and webservices as is, without interpreting their content.                                |
| CustomFileAction | 1:1 | FileSubFeed | Transform files as byte/line streams with a custom transformer, distributed on Spark executors, e.g. to unzip, decrypt or repair a file format. |
| CustomScriptAction | n:m | ScriptSubFeed | Execute scripts once all inputs are ready and notify the outputs afterwards. No data is read or written by SDLB.                                |

:::info
Choose the most specific Action for the job: it keeps the configuration short and the lineage accurate.
Use CopyAction for a plain 1:1 copy and CustomDataFrameAction only if you really need multiple inputs or outputs.
If the data does not need to be interpreted at all, FileTransferAction avoids reading it into a DataFrame.
:::

### Choosing the SubFeed type

Actions of the "Generic DataFrame API" category (CopyAction, CustomDataFrameAction, DeduplicateAction, HistorizeAction) are implemented independently of a concrete DataFrame library.
SDLB determines the SubFeed type to use in Init-phase from the types supported by all inputs, outputs and transformers, restricted to the engine of the Actions [engine connection](executionEngines#engine-connections), see [Execution Engines](executionEngines#determining-execution-engine-to-use-in-generic-dataframe-api-actions).

DeduplicateAction and HistorizeAction additionally need an output DataObject which is a transactional table supporting SQL merge, currently DeltaLakeTableDataObject, IcebergTableDataObject, JdbcTableDataObject and SparkConnectTableDataObject.
This limits them to the Spark and Spark Connect engines in practice, even though their implementation is engine-agnostic.

If none of the Actions above fits, an own Action can be implemented, see [Extending SDLB](extending). Prefer a [transformation](transformations) on an existing Action whenever possible: it keeps the lineage SDLB derives from your configuration accurate.

## Transformations
These can be custom transformers in SQL, Scala/Spark, or Python, OR predefined transformations like Copy, Historization and Deduplication, see [Transformations](transformations).

## Metadata
As for DataObjects and Connections, various metadata can be provided for Action items. These help manage and explore data in the Smart Data Lake. Beside *name* and *description*, a *feed* and a list of *tags* can be specified. 

## ExecutionMode
By default, all data in the specified DataObjects are processed. The execution mode option provides the possibility to select the data to process, e.g. partially process them. This can be specific partitions or also incremental processing. See [ExecutionMode](executionModes) for detailled description of the various possibilities.

## executionCondition
By default, an Action is executed if all inputs are available, e.g. no input from a previous Action is skipped.
Override the default behaviour by specifying an *executionCondition* in SQL syntax on the Action. It is evaluated against the properties available in [[SubFeedsExpressionData]]. If true, the Action is executed, otherwise it is skipped. Details see also [[Condition]].

Example: execute if input stg-src1 or input stg-src2 is not skipped.
```
  action1 {
    type = CustomDataFrameAction
    inputIds = [stg-src1, stg-src2]
    outputIds = [int-tgt]
    executionCondition = {
      description = "execute if at least one of the inputs is not skipped"
      expression = "!inputSubFeeds['stg-src1'].isSkipped or !inputSubFeeds['stg-src2'].isSkipped"
    }
    ...
```


## metricsFailCondition
Specify a condition in SQL syntax checking the metrics created by an Action. The expression is evaluated as where-clause against dataframe of metrics with columns `dataObjectId`, `key`, `value`. If there are any rows passing the where clause, the Action is failed (MetricCheckFailed exception) and further execution is stopped. 

To fail an action writing to output `int-tgt` in case there are no records written, specify `"dataObjectId = 'int-tgt' and key = 'no_data' and value = true"`.

This functionality is similar to [Expectations](dataQuality#expectations-on-dataobjects), but the *metricsFailCondition* is defined on an Action and instead of a DataObject. And it can access all metrics produced by an Action, not the custom metric defined by the Expectation.

## recursiveInputIds
In general, we want to avoid cyclic graph of action. This option enables updating DataObjects based on its own data. Therewith, the DataObject is input and output at the same time. It needs to be specified as output and as recursiveInputId, but not as input.

Example: assuming an object `stg-src`, which data should be added to an growing table `int-tgt`.

```
  action1 {
    type = CustomDataFrameAction
    inputIds = [stg-src]
    outputIds = [int-tgt]
    recursiveInputIds = [int-tgt]
    ...
```

<!-- TODO describe more action facts
-->
