---
id: executionModes
title: Execution Modes
---

## Execution modes
Execution modes select the data to be processed by a SmartDataLakeBuilder Action.
By default, there is no filter applied meaning that every Action reads all data passed on by the predecessor Action.
If an input has no predecessor Action in the DAG, all data is read from the corresponding DataObject.

This default behavior is applied if you don't set an explicit execution mode.
If you want one of the execution modes described below, you have to explicitly set it on the Action:
```
executionMode {
  type = PartitionDiffMode
  attribute1 = ...
}
```

There are 2 major types of execution modes selecting the subset of data based on:

* __partition-wise__: Either SDLB will automatically select and process missing partitions from the input, or the partitions are defined manually by a command line parameter.
* __incremental__: SDLB will automatically select new and updated data from the input and process data incrementally.



## Partitions
*Partition* is an ambiguous term.
It is used in distributed data processing (e.g. Spark tasks and repartitioning), but also for splitting data in storage for performance reasons,
e.g. a partitioned database table, kafka topic or hadoop directory structure.

The use of partition in SDLB is about the latter case.
SDLB supports intelligent processing of partitioned data (data split in storage) for optimal performance.

:::info
Partition-wise data processing is one of the most performant data pipeline paradigms.
But it also limits data processing to always write or replace whole partitions.
Keep that in mind when designing your Actions.
:::

To use partitioned data and partition-wise data processing in SDLB, you need to configure partition columns on a DataObject that can handle partitions.
i.e. on a `DeltaLakeTableDataObject`:
```
partitionedDataObject {
    type = DeltaLakeTableDataObject
    table = {
        name = "partitionedTable"
    }
    partitions = ["day"]
}
```
Note that not all DataObjects support partitioning.

TODO:
<!--
too detailed:
This is the case for most DataObjects, even though some cannot directly access the partitions in the data storage. 
We implemented a concept called virtual partitions for them, see JdbcTableDataObject and KafkaTopicDataObject.
-->

### Default Behavior
In its simplest form, you manually define the partition values to be processed by specifying the command line parameter `--partition-values` or `--multi-partition-values`, see [Command Line](commandLine.md).
This is in the form of columnName=columnValue, e.g. `--partition-values day=2020-01-01`.
The partition values specified are passed to **all** start inputs (inputs with no predecessor in the DAG).
In other words, each input DataObject is filtered by the given partition values on the specified column(s).

If the parameters `--partition-values` or `--multi-partition-values` are not specified, SDLB will simply process all available data.

Subsequent Actions will automatically use the same partition values as their predecessor Action.

:::info
Actions can also have multiple input DataObjects (e.g. CustomDataFrameAction).
In this case, a *main input* needs to be defined to take the partition values from.
You can either set the main input yourself by specifying the `mainInputId` property on the Action, or SDLB will automatically choose the input.  
Automatic selection uses a heuristic:  
The first input which is not skipped, is not in the list of `inputIdsToIgnoreFilter`, and has the most partition columns defined is chosen.
:::

TODO: Check
Another special case you might encounter is if an output DataObject has different partition columns than the input DataObject.
In these cases, the partition values of the input DataObject will be used and passed on to the next action.  
So if your input DataObject has [runId] as partition values and your output DataObject [runId, division],
then `division` will be removed from the partition values before they are passed on.

This default behaviour is active without providing any explicit `executionMode` in the config of your Actions.
This means that processing partitioned data is available out-of-the-box in SDLB.

:::info
The partition values act as filter.
If you define partition values that do not exist in the input dataObject, these partitions will not be automatically created for you.

TODO:
--SDLB also validates that partition values actually exist in input DataObjects.
SDLB also validates that partition values exist in input data objects (can be overriden by DataObject.expectedPartitionsCondition). This is especially interesting with CustomDataFrameAction and multiple partitioned inputs, so you dont just join them even if some input data is missing...
Isn't that contradicting the above sentence? What happens if values don't exist? Simply not created?
Or pipeline fails?

:::


### FailIfNoPartitionValuesMode
If you use the method described above, you might want to set the executionMode to `FailIfNoPartitionValuesMode`.
This mode enforces having partition values specified.
It simply checks if partition values are present and fails otherwise.
This is useful to prevent potential reprocessing of whole tables due to wrong usage.

### PartitionDiffMode: Dynamic partition values filter
Instead of specifying the partition values manually, you can let SDLB find missing partitions and set partition values automatically by specifying execution mode `PartitionDiffMode`.
In its basic form, it compares input and output DataObjects and decides which partitions are missing in the output.
It then automatically uses these missing partitions as partition values.

:::info
Remember that a partition can exist in the output DataObject with no data or partial data only.
For SDLB, the partition exists and will not be processed again.
Keep that in mind when recovering from errors.
:::

This mode is quite powerful and has a couple of options to fine tune its behaviour.

#### applyCondition
By default, the PartitionDiffMode is applied if the given partition values are empty (partition values from command line or passed from previous action).
You can override this behavior by defining an `applyCondition`.

ApplyCondition is a spark sql expression working with attributes of [DefaultExecutionModeExpressionData](https://github.com/smart-data-lake/smart-data-lake/blob/c435434d0174ca0862bdeef4838c6678aaef05fd/sdl-core/src/main/scala/io/smartdatalake/workflow/action/executionMode/ExecutionMode.scala#L164) returning a boolean.

Simple Example - apply PartitionDiffMode even if partition values are given (always):
```
  applyCondition = "true"
``` 

<!-- advanced example with feed / application etc. ?? -->

#### failCondition
If you have clear expectations of what your partition values should look like, you can enforce your rules by defining a `failCondition`.
In it, you define a spark sql expression that is evaluated after the PartitionDiffMode is applied.
If it evaluates to `true`, the Action will fail.

In the condition, the following attributes are available amongst others to make the decision: `inputPartitionValues`, `outputPartitionValues` and `selectedPartitionValues`.
Use these to fail the run based on expected partitions or time conditions.

Default is `false` meaning that the application of the PartitionDiffMode does not fail the action.
If there is no data to process, the following actions are skipped.

Example - fail if partitions are not processed in strictly increasing order of partition column `dt`:
```
  failCondition = "(size(selectedPartitionValues) > 0 and array_min(transform(selectedPartitionValues, x -> x.dt)) < array_max(transform(outputPartitionValues, x > x.dt)))"
```

Sometimes the `failCondition` can become quite complex with multiple terms concatenated by or-logic.
To improve interpretability of error messages, multiple fail conditions can be configured as an array using the attribute `failConditions`.
For each condition you can also define a description which will be inserted into the error message.

<!--TODO show a short example of PartitionDiffMode.failConditions with descriptions -->

#### selectExpression
The option `selectExpression` refines or overrides the list of selected output partitions. It can access the partition values selected by the default behaviour and refine the list, or it can override selected partition values by using input & output partition values directly.
Define a spark sql expression working with attributes of PartitionDiffModeExpressionData returning a `Seq(Map(String,String))`.

Example - only process the last selected partition:
```
  selectExpression = "slice(selectedPartitionValues,-1,1)"
```

#### alternativeOutputId
Usually, PartitionDiffMode will check for missing partitions against the main output DataObject.
You can force PartitionDiffMode to check for missing partitions against another DataObject by defining `alternativeOutputId`.

This can even be a DataObject that comes later in your pipeline so it doesn't have to be one of the Actions output DataObjects.

#### partitionColNb
TODO

#### nbOfPartitionValuesPerRun
If you have a lot of partitions, you might want to limit the number of partitions processed per run.
If you define `nbOfPartitionValuesPerRun`, PartitionDiffMode will only process the first n partitions and ignore the rest.

#### applyPartitionValuesTransform
TODO

#### selectAdditionalInputExpression
TODO


### CustomPartitionMode
This execution mode allows for complete customized logic to select partitions to process in Scala.
Implement trait `CustomPartitionModeLogic` by defining a function which receives main input & output DataObjects and returns partition values to process as `Seq[Map[String,String]]`


## Incremental load
Some DataObjects are not partitioned, but nevertheless you don't want to read all data from the input on every run.
You want to load it incrementally.

SDLB implements various methods to incrementally process data from various DataObjects.
Choosing an incremental mode depends on the DataObjects used.
Do they support Spark Streaming?
Does Spark Streaming support the transformations you're using?
Do you want a synchronous data pipeline or run all steps asynchronously to improve latency?

### DataFrameIncrementalMode
One of the most common forms of incremental processing is `DataFrameIncrementalMode`.
You configure it by defining the attribute `compareCol` naming a column that exists in both the input and output DataObject.
`DataFrameIncrementalMode` then compares the maximum values between input and output to decide what needs to be loaded.

For this mode to work, `compareCol` needs to be of a sortable datatype like int or timestamp.

The attributes `applyCondition` and `alternativeOutputId` work the same as for [PartitionDiffMode](executionModes#partitiondiffmode-dynamic-partition-values-filter).

:::info
This execution mode has a performance drawback as it has to query the maximum value for `compareCol` on input and output DataObjects each time.
:::

### DataObjectStateIncrementalMode
To optimize performance, SDLB can remember the state of the last increment it successfully loaded.
For this mode to work, you need to start SDLB with a `--state-path`, see [Command Line](commandLine.md).
The .json file used to store the overall state of your application will be extended with state information for this DataObject.

`DataObjectStateIncrementalMode` needs a main input DataObject that can create incremental output, e.g. `JdbcTableDataObject`, `SparkFileDataObject` or `AirbyteDataObject`.
The inner meaning of the state is defined by the DataObject and not interpreted outside of it.
Some DataObjects have a configuration option to define the incremental output, e.g. `JdbcTableDataObject.incrementalOutputExpr`, others just use technical timestamps existing by design,
e.g. `SparkFileDataObject`.

:::info
This option also comes in handy if you can't use `DataFrameIncrementalMode` because you can't access the output DataObject during initialization.
For example, if you push incremental Parquet files to a remote storage and these files are immediately processed and removed,
you will find an empty directory and therefore can't consult already uploaded data.
In this case, SDLB needs to remember what data increments were already uploaded.
:::

### SparkStreamingMode
SDLB also supports a streaming with the `SparkStreamingMode`.
Under the hood it uses [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html).
An Action with SparkStreamingMode in streaming mode is an asynchronous action.

Its rhythm can be configured by setting a `triggerType` to either `Once`, `ProcessingTime` or `Continuous` (default is `Once`).
When using `ProcessingTime` or `Continuous` you can configure the interval through the attribute `triggerTime`.

:::info
You need to start your SDLB run with the `--streaming` flag to enable streaming mode.
If you don't, SDLB will always use `Once` as trigger type (single microbatch) and the action is executed synchronously.
:::

Spark Structured Streaming is keeping state information about processed data.
To do so, it needs a `checkpointLocation` configured on the SparkStreamingMode.

Note that *Spark Structured Streaming* needs an input DataObject that supports the creation of streaming DataFrames.
For the time being, only the input sources delivered with Spark Streaming are supported.
These are `KafkaTopicDataObject` and all `SparkFileDataObjects`, see also [Spark StructuredStreaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets).


### KafkaStateIncrementalMode
TODO


### FileIncrementalMoveMode
Another paradigm for incremental processing with files is to move or delete input files once they are processed.
This can be achieved by using FileIncrementalMoveMode. If option `archiveSubdirectory` is configured, files are moved into that directory after processing, otherwise they are deleted.

FileIncrementalMoveMode is the only execution mode that can be used with the file engine (see also [Execution engines](executionEngines.md)), but also with SparkFileDataObjects and the data frame engine.

## Others

### ProcessAllMode
An execution mode which forces processing of all data from its inputs.
Any partitionValues and filter conditions received from previous actions are ignored.


### CustomMode
This execution mode allows to implement arbitrary processing logic using Scala.

Implement trait `CustomModeLogic` by defining a function which receives main input and output DataObject and returns an `ExecutionModeResult`.
The result can contain input and output partition values, but also options which are passed to the transformations defined in the Action.