---
id: executionModes
title: Execution Modes
---

:::warning
This page is under review 
:::

## Execution modes
Execution modes select the data to be processed by a SmartDataLakeBuilder job. By default, if you start a SmartDataLakeBuilder job, there is no filter applied. This means every Action reads all data passed on by the predecessor Action. If an input has no predecessor Action in the DAG, all data is read from the corresponding DataObject.

You can set an execution mode by defining attribute "executionMode" of an Action. Define the chosen ExecutionMode by setting the type as follows:
```
executionMode {
  type = PartitionDiffMode
  attribute1 = ...
}
```
If no explicit executionMode is specified, the default behavior is used.

There are 2 major types of Execution Modes selecting the subset of data based on:

* __partitions__: the user selects specific partition to process, or lets SDLB select missing partitions automatically 
* __incremental__: SDLB will automatically selected new and updated data from the input and process data incrementally.

## Partitions
Partition is an ambiuous word. It is used in distributed data processing (e.g. Spark tasks and repartitioning), but also for spliting data in storage for performance reasons, e.g. a partitioned database table, kafka topic or hadoop directory structure. 

The use of partition in SDLB is about the later case. SDLB supports-wise processing of partitioned data (data splitted in storage) for optimal performance. Partition-wise data processing is one of the most performant data pipeline paradigms. But it also limits data processing to always write or replace whole partitions.

To use partitioned data and partition-wise data processing in SDLB, you need to configure partition columns on a DataObject that can handle partitions. This is for most DataObjects the case, even though some cannot directly access the partitions in the data storage. We implemented a concept called virtual partitions for them, see JdbcTableDataObject and KafkaTopicDataObject.

### Default Behavior
A filter based on partitions can be applied manually by specifying the command line parameter `--partition-values` or `--multi-partition-values`, see [Command Line](commandLine.md). The partition values specified are passed to **all** start inputs (inputs with no predecessor in the DAG), and filtered for every input DataObject by its defined partition columns.
On execution every Action takes the partition values of the main input and filters them again for every output DataObject by its defined partition columns, which serve again as partition values for the input of the next Action.

Actions with multiple inputs, e.g. CustomDataFrameAction will use heuristics to define a *main input*. The first input which is not skipped, is not in the list of *inputIdsToIgnoreFilter*, and has the most partition columns defined is choosen. It can be overriden by setting *mainInputId* property.

The default behaviour is active without providing any explicit Execution Mode in the config of your Actions. This means that processing partitioned data available out-of-the-box in SDLB and done automatically.

Note that during execution of the dag, no new partition values are added, they are only filtered. 

If the parameters `--partition-values` or `--multi-partition-values` are not specified, SDLB will process all available data.

An exception is if you place a `PartitionDiffMode` in the middle of your pipeline, see section [PartitionDiffMode](#partitiondiffmode-dynamic-partition-values-filter) below.

### FailIfNoPartitionValuesMode
The *FailIfNoPartitionValuesMode* enforces to have specified partition values. It simply checks if partition values are present and fails otherwise.
This is useful to prevent potential reprocessing of whole tables due to wrong usage.

### PartitionDiffMode: Dynamic partition values filter
In contrast to specifying the partitions manually, you can let SmartDataLakeBuilder find missing partitions and set partition values automatically by specifying execution mode *PartitionDiffMode*. This mode has a couple of options to fine tune:

By defining the **applyCondition** attribute you can give a condition to decide at runtime if the PartitionDiffMode should be applied or not.
Default is to apply the PartitionDiffMode if the given partition values are empty (partition values from command line or passed from previous action).
Define an applyCondition by a spark sql expression working with attributes of DefaultExecutionModeExpressionData returning a boolean.

Example - apply also if given partition values are not empty, e.g. always:
```
  applyCondition = "true"
```

By defining the **failCondition** attribute you can give a condition to fail application of execution mode if *true*.
It can be used to fail a run based on expected partitions, time and so on.
The expression is evaluated after execution of PartitionDiffMode. In the condition the following attributes are available amongst others to make the decision: **inputPartitionValues**, **outputPartitionValues** and **selectedPartitionValues**.
Default is that the application of the PartitionDiffMode does not fail the action. If there is no data to process, the following actions are skipped.
Define a failCondition by a spark sql expression working with attributes of PartitionDiffModeExpressionData returning a boolean.

Example - fail if partitions are not processed in strictly increasing order of partition column "dt":
```
  failCondition = "(size(selectedPartitionValues) > 0 and array_min(transform(selectedPartitionValues, x -&gt x.dt)) &lt array_max(transform(outputPartitionValues, x > x.dt)))"
```

Sometimes the failCondition can become quite complex with multiple terms concatenated by or-logic.
To improve interpretability of error messages, multiple fail conditions can be configured as array with attribute **failConditions**. For every condition you can also define a description which will be inserted into the error message.
<!--TODO show a short example of PartitionDiffMode.failConditions with descriptions -->

The option **selectExpression** defines the selection of custom partitions.
Define a spark sql expression working with attributes of PartitionDiffModeExpressionData returning a Seq(Map(String,String)).
Example - only process the last selected partition:
```
  selectExpression = "slice(selectedPartitionValues,-1,1)"
```

By defining **alternativeOutputId** attribute you can define another DataObject which will be used to check for already existing data.
This can be used to select data to process against a DataObject later in the pipeline.

<!--TODO describe also PartitionDiffMode options  partitionColNb -->
<!--TODO describe also PartitionDiffMode options  nbOfPartitionValuesPerRun -->
<!--TODO describe also PartitionDiffMode options  applyPartitionValuesTransform -->
<!--TODO describe also PartitionDiffMode options  selectAdditionalInputExpression -->

### CustomPartitionMode
This execution mode allows for custom logic to select partitions to process in Scala.

Implement trait CustomPartitionModeLogic by defining a function which receives main input & output DataObject and returns partition values to process as Seq[Map[String,String]\]


## Incremental load
Some DataObjects are not partitioned, but nevertheless you don't want to read all data from the input on every run. You want to load it incrementally.

SDLB implements various methods to incrementally process data from the different DataObjects. Choosing an incremental mode depends the DataObjects used, e.g. do they support Spark Streaming, the transformations implemented, e.g. does Spark Streaming support these transformations, and also if you want a synchronous data pipeline or run all steps asynchronously to improve latency.

### SparkStreamingMode

This can be accomplished by specifying execution mode SparkStreamingMode. Under the hood it uses "Spark Structured Streaming".
An Action with SparkStreamingMode in streaming mode is an asynchronous action. Its rhythm can be configured by setting a *triggerType* different from *Once* and a triggerTime.
If not in streaming mode SparkStreamingMode triggers a single microbatch per SDLB job. Like this the Action is executed synchronous. It is configured by setting triggerType=Once. 

"Spark Structured Streaming" is keeping state information about processed data. It needs a *checkpointLocation* configured which can be given as parameter to SparkStreamingMode.

Note that "Spark Structured Streaming" needs an input DataObject supporting the creation of streaming DataFrames.
For the time being, only the input sources delivered with Spark Streaming are supported.
This are KafkaTopicDataObject and all SparkFileDataObjects, see also [Spark StructuredStreaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets).

### DataFrameIncrementalMode
As not every input DataObject supports the creation of streaming DataFrames, e.g. JdbcTableDataObject, there is an other execution mode called SparkIncrementalMode.
You configure it by defining the attribute **compareCol** with a column name present in input and output DataObject.
SparkIncrementalMode then compares the maximum values between input and output and creates a filter condition.
On execution the filter condition is applied to the input DataObject to load the missing increment.
Note that compareCol needs to have a sortable datatype.

The **applyCondition** and **alternativeOutputId** attributes works the same as for [PartitionDiffMode](executionModes#partitiondiffmode-dynamic-partition-values-filter).

This execution mode has a performance drawback as it has to query the maximum value for *compareCol* on input and output in each SDLB job.

### DataObjectStateIncrementalMode
Performance can be optimized by remembering the state of the last increment. The state is saved in a .json file for which the user needs to provide a path using the `--state-path` option, see [Command Line](commandLine.md).

*DataObjectStateIncrementalMode* needs a main input DataObject that can create incremental output, e.g. JdbcTableDataObject, SparkFileDataObject or AirbyteDataObject. The inner meaning of the state is defined by the DataObject and not interpreted outside of it. Some DataObjects have a configuration option to define the incremental output, e.g. JdbcTableDataObject.incrementalOutputExpr, others just use technical timestamps existing by design, e.g. SparkFileDataObject. 

### FileIncrementalMoveMode
Another paradigm for incremental processing with files is to move or delete input files once they are processed. This can be achieved by using FileIncrementalMoveMode. If option *archiveSubdirectory* is configured, files are moved into that directory after processing, otherwise they are deleted.

FileIncrementalMoveMode is the only execution mode that can be used with the file engine (see also [Execution engines](executionEngines.md)), but also with SparkFileDataObjects and the data frame engine.

## Others

### ProcessAllMode
An execution mode which forces processing all data from it's inputs, removing partitionValues and filter conditions received from previous actions.


### CustomMode
This execution mode allows to implement aritrary processing logic using Scala.

Implement trait CustomModeLogic by defining a function which receives main input&output DataObject and returns a ExecutionModeResult. The result can contain input and output partition values, but also options which are passed to the transformations defined in the Action.