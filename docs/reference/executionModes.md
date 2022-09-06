---
id: executionModes
title: Execution Modes
---

:::warning
This page is under review 
:::

## Execution modes
Execution modes select the data to be processed. By default, if you start SmartDataLakeBuilder, there is no filter applied. This means every Action reads all data from its input DataObjects.

You can set an execution mode by defining attribute "executionMode" of an Action. Define the chosen ExecutionMode by setting type as follows:
```
executionMode {
  type = PartitionDiffMode
  attribute1 = ...
}
```
If no explicit executionMode is specified, the default behavior is used (see chapter below).

There are 2 major types of Execution Modes selecting the subset of data based on:

* partitions 
* updates (incremental)

With partitions, the user selects the targeted partition or selecting all different partitions. 
With incremental, the difference between input and output can be selected. 

## Partitions
<!--TODO find a good introduction to partitions-->

### Default Behavior
A filter based on partitions can be applied manually by specifying the command line parameter `--partition-values` or `--multi-partition-values`, see [Command Line](commandLine.md). The partition values specified are passed to **all** start-Actions of a DAG and filtered for every input DataObject by its defined partition columns.
On execution every Action takes the partition values of the input and filters them again for every output DataObject by its defined partition columns, which serve again as partition values for the input of the next Action.
This can be used without providing any explicit Execution Mode in the config of your Actions. It is the Default Behavior of all Actions. 
Note that during execution of the dag, no new partition values are added, they are only filtered. An exception is if you place a `PartitionDiffMode` in the middle of your pipeline, see section [PartitionDiffMode](#partitiondiffmode-dynamic-partition-values-filter) below.
If the parameters `--partition-values` or `--multi-partition-values` are not specified, SDLB will process all available data.

### FailIfNoPartitionValuesMode
The *FailIfNoPartitionValuesMode* enforces to specified partition values. It simply checks if partition values are present and fails otherwise.
This is useful to prevent potential reprocessing of whole tables due to wrong usage.

### PartitionDiffMode: Dynamic partition values filter
In contrast to specifying the partitions manually, you can let SmartDataLakeBuilder find missing partitions and set partition values automatically by specifying execution mode *PartitionDiffMode*. This mode has a couple of options to fine tune:

By defining the **applyCondition** attribute you can give a condition to decide at runtime if the PartitionDiffMode should be applied or not.
Default is to apply the PartitionDiffMode if the given partition values are empty (partition values from command line or passed from previous action).
Define an applyCondition by a spark sql expression working with attributes of DefaultExecutionModeExpressionData returning a boolean.
<!--TODO provide an example for PartitionDiffMode.applyCondition -->

By defining the **failCondition** attribute you can give a condition to fail application of execution mode if *true*.
It can be used to fail a run based on expected partitions, time and so on.
The expression is evaluated after execution of PartitionDiffMode. In the condition are attributes available, like **inputPartitionValues**, **outputPartitionValues** and **selectedPartitionValues** , amongst others, to make the decision.
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
This prepares for custom specification of partition logic in scala.
Implement trait CustomPartitionModeLogic by defining a function which receives main input&output DataObject and returns partition values to process as Seq[Map[String,String]\]


## Incremental load
<!-- TODO introduction to incremental -->



### SparkStreamingMode: Incremental load
Some DataObjects are not partitioned, but nevertheless you don't want to read all data from the input on every run. You want to load it incrementally.
This can be accomplished by specifying execution mode SparkStreamingMode. Under the hood it uses "Spark Structured Streaming".
In streaming mode this an Action with SparkStreamingMode is an asynchronous action. Its rhythm can be configured by setting triggerType and triggerTime.
If not in streaming mode SparkStreamingMode triggers a single microbatch by using triggerType=Once and is fully synchronized. Synchronous execution can be forced for streaming mode as well by explicitly setting triggerType=Once.
"Spark Structured Streaming" is keeping state information about processed data. It needs a checkpointLocation configured which can be given as parameter to SparkStreamingMode.

Note that "Spark Structured Streaming" needs an input DataObject supporting the creation of streaming DataFrames.
For the time being, only the input sources delivered with Spark Streaming are supported.
This are KafkaTopicDataObject and all SparkFileDataObjects, see also [Spark StructuredStreaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets).

<!-- TODO does SparkIncrementalMode still exist or is it now better specified with the other incremental modes? -->
### SparkIncrementalMode: Incremental Load
As not every input DataObject supports the creation of streaming DataFrames, there is an other execution mode called SparkIncrementalMode.
You configure it by defining the attribute **compareCol** with a column name present in input and output DataObject.
SparkIncrementalMode then compares the maximum values between input and output and creates a filter condition.
On execution the filter condition is applied to the input DataObject to load the missing increment.
Note that compareCol needs to have a sortable datatype.

By defining **applyCondition** attribute you can give a condition to decide at runtime if the SparkIncrementalMode should be applied or not.
Default is to apply the SparkIncrementalMode. Define an applyCondition by a spark sql expression working with attributes of DefaultExecutionModeExpressionData returning a boolean.

By defining **alternativeOutputId** attribute you can define another DataObject which will be used to check for already existing data.
This can be used to select data to process against a DataObject later in the pipeline.

<!-- TODO describe DataFrameIncrementalMode -->
<!-- TODO describe FileIncrementalMode -->




## ProcessAllMode
An execution mode which forces processing all data from it's inputs, removing partitionValues and filter conditions received from previous actions.

## DataObjectStateIncrementalMode
An execution mode for incremental processing by remembering DataObjects state from last increment. The state is saved in a .json file for which the user needs to provide a path using the `--state-path` option. An application name must also be specified with the `-n` option. The state file will be named after the application name.

## Execution Condition
For every Action an executionCondition can be defined. The execution condition allows to define if an action is executed or skipped. The default behaviour is that an Action is skipped if at least one input SubFeed is skipped.
Define an executionCondition by a spark sql expression working with attributes of SubFeedsExpressionData returning a boolean.
The Action is skipped if the executionCondition is evaluated to false. In that case dependent actions get empty SubFeeds marked with isSkipped=true as input.

Example - skip Action only if input1 and input2 SubFeed are skipped:
```
  executionCondition = "!inputSubFeeds.input1.isSkipped or !inputSubFeeds.input2.isSkipped"
```

Example - Always execute Action and use all existing data as input:
```
  executionCondition = true
  executionMode = ProcessAllMode
```
