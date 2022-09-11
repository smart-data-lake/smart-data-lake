---
id: actions
title: Actions
---

Actions describe dependencies between input and output DataObjects and necessary transformation to connect them. 

## Transformations
These can be custom transformers in SQL, Scala/Spark, or Python, OR predefined transformations like Copy, Historization and Deduplication, see [Transformations](transformations). 

## MetaData
As for DataObjects and Connections, various metadata can be provided for Action items. These help manage and explore data in the Smart Data Lake. Beside *name* and *description*, a *feed* and a list of *tags* can be specified. 

## ExecutionMode
By default all data in the specified DataObjects are processed. The execution mode option provides the possibility to select the data to process, e.g. partially process them. This can be specific partitions or also incremental processing. See [ExecutionMode](executionModes) for detailled description of the various possibilities.

## executionCondition
By default an Action is executed if all inputs are available, e.g. no input from a previous Action is skipped.
Override the default behaviour by specifying an *executionCondition* in SQL syntax on the Action. It is evaluated against the properties available in [[SubFeedsExpressionData]]. If true, the Action is executed, otherwise it is skipped. Details see also [[Condition]].

Example: execute if input stg-src1 is not skipped.
```
  action1 {
    type = CustomDataFrameAction
    inputIds = [stg-src1, stg-src2]
    outputIds = [int-tgt]
    executionCondition = {
      description = "execute if input stg-src1 is not skipped"
      expression = "!inputSubFeeds.stg-src1.isSkipped"
    }
    ...
```


## metricsFailCondition
Specify a condition in SQL syntax checking the metrics created by an Action. The expression is evaluated as where-clause against dataframe of metrics with columns `dataObjectId`, `key`, `value`. If there are any rows passing the where clause, the Action is failed (MetricCheckFailed exception) and further execution is stopped. 

To fail an action writting to output int-tgt in case there are no records written, specify `"dataObjectId = 'int-tgt' and key = 'no_data' and value = true"`.

This functionality is similar to [Expectations](dataQuality#Expectations), but the *metricsFailCondition* is defined on an Action and instead of a DataObject. And it can access all metrics produced by an Action, not the custom metric defined by the Expectation.

## recursiveInputIds
In general we want to avoid cyclic graph of action. This option enables updating DataObjects based on its own data. Therewith, the DataObject is input and output at the same time. It needs to be specified as output and as recursiveInputId, but not as input.

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
