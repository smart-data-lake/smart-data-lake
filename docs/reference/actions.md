---
id: actions
title: Actions
---

Actions describe dependencies between input and output dataObjects and necessary transformation to connect them. 

## Transformations
These can be custom transformers in SQL, Scala/Spark, or Python, OR predefined transformations like Copy, Historization and Deduplication, see [Transformations](transformations). 

## MetaData
As well as dataObject and connections, various metadata can be provided for action items. These help manage and explore data in the Smart Data Lake. Beside *name* and *description*, a *feed* and a list of *tags* can be specified. 

## ExecutionMode
By default all data in the specified dataObjects are processes. The option execution mode provides the possibility to e.g. partially process them. This could be specific partitions or incrementally process, see [ExecutionMode](executionModes). 
<!--TODO -->

## ExecutionCondition
A condition can be specified in SQL format, which can be further used in other options, e.g. failCondition of PartitionDiffMode. 
<!--TODO -->

## recursiveInputIds
In general we want to avoid cyclic graph of action. This option enables updating dataObjects. Therewith, the dataObject is input and output at the same time. It needs to be specified in as output, but not as input.

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
