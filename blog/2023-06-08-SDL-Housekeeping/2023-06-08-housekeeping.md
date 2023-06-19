---
title: Housekeeping
description: Use advanced housekeeping functions to optimize execution
slug: sdl-housekeeping
authors:
  - name: Patrick Grütter
    url: https://github.com/pgruetter
tags: [housekeeping, performance, partitioning]
hide_table_of_contents: false
---

# Housekeeping
In this article, we're taking a look on how we use SDLB's housekeeping features to keep our pipelines running efficiently.

Some DataObject contain housekeeping features of their own. 
Make sure you use them!
For example, Delta Tables support commands like `optimize` and `vacuum` to optimize storage and delete no longer needed files.

But usually, those commands do not re-organize your partitions. 
This is where SDLBs housekeeping mode comes in.

The example is taken from a real world project we've implemented.

## Context
In this particular project we are collecting data from various reporting units and process it in batches.
The reporting units use an Azure Function to upload JSON files to an Azure Data Lake Storage. 
From there, we pick them up for validation and processing. 
Reporting units can upload data anytime, but it is only processed a few times a day.

Once validated, we use Delta Lake tables in Databricks to process data through the layers of the Lakehouse.

## Partitioning
The Azure Function puts uploaded JSON files in a subfolder for each reporting unit. 
As such, JSON files are already neatly partitioned by `reporting_unit`:
```
uploadFolder/
  reporting_unit=rp01
    file1.json
    file2.json
    file3.json
  reporting_unit=rp02
    file1.json
  reporting_unit=rp03
    fileX.json
```

To read these JSON files, we can therefore use the following DataObject definition:
```
import_json {
    type = JsonFileDataObject
    path = uploadFolder/
    partitions = [reporting_unit]    
}
```

These files are then processed with a `FileTransferAction` into an output DataObject `stage_json`: 
```
stage_json {
    type = FileTransferAction
    inputId = import_json
    outputId = stage_json
    executionMode = { type = FileIncrementalMoveMode }
    metadata.feed = stage_json
}
```

Each time we start to process uploaded data, we use the `run_id` to keep track of all batch jobs and version of files delivered.
If you use a state path (see [commandLine](../../docs/reference/commandLine)), 
your runs automatically use `run_id`s and you can use it by extending your DataObject:

```
stage_json {
    type = JsonFileDataObject
    path = processedFolder
    partitions = [run_id,reporting_unit]
    schema = """reporting_unit string, run_id string, ...."""
}
```
Note how we just use run_id as part of the schema without any further declaration.
Since we use the state path, SDLB uses a `run_id` internally, and we can use it too.

## Drawback
Let's take a look at the resulting partition layout of `stage_json`:
```
processedFolder/
  run_id=1/
    reporting_unit=rp01/
      file1.json
      file2.json
      file3.json
    reporting_unit=rp02/
      file1.json
    reporting_unit=rp03/
      fileX.json
```
This partition layout has many advantages in our case as we know exactly 
during which run a particular file was processed and which reporting unit uploaded it.
In further stages we can clearly work with files that were processed in the current run and not touch any old `run_id`s. 

But the drawbacks of this partition scheme becomes apparent after running for a longer time:
  - Some reporting units don't upload data for days. You end up with only a few reporting_unit partitions per run_id.
  - File sizes are rather small (< 1 MiB), partition sizes end up very small too.
  - If you use hourly runs and run 24/7, you end up with 168 partitions per week, plus sub-partitions for reporting units.
  - Once files are correctly processed, we don't read the uploaded files anymore. 
    We still keep them as raw files should we ever need to re-process them.

If you have actions working with all partitions, they will become very slow.
Spark doesn't like a lot of small partitions.

To mitigate these drawbacks, we use SDLB's Housekeeping Feature.

## HousekeepingMode
If you take a look at DataObject's parameters, you will see a `housekeepingMode`. 
There are two modes available:
  - **PartitionArchiveCompactionMode**: to compact / archive partitions  
  - **PartitionRetentionMode**: to delete certain partitions completely

### PartitionArchiveCompactionMode
In this mode, you solve two tasks at once:
  - You define how many smaller partitions are aggregated into one larger partition (archive)
  - Rewrite all files in a partition to combine many small files into larger files (compact)


#### Archive
In our example above, we stated that we don't want to alter any input files, so we won't use compaction.
We want to keep them as is (raw data). 
But we do want to get rid of all the small partitions after a certain amount of time. 
For that, we extend `stage_json` to include the `housekeepingMode` with a `archivePartitionExpression`:

```
stage_json {
    type = JsonFileDataObject
    path = processedFolder
    partitions = [run_id,reporting_unit]
    schema = """reporting_unit string, run_id string, ...."""
    housekeepingMode = {
      type = PartitionArchiveCompactionMode
      archivePartitionExpression = "if( elements.run_id < (runId - 500), map('run_id', (cast(elements.run_id as integer) div 500) * 500, 'reporting_unit', elements.reporting_unit), elements)"
    }
}
```

This expression probably needs some explanation:  
The Spark SQL expression works with attributes of [`PartitionExpressionData`](https://github.com/smart-data-lake/smart-data-lake/blob/master-spark3/sdl-core/src/main/scala/io/smartdatalake/workflow/dataobject/HousekeepingMode.scala#L136).
In this case we use `runId` (the current runId) and `elements` (all partition values as map(string,string)).
It needs to return a map(string,string) to define new partition values. 
In our case, it needs to define `run_id` and `reporting_unit` because these are the partitions defined in `stage_json`.

Let's take the expression apart:  
`if(elements.run_id < (runId - 500), ...`  
Only archive the partition if it's runId is older than 500 run_ids ago. 

`map('run_id', (cast(elements.run_id as integer) div 500) * 500, 'reporting_unit', elements.reporting_unit)`  
Creates the map with the new values for the partitions. 
The run_id is floored to the next 500 value, so as example, the new value of run_id 1984 will be 1500 (because integer 1984/500=3, 3*500=1500).  
Remember that we need to return all partition values in the map, also the ones we don't want to alter.
For `reporting_unit` we simply return the existing value `elements.reporting_unit`.

`..., elements)`  
This is the else condition and simply returns the existing partition values if there is nothing to archive.

:::info
The housekeeping mode is applied after writing a DataObject.
Keep in mind, that it is always executed.
:::

#### Compaction
We don't want compact files in our case. 
But from the documentation you can see that compaction works very similarly:  
You also work with attributes from `PartitionExpressionData` but instead of new partition values, 
you return a boolean to indicate for each partition if it should be compacted or not. 

### PartitionRetentionMode
Again, not used in our example as we never delete old files.
But if you need to, you define a Spark SQL expression returning a boolean indicating if a partition should be retained or deleted.

```
stage_json {
    type = JsonFileDataObject
    path = processedFolder
    partitions = [run_id,reporting_unit]
    schema = """reporting_unit string, run_id string, ...."""
    housekeepingMode = {
      type = PartitionRetentionMode
      retentionCondition = "elements.run_id > (runId - 500)"
    }
}
```

## Result
In our example, we had performance gradually decreasing because Spark had to read more than 10'000 partitions and subpartitions.
Just listing all available partitions, even if you only worked with the most recent one, took a few minutes and these operations added up.

With the housekeeping mode enabled, older partitions continuously get merged into larger partitions containing up to 500 runs. 
This brought the duration of list operations back to a few seconds.

The operations are fully automated, no manual intervention is required.