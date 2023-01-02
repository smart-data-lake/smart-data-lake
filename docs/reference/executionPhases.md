---
id: executionPhases
title: Execution Phases
---

### Early validation
Execution of a SmartDataLakeBuilder run is designed with "early validation" in mind. This means it tries to fail as early as possible if something is wrong.

The following phases are involved during each execution:
1. **Parse configuration**:   
Parses and validates your configuration files.
This step fails if there is anything wrong with your configuration, i.e. if a required attribute is missing or a whole block like `actions {}` is missing or misspelled.
There's also a neat feature that will warn you of typos and will suggest spelling corrections if it can.
2. **DAG prepare:**  
Preconditions are validated.
This includes testing Connections and DataObject structures that must exists.
3. **DAG init:**  
Creates and validates the whole lineage of Actions according to the DAG.
For Spark Actions this involves the validation of the DataFrame lineage. 
A column which doesn't exist but is referenced in a later Action will fail the execution.
4. **DAG exec**:    
Apply [Execution Modes](executionMode.md) to select data and execute Actions.
Data is effectively transferred in this phase (and only in this phase!).

### Implications
#### Early validation
As mentioned, especially the init phase is very powerful as SDL will validate your whole lineage.
This even includes custom transformations you have in your pipeline.  
So if you have a typo in a column name or reference a column that will not exist at that state in the pipeline, 
SDL will report this error and fail within seconds. 
This saves a lot of time as you don't have to wait for the whole pipeline to execute to catch these errors.

#### No data during Init Phase
At one point you will start implementing your own transformers. 
When analyzing problems you will want to debug them and most likely set break points somewhere in the `transform` method.
It's important to know, that execution will pass your break point twice:  
Once during the `init` phase, once during `exec` phase. 
In the `init` phase, the whole execution is validated but without actually moving any data.
If you take a look at your DataFrames at this point, it will be empty.
We can guarantee that you will fall into this trap at least once. ;-)

:::caution
If you debug your code and wonder why your DataFrame is completely empty, 
you probably stopped execution during init phase.

Continue execution and make sure you're in the exec phase before taking a look at data in your DataFrame.
:::

#### Watch the log output
The stages are also clearly marked in the log output.
Here is the sample output of [part-3 of the gettings-started guide](../getting-started/part-3/custom-webservice.md) again with a few things removed:
```
Action~download-departures[CopyAction]: Prepare started
Action~download-departures[CopyAction]: Prepare succeeded
Action~download-departures[CopyAction]: Init started
Action~download-departures[CopyAction]: Init succeeded
Action~download-departures[CopyAction]: Exec started
...
```
If execution stops, always check during which phase that happens.
If it happens while still in the init phase, it probably has nothing to do with the data itself but more with the structure of your DataObjects.
