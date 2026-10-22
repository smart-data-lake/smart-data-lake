---
id: customDataFrameAction
title: CustomDataFrameAction
---

CustomDataFrameAction transforms data between **many input and many output DataObjects** using DataFrames.
It is the Action to use for joins, unions and fan-outs, everything that cannot be expressed as a 1:1 relation.

The transformation itself is not part of the Action: it is configured as a list of many-to-many transformers, in SQL, Scala or Python, see [Transformations](../transformations).

:::info
Although *n:m* is possible, stick to *n:1* or *1:m*. With several outputs SDLB can no longer tell which input contributed to which output, so lineage and impact analysis become inaccurate.
For a plain 1:1 relation prefer CopyAction, which derives more accurate lineage and needs less configuration.
:::

## Requirements

* Inputs must be DataObjects able to create a DataFrame, outputs must be able to write one.
* The last transformer in the list must return a result for **every** `outputId`, keyed by the id of the output DataObject. Otherwise the Action fails with a configuration error listing the results it did receive.
* All inputs, outputs and transformers must share a common SubFeed type, see [Choosing the SubFeed type](../actions.md#choosing-the-subfeed-type).

## Example: joining two inputs

Many-to-many transformers use a map of output DataObject id to code, so SDLB knows which result belongs to which output.
Input DataObjects are available as SQL views named `<dataObjectId>_sdltemp`, with special characters replaced by an underscore. Use the token `%{inputViewName_<dataObjectId>}` to avoid spelling that out:

```
actions {
  join-departures-airports {
    type = CustomDataFrameAction
    inputIds = [stg-departures, int-airports]
    outputIds = [btl-departures-arrivals-airports]
    mainInputId = stg-departures
    transformers = [{
      type = SQLDfsTransformer
      code = {
        btl-departures-arrivals-airports = """
          select d.*, a.name, a.latitude_deg, a.longitude_deg
          from %{inputViewName_stg-departures} d
          join %{inputViewName_int-airports} a on d.estdepartureairport = a.ident
        """
      }
    }]
  }
}
```

Note that the key of the `code` map (`btl-departures-arrivals-airports`) is the output DataObject id, not a free name.

## Example: fan-out into two outputs

```
actions {
  split-departures {
    type = CustomDataFrameAction
    inputIds = [stg-departures]
    outputIds = [int-departures-ch, int-departures-other]
    transformers = [{
      type = SQLDfsTransformer
      code = {
        int-departures-ch = "select * from %{inputViewName_stg-departures} where estdepartureairport like 'LS%'"
        int-departures-other = "select * from %{inputViewName_stg-departures} where estdepartureairport not like 'LS%'"
      }
    }]
  }
}
```

## Chaining transformers

`transformers` are applied in the order of the list. The results of a transformer are added to the DataFrames available to the next one, under the names it used as keys.
This allows to build intermediate results, which is often more readable than one large statement:

```
    transformers = [{
      type = SQLDfsTransformer
      code = {
        departures-cleaned = "select * from %{inputViewName_stg-departures} where estdepartureairport is not null"
      }
    },{
      type = SQLDfsTransformer
      code = {
        btl-departures-arrivals-airports = """
          select d.*, a.name from %{inputViewName_departures-cleaned} d
          join %{inputViewName_int-airports} a on d.estdepartureairport = a.ident
        """
      }
    }]
```

Only the results of the **last** transformer are mapped to output DataObjects. Intermediate results like `departures-cleaned` above are not written anywhere, and they do not need to be declared in `outputIds`.

## Main input and main output

With more than one input or output, SDLB needs to know which one to use for [ExecutionMode](../executionModes) evaluation and partition value propagation.
It picks the DataObject with the most partition columns by default, which is often not what you want. Be explicit:

```
    mainInputId = stg-departures
    mainOutputId = btl-departures-arrivals-airports
```

For a join this is typically the fact table on the input side, while the dimension tables are read completely.
To read a dimension table completely although a filter or partition values are propagated, list it in `inputIdsToIgnoreFilter`:

```
    inputIdsToIgnoreFilter = [int-airports]
```

## Optional inputs

By default an Action is only executed if none of its inputs was skipped. For a union of sources which are not always available, override this with an `executionCondition`, see [Actions](../actions.md#executioncondition):

```
actions {
  union-sources {
    type = CustomDataFrameAction
    inputIds = [stg-src1, stg-src2]
    outputIds = [int-tgt]
    executionCondition = {
      description = "execute if at least one of the inputs is not skipped"
      expression = "!inputSubFeeds['stg-src1'].isSkipped or !inputSubFeeds['stg-src2'].isSkipped"
    }
    transformers = [{
      type = SQLDfsTransformer
      code = {
        int-tgt = "select * from %{inputViewName_stg-src1} union all select * from %{inputViewName_stg-src2}"
      }
    }]
  }
}
```

## Updating a DataObject from its own data

`recursiveInputIds` allows an output DataObject to be read as input in the same Action. Declare it as output and as `recursiveInputId`, but **not** as input, see [Actions](../actions.md#recursiveinputids):

```
actions {
  accumulate-departures {
    type = CustomDataFrameAction
    inputIds = [stg-departures]
    outputIds = [int-departures]
    recursiveInputIds = [int-departures]
    transformers = [{
      type = SQLDfsTransformer
      code = {
        int-departures = "select * from %{inputViewName_stg-departures} union all select * from %{inputViewName_int-departures}"
      }
    }]
  }
}
```

## Streaming

Defining custom stateful streaming operations with SQL code is not well supported by Spark and can create strange errors or effects. SDLB logs a warning if you combine a streaming [ExecutionMode](../executionModes) with `SQLDfsTransformer`. Use a Scala transformer instead, see [Streaming](../streaming).

## Parameters

The complete list of parameters is available in the [Configuration Schema Viewer](../../../json-schema-viewer). The most relevant ones:

| Parameter | Default | Description |
| --------- | ------- | ----------- |
| `inputIds` | - | Input DataObjects. |
| `outputIds` | - | Output DataObjects. The last transformer must return a result for each of them. |
| `transformers` | `[]` | Many-to-many transformations to apply, in the order of the list. |
| `mainInputId` | - | Input used for ExecutionMode evaluation and partition value propagation. Only needed with multiple inputs. |
| `mainOutputId` | - | Output used for ExecutionMode evaluation and partition value propagation. Only needed with multiple outputs. |
| `recursiveInputIds` | `[]` | Outputs of this Action which are read as input in the same Action. |
| `inputIdsToIgnoreFilter` | `[]` | Inputs which are read completely, ignoring partition values and column filters. |
| `expectations` | `[]` | Data quality expectations evaluated across inputs and outputs of this Action, see [Data Quality](../dataQuality#expectations-on-actions). |
| `cacheInput` / `cacheOutput` | `false` | Cache the DataFrames, useful if an input or output is used more than once. |

## See also

* [Actions overview](../actions.md)
* [Transformations](../transformations) for the available transformer types and how to write custom code
* [Getting Started: Joining it together](../../getting-started/part-1/joining-it-together.md) for a hands-on walkthrough
* [Data Quality](../dataQuality)
