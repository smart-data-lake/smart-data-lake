---
title: Joining It Together
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Goal
So now we have data from departures in our stage layer, and we have cleaned data for airports in our integration layer.
In this step we will finally join both data sources together.
We will continue based on the airports.conf file available [here](https://github.com/smart-data-lake/getting-started/tree/master/config/airports.conf.part-1c-solution).
At the end of the step, we will have all planes departing from Bern Airport in the given timeframe along with their readable destination airport names, as well as geo-coordinates.

Like in the previous step, we need one more action and one DataObject for our output.
For the transformations we will use the btl.conf file:

## Define output object
```
  btl-connected-airports {
    type = CsvFileDataObject
    path = "~{id}"
  }
```

## Define join_departures_airports action
```
  join-departures-airports {
    type = CustomDataFrameAction
    inputIds = [stg-departures, int-airports]
    outputIds = [btl-connected-airports]
    transformers = [{
      type = SQLDfsTransformer
      code = {
        btl-connected-airports = """
          select stg_departures.estdepartureairport, stg_departures.estarrivalairport, airports.*
          from stg_departures join int_airports airports on stg_departures.estArrivalAirport = airports.ident
        """
      }
    }]
    metadata {
      feed = compute
    }
  }
```

Now it gets interesting, a couple of things to note here:
- This time, we changed the Action Type from CopyAction to CustomDataFrameAction.
Use CustomDataFrameAction when you need to do complex operations. For instance, CustomDataFrameAction allows multiple inputs,
which CopyAction does not.
- Our input/output fields are now called inputId**s** and outputId**s** and they take a list of DataObject ids.
Similarly, our transformer is now of type SQLDf**s**Transformer.
Again, the **s** is important, since it shows that multiple inputs/output Data Objects are possible, which is what we need in this step.
In the previous step, we defined a SQLDfTransformer because we only needed one input.
- Finally, the *SQLDfsTransformer* expects its code as a HOCON object rather than as a string. 
This is due to the fact that you could have multiple
outputs, in which case you would need to name them in order to distinguish them.
In our case, there is only one output DataObject: *btl-connected-airports*.
The SQL-Code itself is just a join between the two input Data Objects on the ICAO identifier.
Note that we can just select all columns from airports, since we selected the ones that interest us in the previous step.

:::tip Use only one output
As you can see, with CustomDataFrameAction it's possible to read from multiple inputs and write to multiple outputs.
We usually discourage writing to multiple Data Objects in one action though. 
At some point, you will want to use the metadata from SDLB to analyze your data lineage. If you have a CustomDataFrameAction
with multiple inputs and multiple outputs (an M:N-relationship), SDLB assumes that all outputs depend on all inputs. This might add
some dependencies between DataObjects that don't really exist in the CustomDataFrameAction.
Always using one Data Object as output will make your data lineage more detailed and clear.
:::

## Try it out
You can run the usual *./startJob.sh* command:

```
./startJob.sh -c /mnt/config --feed-sel compute
```

You should now see the resulting files in *data/btl-connected-airports*.
Great! Now we have names and coordinates of destination airports.
We are just missing the coordinates of Bern Airport. 
Let's add them in the next step.
