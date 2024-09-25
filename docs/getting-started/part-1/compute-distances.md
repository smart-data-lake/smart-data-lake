---
title: Compute Distances
---

## Goal

In this part, we will compute the distances between departure and arrival airports
so that our railway enthusiast Tom can see which planes could be replaced by rail traffic.


## Define output object
Let's define our final output object for this part in btl.conf file:

```
  btl-distances {
    type = CsvFileDataObject
    path = "~{id}"
  }
```


## Define compute_distances action

How do we compute the distances ? 
The answer is in the file [src/main/scala/com/sample/ComputeDistanceTransformer.scala](https://github.com/smart-data-lake/getting-started/blob/master/src/main/scala/com/sample/ComputeDistanceTransformer.scala).

So far, we only used SQL transformers. 
But more complex transformations can be written in custom code and referenced in the config.
This gives you great flexibility for cases with specific business logic, such as this one.

We have one input and one output: therefore our custom class is a *CustomDfTransformer* (instead of CustomDf**s**Transformer).
It takes the input dataframe *df* and calls a User Defined Function called *calculateDistanceInKilometerUdf*
to do the computation.
It expects the column names dep_latitude_deg, dep_longitude_deg, arr_latitude_deg and arr_longitude_deg in the input.
This matches the column names we used in the SQL-Code in the last-step.

We won't go into the details of the Udf. 
You can follow this [stackoverflow link](https://stackoverflow.com/questions/27928/calculate-distance-between-two-latitude-longitude-points-haversine-formula) if you want to learn more.

Finally, the transformation writes the result into a column called distance.
It also adds a column *could_be_done_by_rail* to the output that simply checks if the distance is smaller than 500 km.

In order to wire this CustomTransformation into our config, we add the following action:

```
  compute-distances {
    type = CopyAction
    inputId = btl-departures-arrivals-airports
    outputId = btl-distances
    transformers = [{
      type = ScalaClassSparkDfTransformer
      className = com.sample.ComputeDistanceTransformer
    }]
    metadata {
      feed = compute
    }
  }
```

We used a CopyAction and told it to execute the code in the class *com.sample.ComputeDistanceTransformer* to transform the data.
We could also have used a CustomDataFrameAction like in the previous step, 
but this would have resulted in more complex code working with lists of inputs, outputs and transformers.


## Try it out

[This](https://github.com/smart-data-lake/getting-started/tree/master/config/btl.conf.part-1-solution) is how the final btl.conf file should look like.

To use the Java Code in our sdl-spark docker image, we have to compile it. 
You have already done this in the [setup](../setup.md), but lets review this step again. It can be done by using a maven docker image as follows.

```
mkdir .mvnrepo
./buildJob.sh
```

or you can also use maven directly if you have Java SDK and Maven installed

```
mvn package
```

This creates a jar-file ./target/getting-started-1.0.jar containing the compiled Scala classes.
The *./startJob.sh* includes a parameter to mount ./target into the docker image, which makes this jar-file accessible to SDLB.

Now you can start SDLB again:

```
./startJob.sh -c /mnt/config --feed-sel compute
```

Under *data/btl-distances* you can now see the final result. 

### The Execution DAG of the compute feed

In the console, you probably started noticing some pretty ASCII Art that looks like this:
```
                         ┌─────┐
                         │start│
                         └┬──┬─┘
                          │  │
                          │  └───────────────────┐
                          v                      │
     ┌─────────────────────────────────────────┐ │
     │select-airport-cols SUCCEEDED PT4.421793S│ │
     └───────────────┬─────────────────────────┘ │
                     │                           │
                     v                           v
     ┌──────────────────────────────────────────────┐
     │join_departures_airports SUCCEEDED PT2.938995S│
     └──────────────────────┬───────────────────────┘
                            │
                            v
       ┌────────────────────────────────────────┐
       │compute_distances SUCCEEDED PT1.160045S│
       └────────────────────────────────────────┘
```
This is the Execution *DAG* of our data pipeline. 
SDLB internally builds a Directed Acyclic Graph (DAG) to analyze all dependencies between your actions. 
Because it's acyclic, it cannot have loops, so it's impossible to define an action that depends on an output of subsequent actions.
SDLB uses this DAG to optimize execution of your pipeline as it knows in which order the pipeline needs to execute.

What you see in the logs, is a representation of what the SDLB has determined as dependencies.
If you don't get the results you expect, it's good to check if the DAG looks correct.
At the end you will also see the graph again with status indicators (SUCCEEDED, CANCELLED, ...) and the duration it took to execute the action.

:::tip Hold on
It's worth pausing at this point to appreciate this fact:  
You didn't have to explicitly tell SDLB how to execute your actions.
You also didn't have to define the dependencies between your actions.
Just from the definitions of DataObjects and Actions alone, SDLB builds a DAG and knows what needs to be executed and how.
:::


### The Execution DAG of the .* feed

You can also execute the entire data pipeline by selecting all feeds:

```
./startJob.sh -c /mnt/config --feed-sel '.*'
```

The successful execution DAG looks like this
```
                                            ┌─────┐
                                            │start│
                                            └─┬─┬─┘
                                              │ │
                                              │ └─────────────────────┐
                                              v                       │
                   ┌───────────────────────────────────────┐          │
                   │download-airports SUCCEEDED PT8.662886S│          │
                   └──────┬────────────────────────────────┘          │
                          │                                           │
                          v                                           v
     ┌─────────────────────────────────────────┐ ┌─────────────────────────────────────────┐
     │select-airport-cols SUCCEEDED PT4.629163S│ │download-departures SUCCEEDED PT0.564972S│
     └─────────────────────────────────────────┘ └─────────────────────────────────────────┘                                      
                                        v               v
                        ┌──────────────────────────────────────────────┐
                        │join_departures_airports SUCCEEDED PT4.453514S│
                        └──────────────────────┬───────────────────────┘
                                               │
                                               v
                           ┌───────────────────────────────────────┐
                           │compute_distances SUCCEEDED PT1.404391S│
                           └───────────────────────────────────────┘
```
SDLB was able to determine that download-airports and download-departures can be executed in parallel,
independently from each other. It's only at join-departures-airports and beyond that both Data Sources are needed.

SDLB has command-line option called --parallelism which allows you to set the degree of parallelism used when executing the DAG run.
Per default, it's set to 1.


### The Execution DAG of the failed .* feed

If you set the option *readTimeoutMs=0* in the DataObject *ext-departures*  it's possible that the *download-departures* action fails.
That's because SDLB abandons the download because the REST-Service is too slow to respond.
If that happens, the DAG will look like this:
```
    21/09/14 09:05:36 ERROR ActionDAGRun: Exec: TaskFailedException: Task download-departures failed. Root cause is 'WebserviceException: connect timed out': WebserviceException: connect timed out
    21/09/14 09:05:36 INFO ActionDAGRun$: Exec FAILED for .* runId=1 attemptId=1:
                                           ┌─────┐
                                           │start│
                                           └─┬─┬─┘
                                             │ │
                                             │ └─────────────────────┐
                                             v                       │
                  ┌───────────────────────────────────────┐          │
                  │download-airports SUCCEEDED PT6.638844S│          │
                  └───────┬───────────────────────────────┘          │
                          │                                          │
                          v                                          v
     ┌─────────────────────────────────────────┐ ┌──────────────────────────────────────┐
     │select-airport-cols SUCCEEDED PT3.064118S│ │download-departures FAILED PT1.084153S│
     └──────────────────────────────────┬──────┘ └──┬───────────────────────────────────┘
                                        │           │
                                        v           v
                            ┌──────────────────────────────────┐
                            │join-departures-airports CANCELLED│
                            └─────────────────┬────────────────┘
                                              │
                                              v
                                ┌───────────────────────────┐
                                │compute-distances CANCELLED│
                                └───────────────────────────┘
```
As you can see, in this example, the action *download-departures* failed because of a timeout.
Because SDLB determined that both downloads are independent, it was able to complete *select-airport-cols* successfully.

**Congratulations!**  
You successfully recreated the configuration files that were executed when you ran SDLB in the first step.
