---
id: incremental-mode
title: Incremental Mode
---

## Goal
The goal of this part is to use a DataObject with state, which can be used in subsequent requests. 
This allows for more dynamic querying of the API. 
For example in the `ext-departures` DataObject we currently query the API with fixed airport, begin and end query parameters. 
Consequently, we will always query the same time period for a given airport.
In a more real-world example, we would want a delta load mechanism that loads any new data since the last execution of the action. 
To demonstrate these incremental queries based on previous state we will start by rewriting our configuration file.

## Adapt Action
We only have to make a minor change in `departures.conf` file to Action `download-deduplicate-departures`.
Adding the executionMode `DataObjectStateIncrementalMode` will enable DataObject incremental mode. With every run the input DataObject will only return new data in this mode.
The DataObject saves its state in the state file that is written after each run of the SDLB. You haven't worked with this state file before, more on that later.
```
  download-deduplicate-departures {
    type = DeduplicateAction
    inputId = ext-departures
    outputId = int-departures
    executionMode = { type = DataObjectStateIncrementalMode }
    ...
  }
```

:::info Execution Modes
An Execution Mode is a way to select which data needs to be processed.
With DataObjectStateIncrementalMode, we tell SDLB to load data based on the state managed by the DataObject itself.
See [Execution Modes](/docs/reference/executionModes) for detailed documentation.
:::

:::caution
Due to load limits of the departures web service, the time interval in `ext-departures` should not be larger than a week. As mentioned, we will implement a simple incremental query logic that always queries from the last execution time until the current execution.
If the time difference between the last execution and the current execution time is larger than a week, we will query the next four days since the last execution time. Otherwise, we query the data from the last execution until now.
:::

## Define state variables
To make use of the newly configured execution mode, we need state variables. Add the following two variables to our CustomWebserviceDataObject.
```scala  
  private var previousState : Seq[State] = Seq()
  private var nextState : Seq[State] = Seq()
```

The corresponding `State` case class is already defined as
```scala
  case class State(airport: String, nextBegin: Long)
```
It always stores the `airport` and a `nextBegin` as unix timestamp to indicate to the next run, what data needs to be loaded. 

Concerning the state variables, `previousState` will basically be used for all the logic of the DataObject and `nextState` will be used to store the state for the next run.

## Read and write state
To actually work with the state, we need to implement the `CanCreateIncrementalOutput` trait. 
This can be done by adding `with CanCreateIncrementalOutput` to the `CustomWebserviceDataObject`. 
Consequently, we need to implement the functions `setState` and `getState` defined in the trait. 
```scala
  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {
    implicit val formats: Formats = DefaultFormats
    previousState = state.map(s => JsonMethods.parse(s).extract[Seq[State]]).getOrElse(Seq())
  }
  
  override def getState: Option[String] = {
    implicit val formats: Formats = DefaultFormats
    Some(Serialization.write(nextState))
  }
```

We can see that by implementing these two functions, we start using the variables defined in the section above.

To compile the following additional import statements are necessary at the top of the file:
```scala
import io.smartdatalake.workflow.dataobject.CanCreateIncrementalOutput
import org.json4s.jackson.{JsonMethods, Serialization}
```

## Try it out
We only spoke about this state, but it was never explained where it is stored. 
To work with a state, we need to introduce two new command line parameters: `--state-path` and `--name` or `-n` in short. 
This allows us to define the folder and name of the state file. 
To have access to the state file, we specify the path to be in an already mounted folder.

```
./buildJob.sh
./startJob.sh -c /mnt/config,/mnt/envConfig/dev.conf --feed-sel ids:download-deduplicate-departures --state-path /mnt/data/state -n getting-started
```

Use this slightly modified command to run `download-deduplicate-departures` Action. 
Nothing should have changed so far, since we only read and write an empty state.   
You can verify this by opening the file `getting-started.<runId>.<attemptId>.json` and having a look at the field `dataObjectsState`. The stored state is currently empty. 
In the next section, we will assign a value to `nextState`, such that the `DataObject's` state is written. 

:::info
The same state file is used by SDLB to enable automatic recoveries of failed jobs.
This will also be explained in detail separately, but we're mentioning the fact here, so you can understand the two variables `<runId>` and `<attemptId>` appearing in the file name.
For each execution, the `<runId>` is incremented by one.
The `<attemptId>` is usually 1, but gets increased by one if SDLB has to recover a failed execution.
:::


## Define a Query Logic
Now we want to achieve the following query logic:

The starting point are the query parameters provided in the configuration file and no previous state. 
During the first execution, we query the departures for the two airports in the given time window.
If no begin and end time are provided, we take the interval of [2 weeks and 2 days ago] -> [2 weeks ago] as a starting point.
Afterward, the `end`-parameter of the current query will be stored as `begin`-parameter for the next query.
Now the true incremental phase starts as we can get the state of the last successful run. 
We query the flight-data API to get data from the last successful run up until now.

For this to work, we need to make the following change to the `currentQueryParameters` variable:
```scala
      // if we have query parameters in the state, we will use them from now on
      val currentQueryParameters = if (previousState.isEmpty) checkQueryParameters(queryParameters) else checkQueryParameters(previousState.map{
        x => DepartureQueryParameters(x.airport, x.nextBegin, java.time.Instant.now.getEpochSecond)
      })
```

Then the logic for the next state must be placed below the comment `// put simple nextState logic below`.
```scala
      nextState = currentQueryParameters.map(params => State(params.airport, params.end))
```

Compile and execute the code of this project again and execute it multiple times.
The scenario will be that the first run fetches the data defined in the configuration file, then the proceeding run retrieves the data from the endpoint of the last run until now. 
If this time difference is larger than a week, the program only queries the next four days since the last execution.
If there is no data available in a time window, because only a few seconds have passed since the last execution, the execution will fail with Error **404**.

At the end the departure.conf file should look something like [this](https://github.com/smart-data-lake/getting-started/tree/master/config/departures.conf.part-3-solution)
and the CustomWebserviceDataObject code like [this](https://github.com/smart-data-lake/getting-started/tree/master/src/main/scala/com/sample/CustomWebserviceDataObject.scala.part-3-solution).

:::info
Unfortunately, the webservice on opensky-network.org responds with a **404** error code when no data is available, rather than a **200** and an empty response. 
Therefore, SDLB gets a 404 and will fail the execution. The exception could be caught inside CustomWebserviceDataObject, but what if we have a real 404 error?!
:::

## Summary

Congratulation, you just completed implementing a nice incremental loading mechanism!
At the end, your departures.conf file should look something like [this](https://github.com/smart-data-lake/getting-started/tree/master/config/departures.conf.part-3b-solution)

In the next step we are going to have a look at descriptive metadata...