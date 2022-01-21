---
id: incremental-mode
title: Incremental Mode
---

## Goal
The goal of this part is to use the DataObject's state, such that it can be used in subsequent requests. 
This allows for more dynamic querying of the API. 
For example in the `ext-departures` DataObject we currently query the API with fixed airport, begin and end query parameters. 
Consequently, we will always query the same time period for a given airport.
In a more real-world example, we would want a delta load mechanism that loads any new data since the last execution of the action. 
To demonstrate these incremental queries based on previous state we will start by rewriting our configuration file.

## Define Data Objects
We only make the following minor changes in our config file to Action `download-deduplicate-departures`:
```
download-deduplicate-departures {
  type = DeduplicateAction
  inputId = ext-departures
  outputId = int-departures
  executionMode = { type = DataObjectStateIncrementalMode }
  mergeModeEnable = true
  updateCapturedColumnOnlyWhenChanged = true
  transformers = [{
    type = SQLDfTransformer
    code = "select ext_departures.*, date_format(from_unixtime(firstseen),'yyyyMMdd') dt from ext_departures"
  },{
    type = ScalaCodeDfTransformer
    code = """
      import org.apache.spark.sql.{DataFrame, SparkSession}
      def transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {
        import session.implicits._
        df.dropDuplicates("icao24", "estdepartureairport", "dt")
      }
      // return as function
      transform _
    """
  }]
  metadata {
    feed = deduplicate-departures
  }
}
```
- Adding the executionMode `DataObjectStateIncrementalMode` will enable DataObject incremental mode. With every run the input DataObject will only return new data in this mode.
The DataObject saves its state in the global state file that is written after each run of the Smart Data Lake Builder. You haven't worked with this state file before, more on that later.
- `mergeModeEnable = true` tells DeduplicateAction to merge changed data into the output DataObject, instead of overwriting the whole DataObject. This is especially useful if incoming data is read incrementally.
The output DataObject must implement CanMergeDataFrame interface (also called trait in Scala) for this. DeltaLakeTableDataObject will then create a complex SQL-Upsert statement to merge new and changed data into existing output data.
- By default DeduplicateAction updates column dl_captured in the output for every record it receives. To reduce the number of updated records, `updateCapturedColumnOnlyWhenChanged = true` can be set.
In this case column dl_captured is only updated in the output, when some attribute of the record changed.

:::info
Execution mode is also something that is going to be explained in more detail later.
For now, just think of the execution mode as a way to select which data needs to be processed.
In this case, we tell Smart Data Lake Builder to load data based on the state stored in the DataObject itself.
:::

:::caution
Remember that the time interval in `ext-departures` should not be larger than a week. As mentioned, we will implement a simple incremental query logic that always queries from the last execution time until the current execution.
If the time difference between the last execution and the current execution time is larger than a week, we will query the next four days since the last execution time. Otherwise we query the data from the last execution until now.
:::

## Define state variables
To make use of the newly configured execution mode, we need state variables. Add the following two variables to our CustomWebserviceDataObject.
```scala  
  private var previousState : Seq[State] = Seq()
  private var nextState : Seq[State] = Seq()
```
The corresponding `State` case class is defined as 

```scala
  case class State(airport: String, nextBegin: Long)
```

and should be added in the same file outside the DataObject. For example, add it just below the already existing case classes. 
The state always stores the `airport` and a `nextBegin` as unix timestamp to indicate to the next run, what data needs to be loaded. 

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

## Try it out
We only spoke about this state, but it was never explained where it is stored. 
To work with a state, we need to introduce two new command line parameters: `--state-path` and `--name` or `-n` in short. 
This allows us to define the folder and name of the state file. 
To have access to the state file, we specify the path to be in an already mounted folder.

```
  docker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package
  docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel ids:download-deduplicate-departures --state-path /mnt/data/state -n getting-started
```
Use this slightly modified command to run `download-deduplicate-departures` Action. 
Nothing should have changed so far, since we only read and write an empty state.   
You can verify this by opening the file `getting-started.<runId>.<attemptId>.json` and having a look at the field `dataObjectsState`. The stored state is currently empty. 
In the next section, we will assign a value to `nextState`, such that the `DataObject's` state is written. 

:::info
The same state file is used by Smart Data Lake Builder to enable automatic recoveries of failed jobs.
This will also be explained in detail separately, but we're mentioning the fact here, so you can understand the two variables `<runId>` and `<attemptId>` appearing in the file name.
For each execution, the `<runId>` is incremented by one.
The `<attemptId>` is usually 1, but gets increased by one if Smart Data Lake Builder has to recover a failed execution.
:::


## Define a Query Logic
Now we want to achieve the following query logic:

The starting point are the query parameters provided in the configuration file and no previous state. 
During the first execution, we query the departures for the two airports in the given time window. 
Afterwards, the `end`-parameter of the current query will be stored as `begin`-parameter for the next query.
Now the true incremental phase starts as we can now get the state of the last successful run. 
We query the flight-data API to get data from the last successful run up until now.
For this to work, we need to make two changes. First add the variable
```
private val now = Instant.now.getEpochSecond
``` 
just below the `nextState` variable. Then modify the `currentQueryParameters` variable according to
```scala
// if we have query parameters in the state we will use them from now on
val currentQueryParameters = if (previousState.isEmpty) checkQueryParameters(queryParameters.get) else checkQueryParameters(previousState.map{
  x => DepartureQueryParameters(x.airport, x.nextBegin, now)
})
```
The implemented logic 
```scala
if(previousState.isEmpty){
  nextState = currentQueryParameters.map(params => State(params.airport, params.end))
} else {
  nextState = previousState.map(params => State(params.airport, now))
}
```
for the next state can be placed below the comment `// put simple nextState logic below`. 

Compile and execute the code of this project again and execute it multiple times.
The scenario will be that the first run fetches the data defined in the configuration file, then the proceeding run retrieves the data from the endpoint of the last run until now. 
If this time difference is larger than a week, the program only queries the next four days since the last execution.
If there is no data available in a time window, because only a few seconds have passed since the last execution, the execution will fail with Error **404**.

At the end your config file should look something like [this](../config-examples/application-part3-download-incremental-mode.conf) and the CustomWebserviceDataObject code like [this](../config-examples/CustomWebserviceDataObject-2.scala).

:::info
Unfortunately, the webservice on opensky-network.org responds with a **404** error code when no data is available, rather than a **200** and an empty response. 
Therefore, SDLB gets a 404 and will fail the execution. The exception could be catched inside CustomWebserviceDataObject, but what if we have a real 404 error?!
:::

Congratulation, you just completed implementing a nice incremental loading mechanism!

That's it from getting-started for now. We hope you enjoyed your first steps with SDLB. 
For further informations check the rest of the documentation and the blog on this page!
