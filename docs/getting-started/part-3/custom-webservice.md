---
id: custom-webservice
title: Custom Webservice
---

## Goal
  In the previous examples we worked mainly with data that was available as a file or could be fetched with the built-in `WebserviceFileDataObject`.
  To fetch data from a webservice, the `WebserviceFileDataObject` is sometimes not enough and has to be customized. 
  The reasons why the built-in DataObject is not sufficient are manifold, but it's connected to the way Webservices are designed. 
  Webservices often include design features like: 
* data pagination
* protect resources using rate limiting 
* different authentication mechanisms
* filters for incremental load
* well defined schema
* ...

Smart Data Lake Builder can not cover all these various needs in a generic `WebserviceDataObject`, which is why we have to write our own `CustomWebserviceDataObject`. 
The goal of this part is to learn how such a CustomWebserviceDataObject can be implemented in Scala.

:::info
Other than part 1 and 2, we are writing customized Scala classes in part 3 and making use of Apache Spark features.
As such, we expect you to have some Scala and Spark know-how to follow along.

It is also a good idea to configure a working development environment at this point. 
In the [Technical Setup](../setup.md) chapter we briefly introduced how to use IntelliJ for development.
That should greatly improve your development experience compared to manipulating the file in a simple text editor.
:::

## Starting point
Again we start with the `application.conf` that resulted from finishing the last part. 
If you don't have the application.conf from part 2 anymore, please copy [this](../config-examples/application-part2-historical.conf) configuration file to **config/application.conf** again.

## Define Data Objects
We start by rewriting the existing `ext-departures` DataObject. 
In the configuration file, replace the old configuration with its new definition:
```
ext-departures {
  type = CustomWebserviceDataObject
  schema = """array< struct< icao24: string, firstSeen: integer, estDepartureAirport: string, lastSeen: integer, estArrivalAirport: string, callsign: string, estDepartureAirportHorizDistance: integer, estDepartureAirportVertDistance: integer, estArrivalAirportHorizDistance: integer, estArrivalAirportVertDistance: integer, departureAirportCandidatesCount: integer, arrivalAirportCandidatesCount: integer >>"""
  baseUrl = "https://opensky-network.org/api/flights/departure"
  nRetry = 5
  queryParameters = [{
    airport = "LSZB"
    begin = 1641393602
    end = 1641483739
  },{
    airport = "EDDF"
    begin = 1641393602
    end = 1641483739
  }]
  timeouts {
    connectionTimeoutMs = 200000
    readTimeoutMs = 200000
  }
}
```
  
The Configuration for this new `ext-departures` includes the type of the DataObject, the expected schema, the base url from where we can fetch the departures from, the number of retries, a list of query parameters and timeout options. 
To have more flexibility, we can now configure the query parameters as options instead defining them in the query string.   
The connection timeout corresponds to the time we wait until the connection is established and the read timeout equals the time we wait until the webservice responds after the request has been submitted. 
If the request cannot be answered in the times configured, we try to automatically resend the request. 
How many times a failed request will be resend, is controlled by the `nRetry` parameter.

:::info
The *begin* and *end* can now be configured for each airport separatly. 
The configuration expects unix timestamps, if you don't know what that means, have a look at this [website](https://www.unixtimestamp.com/).
The webservice will not respond if the interval is larger than a week.
Hence, we enforce the rule that if the chosen interval is larger, we query only the next four days given the *begin* configuration. T
:::

Note that we changed the type to `CustomWebserviceDataObject`.
This is a custom DataObject type, not included in standard Smart Data Lake Builder. 
To make it work please go to the project's root directory and **unzip part3.additional-files.zip** into the project's root folder.
It includes the following file for you:

  - ./src/scala/io/smartdatalake/workflow/dataobject/CustomWebserviceDataObject.scala
  
In this part we will work exclusively on the `CustomWebserviceDataObject.scala` file.

## Define Action
In the configuration we only change one action again:
```
download-departures {
  type = CopyAction
  inputId = ext-departures
  outputId = stg-departures
  metadata {
    feed = download
  }
}
```
The type is no longer `FileTransferAction` but a `CopyAction` instead, as our new `CustomWebserviceDataObject` converts the Json-Output of the Webservice into a Spark DataFrame.

:::info
`FileTransferAction`s are used, when your DataObjects reads an InputStream or writes an OutputStream like `WebserviceFileDataObject` or `SFtpFileRefDataObject`. 
These transfer files one-to-one from input to output.  
More often you work with one of the many provided `SparkAction`s like the `CopyAction` shown here. 
They work by using Spark Data Frames under the hood. 
:::

## Try it out
Compile and execute the code of this project with the following commands.
Note that parameter `--feed-sel` only selects `download-departures` as Action for execution. 
```
  mkdir .mvnrepo
  docker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package
  docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel ids:download-departures
```

Nothing should have changed. You should again receive data as json files in the corresponding `stg-departures` folder. 
But except of receiving the departures for only one airport, the DataObject returns the departures for all configured airports. 
In this specific case this would be *LSZB* and *EDDF* within the corresponding time window.

Having a look at the log, something similar should appear on your screen. 
```
2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Prepare started
2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:237 - Action~download-departures[CopyAction]: Prepare succeeded
2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Init started
2021-11-10 14:00:33 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1630200800&end=1630310979
2021-11-10 14:00:33 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1630200800&end=1630310979
2021-11-10 14:00:35 INFO  ActionDAGRun$ActionEventListener:237 - Action~download-departures[CopyAction]: Init succeeded
2021-11-10 14:00:35 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Exec started
2021-11-10 14:00:35 INFO  CopyAction:158 - (Action~download-departures) getting DataFrame for DataObject~ext-departures
2021-11-10 14:00:36 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1630200800&end=1630310979
2021-11-10 14:00:37 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1630200800&end=1630310979
```
It is important to notice that the two requests for each airport to the API were not send only once, but twice. 
This stems from the fact that the method `getDataFrame` of the Data Object is called twice in the DAG execution of the Smart Data Lake Builder: 
Once during the Init Phase and once again during the Exec Phase. See [this page](/docs/reference/executionPhases) for more information on that. 
Before we address and mitigate this behaviour in the next section, let's have a look at the `getDataFrame` method and the currently implemented logic:
```scala
// use the queryParameters from the config
val currentQueryParameters = checkQueryParameters(queryParameters)

// given the query parameters, generate all requests
val departureRequests = currentQueryParameters.map(
  param => s"${baseUrl}?airport=${param.airport}&begin=${param.begin}&end=${param.end}"
)
// make requests
val departuresResponses = departureRequests.map(request(_))
// create dataframe with the correct schema and add created_at column with the current timestamp
val departuresDf = departuresResponses.toDF("responseBinary")
  .withColumn("responseString", byte2String($"responseBinary"))
  .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))
  .select(explode($"response").as("record"))
  .select("record.*")
  .withColumn("created_at", current_timestamp())
// return
departuresDf
```
Given the configured query parameters, the requests are first prepared using the request method. 
If you have a look at the implementation of the `request` method, you notice that we provide some ScalaJCustomWebserviceClient that is based on the *ScalaJ* library. 
Also in the `request` method you can find the configuration for the number of retries.
Afterwards, we create a data frame out of the response. 
We implemented some transformations to flatten the result returned by the API.   
Spark has lots of *Functions* that can be used out of the box. 
We used such a column based function *from_json* to parse the response string with the right schema. 
At the end we return the freshly created data frame `departuresDf`.

:::tip
The return type of the response is `Array[Byte]`. To convert that to `Array[String]` a *User Defined Function* (also called *UDF*) `byte2String` has been used, which is declared inside the getDataFrame method.
This function is a nice example of how to write your own *UDF*.
:::

## Get Data Frame
In this section we will learn how we can avoid sending the request twice to the API using the execution phase information provided by the Smart Data Lake Builder. 
We will now implement a simple *if ... else* statement that allows us to return an empty data frame with the correct schema in the **Init** phase and to only query the data in the **Exec** phase. 
This logic is implemented in the next code snipped and should replace the code currently enclosed between the two `// REPLACE BLOCK` comments.
```scala
    if(context.phase == ExecutionPhase.Init){
  // simply return an empty data frame
  Seq[String]().toDF("responseString")
          .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))
          .select(explode($"response").as("record"))
          .select("record.*")
          .withColumn("created_at", current_timestamp())
} else {
  // use the queryParameters from the config
  val currentQueryParameters = checkQueryParameters(queryParameters)

  // given the query parameters, generate all requests
  val departureRequests = currentQueryParameters.map(
    param => s"${baseUrl}?airport=${param.airport}&begin=${param.begin}&end=${param.end}"
  )
  // make requests
  val departuresResponses = departureRequests.map(request(_))
  // create dataframe with the correct schema and add created_at column with the current timestamp
  val departuresDf = departuresResponses.toDF("responseBinary")
          .withColumn("responseString", byte2String($"responseBinary"))
          .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))
          .select(explode($"response").as("record"))
          .select("record.*")
          .withColumn("created_at", current_timestamp())

  // put simple nextState logic below

  // return
  departuresDf
}

```
Note, in the *Init* phase, the pre-defined **schema** (see the `ext-departures` dataObject definition in the config) is used to create an empty/dummy json string, which then gets converted to the DataFrame.

Don't be confused about some comments in the code. They will be used in the next chapter. 
If you re-compile the code of this project and then restart the program with the previous commands
you should see that we do not query the API twice anymore.

:::tip
Use the information of the `ExecutionPhase` in your custom implementations whenever you need to have different logic during the different phases.
:::

## Preserve schema

With this implementation, we still write the Spark data frame of our `CustomWebserviceDataObject` in Json format. 
As a consequence, we lose the schema definition when the data is read again.   
To improve this behaviour, let's directly use the `ext-departures` as *inputId* in the `deduplicate-departures` action, and rename the Action as `download-deduplicate-departures`.
The deduplicate action expects a DataFrame as input. Since our `CustomWebserviceDataObject` delivers that, there is no need for an intermediate step anymore.  
After you've changed that, the first transformer has to be rewritten as well, since the input has changed. 
Please replace it with the implementation below
```
{
  type = SQLDfTransformer
  code = "select ext_departures.*, date_format(from_unixtime(firstseen),'yyyyMMdd') dt from ext_departures"
}
```
The old Action `download-departures` and the DataObject `stg-departures` can be deleted, as it's not needed anymore.

At the end, your config file should look something like [this](../config-examples/application-part3-download-custom-webservice.conf) and the CustomWebserviceDataObject code like [this](../config-examples/CustomWebserviceDataObject-1.scala).
Note that since we changed the file format to delta lake, your new `download-deduplicate-departures` feed now needs the metastore that you setup in part 2.
Therefore, you need to make sure that polynote and the metastore are running as shown in [the first step of part 2](../part-2/delta-lake-format).
Then, you need to delete the files in the data folder and then run the following command in another terminal: 

    mkdir -f data
    docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network getting-started_default sdl-spark:latest --config /mnt/config --feed-sel ids:download-deduplicate-departures


