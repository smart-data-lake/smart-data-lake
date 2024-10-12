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
Again we start with departures/airports/btl.conf that resulted from finishing the last part.
Use the following cmd to reset your configuration files to the final solution of Part 2:
```
pushd config && cp departures.conf.part-2-solution departures.conf && cp airports.conf.part-2-solution airports.conf && cp btl.conf.part-2-solution btl.conf && popd
pushd envConfig && cp dev.conf.part-2-solution dev.conf && popd  
```

## Define Data Objects
We start by rewriting the existing `ext-departures` DataObject. 
In the configuration file, replace the old configuration with its new definition:
```
  ext-departures {
    type = com.sample.CustomWebserviceDataObject
    baseUrl = "https://opensky-network.org/api/flights/departure"
    responseRowSchema = """icao24 string, firstSeen bigint, estDepartureAirport string, lastSeen bigint, estArrivalAirport string, callsign string, estDepartureAirportHorizDistance bigint, estDepartureAirportVertDistance bigint, estArrivalAirportHorizDistance bigint, estArrivalAirportVertDistance bigint, departureAirportCandidatesCount bigint, arrivalAirportCandidatesCount bigint"""
    nRetry = 1
    queryParameters = [{
      airport = "LSZB"
    },{
      airport = "EDDF"
    }]
    timeouts {
      connectionTimeoutMs = 3000
      readTimeoutMs = 200000
    }
  }
```
  
The Configuration for this new `ext-departures` includes the type of the DataObject, the expected schema, the base url from where we can fetch the departures from, the number of retries, a list of query parameters and timeout options. 
To have more flexibility, we can now configure the query parameters as options instead defining them in the query string.   
The connection timeout corresponds to the time we wait until the connection is established and the read timeout equals the time we wait until the webservice responds after the request has been submitted. 
If the request cannot be answered in the times configured, we try to automatically resend the request. 
How many times a failed request will be resent, is controlled by the `nRetry` parameter.

Note that we changed the type to `com.sampleCustomWebserviceDataObject`.
This is a custom DataObject type, not included in standard Smart Data Lake Builder. 
To make it work, please go to the project's root directory and copy the Scala class with 
```
cp src/main/scala/com/sample/CustomWebserviceDataObject.scala.part-3a-initial src/main/scala/com/sample/CustomWebserviceDataObject.scala
```

This created an initial version of the file `src/main/scala/org/sample/CustomWebserviceDataObject.scala`.

:::info
The *begin* and *end* are now automatically set to two weeks ago minus 2 days and two weeks ago, respectively.
They can still be overridden if you want to try out fixed timestamps. For example, you could also write
```
    {
      airport = "LSZB"
      begin = 1696854853   # 29.08.2021
      end = 1697027653     # 30.08.2021
    }
```

However, as noted previously, when you request older data it may be that the webservice does not respond, so we recommend not to specify begin and end anymore.
Can you spot which line of code in `CustomWebserviceDataObject` is responsible for setting the defaults?
:::
  
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
`FileTransferAction`s are used, when your DataObject reads an InputStream or writes an OutputStream like `WebserviceFileDataObject` or `SFtpFileRefDataObject`. 
These transfer files one-to-one from input to output.  
More often you work with one of the many provided `SparkAction`s like the `CopyAction` shown here. 
They work by using Spark Data Frames under the hood. 
:::

## Try it out
Compile and execute the code of this project with the following commands.
Note that parameter `--feed-sel` only selects `download-departures` as Action for execution. 
```
./buildJob.sh
```

Then
```
./startJob.sh -c /mnt/config,/mnt/envConfig/dev.conf --feed-sel ids:download-departures
```

Nothing should have changed. You should again receive data as json files in the corresponding `stg-departures` folder. 
But except of receiving the departures for only one airport, the DataObject returns the departures for all configured airports. 
In this specific case this would be *LSZB* and *EDDF* within the corresponding time window.

Having a look at the log, something similar should appear on your screen. 

```
2024-09-30 11:35:16 INFO  ActionDAGRun$ActionEventListener - Action~download-departures[CopyAction]: Prepare started [dag-1-19]
2024-09-30 11:35:22 INFO  ActionDAGRun$ActionEventListener - Action~download-departures[CopyAction]: Prepare succeeded [dag-1-19]
2024-09-30 11:35:22 INFO  ActionDAGRun$ActionEventListener - Action~download-departures[CopyAction]: Init started [dag-1-64]
2024-09-30 11:35:23 INFO  CustomWebserviceDataObject - (DataObject~ext-departures) Going to request: https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1726313714&end=1726486514 [init-download-departures]
2024-09-30 11:35:25 INFO  CustomWebserviceDataObject - (DataObject~ext-departures) Success for request https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1726313714&end=1726486514 [init-download-departures]
2024-09-30 11:35:25 INFO  CustomWebserviceDataObject - (DataObject~ext-departures) Going to request: https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1726313714&end=1726486514 [init-download-departures]
2024-09-30 11:35:26 INFO  CustomWebserviceDataObject - (DataObject~ext-departures) Success for request https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1726313714&end=1726486514 [init-download-departures]
2024-09-30 11:35:29 INFO  ActionDAGRun$ActionEventListener - Action~download-departures[CopyAction]: Init succeeded [dag-1-64]
2024-09-30 11:35:29 INFO  ActionDAGRun$ActionEventListener - Action~download-departures[CopyAction]: Exec started [dag-1-70]
2024-09-30 11:35:29 INFO  CopyAction - (Action~download-departures) getting DataFrame for DataObject~ext-departures [exec-download-departures]
2024-09-30 11:35:29 INFO  CustomWebserviceDataObject - (DataObject~ext-departures) Going to request: https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1726313714&end=1726486514 [exec-download-departures]
2024-09-30 11:35:29 INFO  CustomWebserviceDataObject - (DataObject~ext-departures) Success for request https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1726313714&end=1726486514 [exec-download-departures]
2024-09-30 11:35:29 INFO  CustomWebserviceDataObject - (DataObject~ext-departures) Going to request: https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1726313714&end=1726486514 [exec-download-departures]
2024-09-30 11:35:30 INFO  CustomWebserviceDataObject - (DataObject~ext-departures) Success for request https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1726313714&end=1726486514 [exec-download-departures]
2024-09-30 11:35:30 INFO  CopyAction - (Action~download-departures) start writing to DataObject~stg-departures [exec-download-departures]
2024-09-30 11:35:31 INFO  JsonFileDataObject - (DataObject~stg-departures) Writing DataFrame to stg-departures [exec-download-departures]
2024-09-30 11:35:38 INFO  CopyAction - (Action~download-departures) finished writing to stg-departures: job_duration=PT7.625S num_tasks=2 records_written=1384 bytes_written=537152 rows_inserted=1384 stage=save stage_duration=PT1.938S [exec-download-departures]
```

It is important to notice that the two requests for each airport to the API were not send only once, but twice. 
This stems from the fact that the method `getSparkDataFrame` of the Data Object is called twice in the DAG execution of the Smart Data Lake Builder: 
Once during the Init Phase and once again during the Exec Phase. See [Execution Phases](/docs/reference/executionPhases) for more information on that. 
Before we address and mitigate this behaviour in the next section, let's have a look at the `getSparkDataFrame` method and the currently implemented logic:

```Scala
    // use the queryParameters from the config
    val currentQueryParameters = checkQueryParameters(queryParameters)
    
    // given the query parameters, generate all requests
    val departureRequests = currentQueryParameters.map(
      param => s"${baseUrl}?airport=${param.airport}&begin=${param.begin}&end=${param.end}"
    )
    // make requests
    val departuresResponses = departureRequests.map { req =>
      logger.info(s"($id) Going to request: " + req)
      queryWebservice(req)
    }
    // create dataframe with the correct schema and add created_at column with the current timestamp
    val departuresDf = departuresResponses.toDF("response")
      .select(from_json($"response", ArrayType(sparkResponseRowSchema)).as("response"))
      .select(explode($"response").as("record"))
      .select("record.*")
      .withColumn("created_at", current_timestamp())
    // return
    departuresDf
```

Given the configured query parameters, the requests are first prepared using the request method. 
If you have a look at the implementation of the `request` method, you notice that we provide some ScalaJCustomWebserviceClient that is based on the *ScalaJ* library. 
Also in the `request` method you can find the configuration for the number of retries.
Afterward, we create a data frame out of the response. 
We implemented some transformations to flatten the result returned by the API.   
Spark has lots of *Functions* that can be used out of the box. 
We used such a column based function *from_json* to parse the response string with the right schema. 
At the end we return the freshly created data frame `departuresDf`.

## Get Data Frame
In this section we will learn how we can avoid sending the request twice to the API using the execution phase information provided by the Smart Data Lake Builder. 
We will now implement a simple *if ... else* statement that allows us to return an empty data frame with the correct schema in the **Init** phase and to only query the data in the **Exec** phase. 
This logic is implemented in the next code snipped and should replace the code currently enclosed between the two `// REPLACE BLOCK` comments.
```Scala
    if(context.phase == ExecutionPhase.Init){
      // simply return an empty data frame
      Seq[String]().toDF("response")
        .select(from_json($"response", ArrayType(sparkResponseRowSchema)).as("response"))
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
      val departuresResponses = departureRequests.map { req =>
        logger.info(s"($id) Going to request: " + req)
        queryWebservice(req)
      }
      // create dataframe with the correct schema and add created_at column with the current timestamp
      val departuresDf = departuresResponses.toDF("response")
        .select(from_json($"response", ArrayType(sparkResponseRowSchema)).as("response"))
        .select(explode($"response").as("record"))
        .select("record.*")
        .withColumn("created_at", current_timestamp())
  
      // put simple nextState logic below
    
      // return
      departuresDf
    }
```
Note, in the *Init* phase, the pre-defined **schema** (see the `ext-departures` DataObject definition in the config) is used to create an empty/dummy json string, which then gets converted to the DataFrame.

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

Now you can start another job:
```
./startJob.sh -c /mnt/config,/mnt/envConfig/dev.conf --feed-sel ids:download-deduplicate-departures
```

Uff, getting error `SchemaViolationException: (DataObject~int-departures) Schema does not match schema defined on write:`?
The error message gives detailed information about columns that are missing or superfluous (comparison includes type):

    - missingCols=
    - superfluousCols=created_at

We have created an additional column `created_at`, which doesn't yet exist in the current table.
SDLB supports schema evolution for DataObjects like DeltaLakeTableDataObject.
To enable it add `allowSchemaEvolution = true` to `int-departures` DataObject definition.

## Summary

At the end, your departures.conf file should look something like [this](https://github.com/smart-data-lake/getting-started/tree/master/config/departures.conf.part-3a-solution)
and the CustomWebserviceDataObject code like [this](https://github.com/smart-data-lake/getting-started/tree/master/src/main/scala/com/sample/CustomWebserviceDataObject.scala.part-3a-solution).

Great work, in the next step we are going to implement incremental loads.
