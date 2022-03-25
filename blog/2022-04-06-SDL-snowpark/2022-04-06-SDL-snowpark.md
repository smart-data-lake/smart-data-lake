---
title: Combine Spark and Snowpark to ingest and transform data in one pipeline
description: An example to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake.
slug: sdl-snowpark
authors:
  - name: Zach Kull
    title: Data Expert
    url: https://www.linkedin.com/in/zacharias-kull-94705886/
tags: [Snowpark, Snowflake]
hide_table_of_contents: false
---

This article shows how to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake.

<!--truncate-->

Recent developments in Smart Data Lake Builder (SDLB) included refactorings to integrate alternative execution engines to Spark.
In particular [Snowpark](https://docs.snowflake.com/en/developer-guide/snowpark/index.html) integration was implemented, a Spark like DataFrame API for implementing transformations in Snowflake.

Implementing transformations in Snowflake has big performance and cost benefits. And using a DataFrame API is much more powerful than coding in SQL, see also [Modern Data Stack: Which Place for Spark?](https://medium.com/towards-data-science/modern-data-stack-which-place-for-spark-8e10365a8772).

Snowpark is good for transforming data inside Snowflake, but not all data might be located in Snowflake and suitable for Snowflake. 
Here it is interesting to use Spark and its many connectors, in particular to ingest and export data.

Combining Spark and Snowpark in a smart data pipeline using a DataFrame API would be the ideal solution.
With the integration of Snowpark as engine in SDLB we created just that. 

This blog post will show how to migrate our example data pipeline of the [Getting Started](getting-started/setup.md) guide Part 1 to use Spark for ingestion and Snowpark for transformation.

## Prerequisits

* Create a Snowflake trial account on https://signup.snowflake.com/ and note the following connection informations:
  * Account URL (copy by navigating to "Organization" and clicking the link symbol on the right of the account name)
  * Username
  * Password
* Create database "testdb" in Snowflake: `create database testdb;`
* Create schema "testdb.test" in Snowflake: `create schema testdb.test;`
* Setup running SDLB docker image with part-1 configuration as described in [Getting Started](getting-started/setup.md)
  * build sdl-spark image
  * copy final application.conf of part-1: `cp config/application.conf.part-1-solution config/application.conf`
  * run download actions with parameter `--feed-sel download`
  * run compute actions with parameter `--feed-sel compute`

## Goal

The example of part-1 has the following DataObjects

Staging Layer
* stg-departures: JsonFileDataObject
* stg-airports: CsvFileDataObject

Integration Layer
* int-airports: CsvFileDataObject

Business Transformation Layer
* btl-departures-arrivals-airports: CsvFileDataObject
* btl-distances: CsvFileDataObject

In this example we will migrate Integration and Business Transformation Layer to Snowflake.
We will use Spark to fill Staging and Integration Layer, and Snowpark for transformation from Integration to Business Transformation Layer.

## Prepare the Snowflake library

First we have add SDLBs Snowflake library to the projects pom.xml dependencies section:

      <dependencies>
        ....
        <dependency>
          <groupId>io.smartdatalake</groupId>
          <artifactId>sdl-snowflake_${scala.minor.version}</artifactId>
          <version>${project.parent.version}</version>
        </dependency>
        ...
      </dependencies>

Then SDLB version needs to be updated to version 2.3.0-SNAPSHOT at least in the parent section:

      <parent>
        <groupId>io.smartdatalake</groupId>
        <artifactId>sdl-parent</artifactId>
        <version>2.3.0-SNAPSHOT</version>
      </parent>

## Define Snowflake connection

To define the Snowflake connection in config/application.conf, add connections section with connection "sf-con", and fill in informations according to prerequisits:

      connections {
        sf-con {
          type = SnowflakeTableConnection
          url = "<accountUrl>",
          warehouse = "COMPUTE_WH",
          database = "testdb",
          role = "ACCOUNTADMIN",
          authMode = {
            type = BasicAuthMode
            userVariable = "CLEAR#<username>"
            passwordVariable = "CLEAR#<pwd>"
        }
      }

## Migrate DataObjects

Now we can change the DataObject type to SnowflakeTableDataObject and the new Snowflake connection, adding the definition of the table:

      int-airports {
        type = SnowflakeTableDataObject
        connectionId = sf-con
        table {
          db = "test"
          name = "int_airports"
        }
      }
    
      btl-departures-arrivals-airports {
        type = SnowflakeTableDataObject
        connectionId = sf-con
        table {
          db = "test"
          name = "btl_departures_arrivals_airports"
        }
      }
    
      btl-distances {
        type = SnowflakeTableDataObject
        connectionId = sf-con
        table {
          db = "test"
          name = "btl_distances"
        }
      }

Note that the attribute `db` of the SnowflakeTableDataObject should be filled with the schema of the Snowflake table and that this is *not* the same as the attribute `database` of SnowflakeTableConnection. 

## Migrating Actions

The new SDLB version introduced some naming changes:
- The CustomSparkAction can now also process Snowpark-DataFrames and is therefore renamed to CustomDataFrameAction.
- The ScalaClassDfTransformer was specific for Spark. In the new SDLB version there is a specific scala-class DataFrame transformer for Spark and Snowpark, e.g. ScalaClassSparkDfTransformer and ScalaClassSnowparkDfTransformer. And there is even a ScalaClassGenericDfTransformer to implement transformations using a unified API. In our case we will migrate the transformation to use Snowpark and set the type to ScalaClassSnowparkDfTransformer.
- See [Architecture](architecture.md) chapter "breaking changes" for other renamings.

      join-departures-airports {
        type = CustomSparkAction -> CustomDataFrameAction
        ...

      compute-distances {
        ...
        transformers = [{
          type = ScalaClassDfTransformer -> ScalaClassSnowparkDfTransformer

There is no need to change the SQL transformtions of join-departures-airport, as the SQL should run on Snowpark aswell.

On the other hand the ComputeDistanceTransformer was implemented with the Spark DataFrame API. We need to migrate it to Snowpark DataFrame API to run this Action with Snowpark. Luckily the API's are very similar. Often it's sufficient to change the import statement, the class we're extending and the session parameters type:

      import com.snowflake.snowpark.functions._
      import com.snowflake.snowpark.{DataFrame, Session}
      import io.smartdatalake.workflow.action.snowflake.customlogic.CustomSnowparkDfTransformer

      class ComputeDistanceTransformer extends CustomSnowparkDfTransformer {
        def transform(session: Session, options: Map[String, String], df: DataFrame, dataObjectId: String) : DataFrame = {
          ...

If you have UDFs in your code, it gets trickier. The UDF Code gets serialized to Snowflake, details see [Snowpark UDFs](https://docs.snowflake.com/de/developer-guide/snowpark/scala/creating-udfs.html). Special care must be taken to minimize the scope the UDF is defined in. Thats why we move the function into the companion object.

      object ComputeDistanceTransformer {
        def calculateDistanceInKilometer(depLat: Double, depLng: Double, arrLat: Double, arrLng: Double): Double = {
          val AVERAGE_RADIUS_OF_EARTH_KM = 6371
          val latDistance = Math.toRadians(depLat - arrLat)
          val lngDistance = Math.toRadians(depLng - arrLng)
          val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(depLat)) * Math.cos(Math.toRadians(arrLat)) * Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2)
          val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
          AVERAGE_RADIUS_OF_EARTH_KM * c
        }
        def getCalculateDistanceInKilometerUdf(session: Session) = {
          // using only udf(...) function results in "SnowparkClientException: Error Code: 0207, Error message: No default Session found. Use <session>.udf.registerTemporary() to explicitly refer to a session."
          session.udf.registerTemporary(ComputeDistanceTransformer.calculateDistanceInKilometer _)
        }
      }

Note that we need to pass the Session to a function for registering the UDF. There is an Error 0207 if we use "udf" function (at least in snowpark version 1.2.0).
Finally we need to adapt the call of the UDF as follows:

    df.withColumn("distance", ComputeDistanceTransformer.getCalculateDistanceInKilometerUdf(session)(col("dep_latitude_deg"),col("dep_longitude_deg"),col("arr_latitude_deg"), col("arr_longitude_deg")))

## Compile and run

Time to see if it works.
Lets build an update SDLB docker image with the updated SDLB version:

      podman build -t sdl-spark .

Then compile the code with the UDF:

      mkdir .mvnrepo
      podman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package

Download initial data with `--feed-sel download`:

      podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download

Compute with `--feed-sel compute`:

      podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel compute

If the SDLB run was SUCCESSFUL, you should now see TEST.BTL_DISTANCES table in Snowpark.
To check that Spark was used for Action select-airport-cols and Snowpark for Action compute-distances, look out for the following logs, e.g. SnowparkSubFeed for Action~compute-distances: 

      INFO  CopyAction - (Action~compute-distances) selected subFeedType SnowparkSubFeed [init-compute-distances]

# Engine selection - uncover the magic

Browsing through the logs it turns out that the Action~join-departures-airports was still executed with Spark (SparkSubFeed)!

      INFO  CustomDataFrameAction - (Action~join-departures-airports) selected subFeedType SparkSubFeed [init-join-departures-airports]

An Action determines the engine to use in Init-phase by checking the supported types of inputs, outputs and transformations. In our case we have input DataObject stg-departures which is still a JsonFileDataObject, that can not create a Snowpark-DataFrame. As we would like to execute this join as well in Snowflake with Snowpark for performance reasons, lets create a SnowflakeTableDataObject int-departures and use it as input for Action~join-departures-airports.

Add a DataObject int-departures:

      int-departures {
        type = SnowflakeTableDataObject
        connectionId = sf-con
        table {
          db = "test"
          name = "int_departures"
        }
      }

Add an Action copy-departures:

      copy-departures {
        type = CopyAction
        inputId = stg-departures
        outputId = int-departures
        metadata {
          feed = compute
        }
      }

Fix inputs of Action join-departures-airports:

      inputIds = [int-departures, int-airports]

... and code of the first SQL transformer:

      code = {
        btl-connected-airports = """
          select int_departures.estdepartureairport, int_departures.estarrivalairport, airports.*
          from int_departures join int_airports airports on int_departures.estArrivalAirport = airports.ident
        """

Compute with Spark and Snowpark again by using `--feed-sel compute` and browsing the logs, we can see that Action~join-departures-airports was executed with Snowpark:

      (Action~join-departures-airports) selected subFeedType SnowparkSubFeed [init-join-departures-airports]

# Summary

We have seen that its quite easy to migrate SDLB pipelines to use Snowpark instead of Spark, also only partially for selected Actions. SDLBs support of different DataFrame-AP-engines allows to still benefit of all other features of SDLB, like having full early validation over the whole pipeline by checking the schemas needed by Actions later in the pipeline.

Migrating Scala code of custom transformations using Spark DataFrame API needs some adaptions of import statements, but the rest stays mostly 1:1 the same. UDFs are also supported and dont need changes, but there might be surprises regarding data types (Snowparks Variant-type is not the same as Sparks nested datatypes) and deployment of needed libraries. We might investigate that in future blog post.