"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[1477],{10:function(e){e.exports=JSON.parse('{"blogPosts":[{"id":"sdl-snowpark","metadata":{"permalink":"/blog/sdl-snowpark","editUrl":"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2022-04-06-SDL-snowpark/2022-04-06-SDL-snowpark.md","source":"@site/blog/2022-04-06-SDL-snowpark/2022-04-06-SDL-snowpark.md","title":"Combine Spark and Snowpark to ingest and transform data in one pipeline","description":"An example to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake.","date":"2022-04-06T00:00:00.000Z","formattedDate":"April 6, 2022","tags":[{"label":"Snowpark","permalink":"/blog/tags/snowpark"},{"label":"Snowflake","permalink":"/blog/tags/snowflake"}],"readingTime":7.095,"truncated":true,"authors":[{"name":"Zach Kull","title":"Data Expert","url":"https://www.linkedin.com/in/zacharias-kull-94705886/"}],"frontMatter":{"title":"Combine Spark and Snowpark to ingest and transform data in one pipeline","description":"An example to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake.","slug":"sdl-snowpark","authors":[{"name":"Zach Kull","title":"Data Expert","url":"https://www.linkedin.com/in/zacharias-kull-94705886/"}],"tags":["Snowpark","Snowflake"],"hide_table_of_contents":false},"nextItem":{"title":"Using Airbyte connector to inspect github data","permalink":"/blog/sdl-airbyte"}},"content":"This article shows how to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake.\\r\\n\\r\\n\x3c!--truncate--\x3e\\r\\n\\r\\nRecent developments in Smart Data Lake Builder (SDLB) included refactorings to integrate alternative execution engines to Spark.\\r\\nIn particular [Snowpark](https://docs.snowflake.com/en/developer-guide/snowpark/index.html) integration was implemented, a Spark like DataFrame API for implementing transformations in Snowflake.\\r\\n\\r\\nImplementing transformations in Snowflake has big performance and cost benefits. And using a DataFrame API is much more powerful than coding in SQL, see also [Modern Data Stack: Which Place for Spark?](https://medium.com/towards-data-science/modern-data-stack-which-place-for-spark-8e10365a8772).\\r\\n\\r\\nSnowpark is good for transforming data inside Snowflake, but not all data might be located in Snowflake and suitable for Snowflake. \\r\\nHere it is interesting to use Spark and its many connectors, in particular to ingest and export data.\\r\\n\\r\\nCombining Spark and Snowpark in a smart data pipeline using a DataFrame API would be the ideal solution.\\r\\nWith the integration of Snowpark as engine in SDLB we created just that. \\r\\n\\r\\nThis blog post will show how to migrate our example data pipeline of the [Getting Started](../../docs/getting-started/setup) guide Part 1 to use Spark for ingestion and Snowpark for transformation.\\r\\n\\r\\n## Prerequisites\\r\\n\\r\\n* Create a Snowflake trial account on https://signup.snowflake.com/ and note the following connection informations:\\r\\n  * Account URL (copy by navigating to \\"Organization\\" and clicking the link symbol on the right of the account name)\\r\\n  * Username\\r\\n  * Password\\r\\n* Create database \\"testdb\\" in Snowflake: `create database testdb;`\\r\\n* Create schema \\"testdb.test\\" in Snowflake: `create schema testdb.test;`\\r\\n* Setup running SDLB docker image with part-1 configuration as described in [Getting Started](../../docs/getting-started/setup)\\r\\n  * build sdl-spark image\\r\\n  * copy final application.conf of part-1: `cp config/application.conf.part-1-solution config/application.conf`\\r\\n  * run download actions with parameter `--feed-sel download`\\r\\n  * run compute actions with parameter `--feed-sel compute`\\r\\n\\r\\n## Goal\\r\\n\\r\\nThe example of part-1 has the following DataObjects\\r\\n\\r\\nStaging Layer\\r\\n* stg-departures: JsonFileDataObject\\r\\n* stg-airports: CsvFileDataObject\\r\\n\\r\\nIntegration Layer\\r\\n* int-airports: CsvFileDataObject\\r\\n\\r\\nBusiness Transformation Layer\\r\\n* btl-departures-arrivals-airports: CsvFileDataObject\\r\\n* btl-distances: CsvFileDataObject\\r\\n\\r\\nIn this example we will migrate Integration and Business Transformation Layer to Snowflake.\\r\\nWe will use Spark to fill Staging and Integration Layer, and Snowpark for transformation from Integration to Business Transformation Layer.\\r\\n\\r\\n## Prepare the Snowflake library\\r\\n\\r\\nFirst we have add SDLBs Snowflake library to the projects pom.xml dependencies section:\\r\\n\\r\\n      <dependencies>\\r\\n        ....\\r\\n        <dependency>\\r\\n          <groupId>io.smartdatalake</groupId>\\r\\n          <artifactId>sdl-snowflake_${scala.minor.version}</artifactId>\\r\\n          <version>${project.parent.version}</version>\\r\\n        </dependency>\\r\\n        ...\\r\\n      </dependencies>\\r\\n\\r\\nThen SDLB version needs to be updated to version 2.3.0-SNAPSHOT at least in the parent section:\\r\\n\\r\\n      <parent>\\r\\n        <groupId>io.smartdatalake</groupId>\\r\\n        <artifactId>sdl-parent</artifactId>\\r\\n        <version>2.3.0-SNAPSHOT</version>\\r\\n      </parent>\\r\\n\\r\\n## Define Snowflake connection\\r\\n\\r\\nTo define the Snowflake connection in config/application.conf, add connections section with connection \\"sf-con\\", and fill in informations according to prerequisits:\\r\\n\\r\\n      connections {\\r\\n        sf-con {\\r\\n          type = SnowflakeTableConnection\\r\\n          url = \\"<accountUrl>\\",\\r\\n          warehouse = \\"COMPUTE_WH\\",\\r\\n          database = \\"testdb\\",\\r\\n          role = \\"ACCOUNTADMIN\\",\\r\\n          authMode = {\\r\\n            type = BasicAuthMode\\r\\n            userVariable = \\"CLEAR#<username>\\"\\r\\n            passwordVariable = \\"CLEAR#<pwd>\\"\\r\\n        }\\r\\n      }\\r\\n\\r\\n## Migrate DataObjects\\r\\n\\r\\nNow we can change the DataObject type to SnowflakeTableDataObject and the new Snowflake connection, adding the definition of the table:\\r\\n\\r\\n      int-airports {\\r\\n        type = SnowflakeTableDataObject\\r\\n        connectionId = sf-con\\r\\n        table {\\r\\n          db = \\"test\\"\\r\\n          name = \\"int_airports\\"\\r\\n        }\\r\\n      }\\r\\n    \\r\\n      btl-departures-arrivals-airports {\\r\\n        type = SnowflakeTableDataObject\\r\\n        connectionId = sf-con\\r\\n        table {\\r\\n          db = \\"test\\"\\r\\n          name = \\"btl_departures_arrivals_airports\\"\\r\\n        }\\r\\n      }\\r\\n    \\r\\n      btl-distances {\\r\\n        type = SnowflakeTableDataObject\\r\\n        connectionId = sf-con\\r\\n        table {\\r\\n          db = \\"test\\"\\r\\n          name = \\"btl_distances\\"\\r\\n        }\\r\\n      }\\r\\n\\r\\nNote that the attribute `db` of the SnowflakeTableDataObject should be filled with the schema of the Snowflake table and that this is *not* the same as the attribute `database` of SnowflakeTableConnection. \\r\\n\\r\\n## Migrating Actions\\r\\n\\r\\nThe new SDLB version introduced some naming changes:\\r\\n- The CustomSparkAction can now also process Snowpark-DataFrames and is therefore renamed to CustomDataFrameAction.\\r\\n- The ScalaClassDfTransformer was specific for Spark. In the new SDLB version there is a specific scala-class DataFrame transformer for Spark and Snowpark, e.g. ScalaClassSparkDfTransformer and ScalaClassSnowparkDfTransformer. And there is even a ScalaClassGenericDfTransformer to implement transformations using a unified API. In our case we will migrate the transformation to use Snowpark and set the type to ScalaClassSnowparkDfTransformer.\\r\\n\\r\\n      join-departures-airports {\\r\\n        type = CustomSparkAction -> CustomDataFrameAction\\r\\n        ...\\r\\n\\r\\n      compute-distances {\\r\\n        ...\\r\\n        transformers = [{\\r\\n          type = ScalaClassDfTransformer -> ScalaClassSnowparkDfTransformer\\r\\n\\r\\nThere is no need to change the SQL transformtions of join-departures-airport, as the SQL should run on Snowpark aswell.\\r\\n\\r\\nOn the other hand the ComputeDistanceTransformer was implemented with the Spark DataFrame API. We need to migrate it to Snowpark DataFrame API to run this Action with Snowpark. Luckily the API\'s are very similar. Often it\'s sufficient to change the import statement, the class we\'re extending and the session parameters type:\\r\\n\\r\\n      import com.snowflake.snowpark.functions._\\r\\n      import com.snowflake.snowpark.{DataFrame, Session}\\r\\n      import io.smartdatalake.workflow.action.snowflake.customlogic.CustomSnowparkDfTransformer\\r\\n\\r\\n      class ComputeDistanceTransformer extends CustomSnowparkDfTransformer {\\r\\n        def transform(session: Session, options: Map[String, String], df: DataFrame, dataObjectId: String) : DataFrame = {\\r\\n          ...\\r\\n\\r\\nIf you have UDFs in your code, it gets trickier. The UDF Code gets serialized to Snowflake, details see [Snowpark UDFs](https://docs.snowflake.com/en/developer-guide/snowpark/scala/creating-udfs.html). Special care must be taken to minimize the scope the UDF is defined in. Thats why we move the function into the companion object.\\r\\n\\r\\n      object ComputeDistanceTransformer {\\r\\n        def calculateDistanceInKilometer(depLat: Double, depLng: Double, arrLat: Double, arrLng: Double): Double = {\\r\\n          val AVERAGE_RADIUS_OF_EARTH_KM = 6371\\r\\n          val latDistance = Math.toRadians(depLat - arrLat)\\r\\n          val lngDistance = Math.toRadians(depLng - arrLng)\\r\\n          val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(depLat)) * Math.cos(Math.toRadians(arrLat)) * Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2)\\r\\n          val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))\\r\\n          AVERAGE_RADIUS_OF_EARTH_KM * c\\r\\n        }\\r\\n        def getCalculateDistanceInKilometerUdf(session: Session) = {\\r\\n          // using only udf(...) function results in \\"SnowparkClientException: Error Code: 0207, Error message: No default Session found. Use <session>.udf.registerTemporary() to explicitly refer to a session.\\"\\r\\n          session.udf.registerTemporary(ComputeDistanceTransformer.calculateDistanceInKilometer _)\\r\\n        }\\r\\n      }\\r\\n\\r\\nNote that we need to pass the Session to a function for registering the UDF. There is an Error 0207 if we use \\"udf\\" function (at least in snowpark version 1.2.0).\\r\\nFinally we need to adapt the call of the UDF as follows:\\r\\n\\r\\n    df.withColumn(\\"distance\\", ComputeDistanceTransformer.getCalculateDistanceInKilometerUdf(session)(col(\\"dep_latitude_deg\\"),col(\\"dep_longitude_deg\\"),col(\\"arr_latitude_deg\\"), col(\\"arr_longitude_deg\\")))\\r\\n\\r\\n## Compile and run\\r\\n\\r\\nTime to see if it works.\\r\\nLets build an update SDLB docker image with the updated SDLB version:\\r\\n\\r\\n      podman build -t sdl-spark .\\r\\n\\r\\nThen compile the code with the UDF:\\r\\n\\r\\n      mkdir .mvnrepo\\r\\n      podman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml \\"-Dmaven.repo.local=/mnt/.mvnrepo\\" package\\r\\n\\r\\nDownload initial data with `--feed-sel download`:\\r\\n\\r\\n      podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\\r\\n\\r\\nCompute with `--feed-sel compute`:\\r\\n\\r\\n      podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel compute\\r\\n\\r\\nIf the SDLB run was SUCCESSFUL, you should now see TEST.BTL_DISTANCES table in Snowpark.\\r\\nTo check that Spark was used for Action select-airport-cols and Snowpark for Action compute-distances, look for the following logs, e.g. SnowparkSubFeed for Action~compute-distances: \\r\\n\\r\\n      INFO  CopyAction - (Action~compute-distances) selected subFeedType SnowparkSubFeed [init-compute-distances]\\r\\n\\r\\n# Engine selection - uncover the magic\\r\\n\\r\\nBrowsing through the logs it turns out that the Action~join-departures-airports was still executed with Spark (SparkSubFeed)!\\r\\n\\r\\n      INFO  CustomDataFrameAction - (Action~join-departures-airports) selected subFeedType SparkSubFeed [init-join-departures-airports]\\r\\n\\r\\nAn Action determines the engine to use in Init-phase by checking the supported types of inputs, outputs and transformations. In our case we have input DataObject stg-departures which is still a JsonFileDataObject, that can not create a Snowpark-DataFrame. As we would like to execute this join as well in Snowflake with Snowpark for performance reasons, lets create a SnowflakeTableDataObject int-departures and use it as input for Action~join-departures-airports.\\r\\n\\r\\nAdd a DataObject int-departures:\\r\\n\\r\\n      int-departures {\\r\\n        type = SnowflakeTableDataObject\\r\\n        connectionId = sf-con\\r\\n        table {\\r\\n          db = \\"test\\"\\r\\n          name = \\"int_departures\\"\\r\\n        }\\r\\n      }\\r\\n\\r\\nAdd an Action copy-departures:\\r\\n\\r\\n      copy-departures {\\r\\n        type = CopyAction\\r\\n        inputId = stg-departures\\r\\n        outputId = int-departures\\r\\n        metadata {\\r\\n          feed = compute\\r\\n        }\\r\\n      }\\r\\n\\r\\nFix inputs of Action join-departures-airports:\\r\\n\\r\\n      inputIds = [int-departures, int-airports]\\r\\n\\r\\n... and code of the first SQL transformer:\\r\\n\\r\\n      code = {\\r\\n        btl-connected-airports = \\"\\"\\"\\r\\n          select int_departures.estdepartureairport, int_departures.estarrivalairport, airports.*\\r\\n          from int_departures join int_airports airports on int_departures.estArrivalAirport = airports.ident\\r\\n        \\"\\"\\"\\r\\n\\r\\nCompute with Spark and Snowpark again by using `--feed-sel compute` and browsing the logs, we can see that Action~join-departures-airports was executed with Snowpark:\\r\\n\\r\\n      (Action~join-departures-airports) selected subFeedType SnowparkSubFeed [init-join-departures-airports]\\r\\n\\r\\n# Summary\\r\\n\\r\\nWe have seen that its quite easy to migrate SDLB pipelines to use Snowpark instead of Spark, also only partially for selected Actions. SDLB\'s support of different DataFrame-API-Engines allows to still benefit of all other features of SDLB, like having full early validation over the whole pipeline by checking the schemas needed by Actions later in the pipeline.\\r\\n\\r\\nMigrating Scala code of custom transformations using Spark DataFrame API needs some adaptions of import statements, but the rest stays mostly 1:1 the same. UDFs are also supported and dont need changes, but there might be surprises regarding data types (Snowparks Variant-type is not the same as Sparks nested datatypes) and deployment of needed libraries. We might investigate that in future blog post."},{"id":"sdl-airbyte","metadata":{"permalink":"/blog/sdl-airbyte","editUrl":"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2022-03-30-SDL-airbyte/2022-03-30-SDL-airbyte.md","source":"@site/blog/2022-03-30-SDL-airbyte/2022-03-30-SDL-airbyte.md","title":"Using Airbyte connector to inspect github data","description":"A short example using Airbyte github connector","date":"2022-03-30T00:00:00.000Z","formattedDate":"March 30, 2022","tags":[{"label":"Airbyte","permalink":"/blog/tags/airbyte"},{"label":"Connector","permalink":"/blog/tags/connector"}],"readingTime":4.46,"truncated":true,"authors":[{"name":"Mandes Sch\xf6nherr","title":"Dr.sc.nat.","url":"https://github.com/mand35"}],"frontMatter":{"title":"Using Airbyte connector to inspect github data","description":"A short example using Airbyte github connector","slug":"sdl-airbyte","authors":[{"name":"Mandes Sch\xf6nherr","title":"Dr.sc.nat.","url":"https://github.com/mand35"}],"tags":["Airbyte","Connector"],"image":"airbyte.png","hide_table_of_contents":false},"prevItem":{"title":"Combine Spark and Snowpark to ingest and transform data in one pipeline","permalink":"/blog/sdl-snowpark"}},"content":"This article presents the deployment of an [Airbyte Connector](https://airbyte.com) with Smart Data Lake Builder (SDLB). \\nIn particular the [github connector](https://docs.airbyte.com/integrations/sources/github) is implemented using the python sources.\\n\\n\x3c!--truncate--\x3e\\n\\nAirbyte is a framework to sync data from a variety of sources (APIs and databases) into data warehouses and data lakes. \\nIn this example an Airbyte connector is utilized to stream data into Smart Data Lake (SDL). \\nTherefore, the [Airbyte dataObject](http://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=2-2) is used and will be configured. \\nThe general [Airbyte connector handling](https://docs.airbyte.com/understanding-airbyte/airbyte-specification#source) is implemented in SDL, which includes the 4 main steps:\\n* `spec`: receiving the specification of the connector\\n* `check`: validating the specified configuration\\n* `discover`: gather a catalog of available streams and its schemas\\n* `read`: collect the actual data\\n\\nThe actual connector is not provided in the SDL repository and needs to be obtained from the [Airbyte repository](https://github.com/airbytehq/airbyte). Besides the [list of existing connectors](https://docs.airbyte.com/integrations), custom connectors could be implemented in Python or Javascript. \\n\\nThe following description builds on top of the example setup from the [getting-started](../../docs/getting-started/setup) guide, using [Podman](https://docs.podman.io) as container engine within a [WSL](https://docs.microsoft.com/en-us/windows/wsl/install) Ubuntu image. \\n\\nThe [github connector](https://docs.airbyte.com/integrations/sources/github) is utilized to gather data about a specific repository.\\n\\n## Prerequisites\\nAfter downloading and installing all necessary packages, the connector is briefly tested:\\n* Python\\n* [Podman including `podman-compose`](../../docs/getting-started/troubleshooting/docker-on-windows) or [Docker](https://www.docker.com/get-started)\\n* [SDL example](https://github.com/smart-data-lake/getting-started/archive/refs/heads/master.zip), download and unpack: \\n  ```Bash\\n  git clone https://github.com/smart-data-lake/getting-started.git SDL_airbyte\\n  cd SDL_airbyte\\n  ```\\n* download the [Airbyte repository](https://github.com/airbytehq/airbyte) \\n  ```Bash\\n  git clone https://github.com/airbytehq/airbyte.git\\n  ```\\n  Alternatively, only the target connector can be downloaded:\\n  ```Bash\\n  svn checkout https://github.com/airbytehq/airbyte/trunk/airbyte-integrations/connectors/source-github\\n  ```\\n  Here the Airbyte `airbyte/airbyte-integrations/connectors/source-github/` directory is copied into the `SDL_airbyte` directory for handy calling the connector.\\n\\n## [Optional] Inspect the connector specification\\nThe first connector command `spec` provides the connector specification. This is the basis to create a connector configuration. To run the connector as is, the Python `airbyte-cdk` package needs to be installed and the connector can be launched:\\n\\n* Install Python airbyte-cdk: `pip install airbyte_cdk`\\n* try the connector: \\n  ```Bash\\n  cd SDL_airbyte\\n  python source_github/main.py spec | python -m json.tool\\n  ```\\n  This provides a [JSON string](github_spec_out.json) with the connector specification. The fields listed under `properties` are relevant for the configuration (compare with the configuration  used later). \\n\\n## Configuration\\nTo launch Smart Data Lake Builder (SDLB) with the Airbyte connector the following needs to be modified:\\n\\n* add the Airbyte ***dataObject*** with its configuration to the `config/application.conf`:\\n  ```Python\\n  dataObjects {\\n    ext-commits {\\n      type = AirbyteDataObject\\n      config = {\\n        \\"credentials\\": {\\n          \\"personal_access_token\\": \\"<yourPersonalAccessToken>\\" ### enter your personal access token here\\n        },\\n        \\"repository\\": \\"smart-data-lake/smart-data-lake\\",\\n        \\"start_date\\": \\"2021-02-01T00:00:00Z\\",\\n        \\"branch\\": \\"documentation develop-spark3 develop-spark2\\",\\n        \\"page_size_for_large_streams\\": 100\\n      },\\n      streamName = \\"commits\\",\\n      cmd = {\\n        type = CmdScript\\n        name = \\"airbyte_connector_github\\"\\n        linuxCmd = \\"python3 /mnt/source-github/main.py\\"\\n      }\\n    }\\n  ...\\n    stg-commits {\\n     type = DeltaLakeTableDataObject\\n     path = \\"~{id}\\"\\n     table {\\n      db = \\"default\\"\\n      name = \\"stg_commits\\"\\n      primaryKey = [created_at]\\n      }\\n    }\\n  ```\\n  Note the options set for `ext-commits` which define the Airbyte connector settings. \\n  While the `config` varies from connector to connector, the remaining fields are SDL specific. \\n  The `streamName` selects the stream, exactly one. \\n  If multiple streams should be collected, multiple dataObjects need to be defined. \\n  In `linuxCmd` the actual connector script is called. \\n  In our case we will mount the connector directory into the SDL container. \\n\\n* also add the definition of the data stream ***action*** to pipe the coming data stream into a `DeltaLakeTableDataObject`:\\n  ```Bash\\n    actions {\\n      download-commits {\\n        type = CopyAction\\n        inputId = ext-commits\\n        outputId = stg-commits\\n        metadata {\\n          feed = download\\n        }\\n      }\\n  ...\\n  ```\\n* Since Airbyte will be called as Python script in the sdl container, we need to (re-)build the container with Python support and the Python `airbyte-cdk` package. \\n  Therefore, in the Dockerfile we add:\\n\\t```\\n\\tRUN \\\\\\n  apt update && \\\\\\n  apt --assume-yes install python3 python3-pip && \\\\\\n  pip3 install airbyte-cdk~=0.1.25\\n  ```\\n  and rebuild \\n  ```Bash\\n  podman build . -t sdl-spark\\n  ```\\n\\nNow we are ready to go. My full [SDLB config file](application.conf) additionally includes the pull-request stream.\\n\\n## Run and inspect results\\nSince the data will be streamed into a `DeltaLakeTableDataObject`, the metastore container is necessary. Further, we aim to inspect the data using the Polynote notebook. Thus, first these containers are launched using (in the SDL example base directory):\\n```Bash\\npodman-compose up\\npodman pod ls\\n```\\nWith the second command we can verify the pod name and both running containers in it (should be three including the infra container).\\n\\nThen, the SDLB can be launched using the additional option to mount the Airbyte connector directory:\\n```Bash\\npodman run --hostname localhost --rm --pod sdl_airbyte -v ${PWD}/source-github/:/mnt/source-github -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\\n```\\n\\nThe output presents the successful run of the workflow:\\n```Bash\\n2022-03-16 07:54:03 INFO  ActionDAGRun$ActionEventListener - Action~download-commits[CopyAction]: Exec succeeded [dag-1-80]\\n2022-03-16 07:54:03 INFO  ActionDAGRun$ - exec SUCCEEDED for dag 1:\\n                 \u250c\u2500\u2500\u2500\u2500\u2500\u2510\\n                 \u2502start\u2502\\n                 \u2514\u2500\u2500\u2500\u252c\u2500\u2518\\n                     \u2502\\n                     v\\n \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\\n \u2502download-commits SUCCEEDED PT11.686865S\u2502\\n \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\\n     [main]\\n2022-03-16 07:54:03 INFO  LocalSmartDataLakeBuilder$ - LocalSmartDataLakeBuilder finished successfully: SUCCEEDED=1 [main]\\n2022-03-16 07:54:03 INFO  SparkUI - Stopped Spark web UI at http://localhost:4040 [shutdown-hook-0]\\n```\\n\\nLaunching Polynote `localhost:8192` in the browser, we can inspect data and develop further workflows. Here an example, where the commits are listed, which were committed in the name of someone else, excluding the web-flow. See [Polynote Notebook](SelectingData.ipynb)\\n![polynote example](polynote_commits.png)\\n\\n## Summary\\n\\nThe Airbyte connectors provide easy access to a variety of data sources. The connectors can be utilized in SDLB with just a few settings. This also works great for more complex interfaces."}]}')}}]);