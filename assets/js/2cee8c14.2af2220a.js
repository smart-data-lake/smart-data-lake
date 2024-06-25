"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[4020],{8349:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>l,contentTitle:()=>s,default:()=>p,frontMatter:()=>i,metadata:()=>o,toc:()=>d});var a=t(4848),r=t(8453);const i={title:"Combine Spark and Snowpark to ingest and transform data in one pipeline",description:"An example to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake.",slug:"sdl-snowpark",authors:[{name:"Zach Kull",title:"Data Expert",url:"https://www.linkedin.com/in/zacharias-kull-94705886/"}],tags:["Snowpark","Snowflake"],hide_table_of_contents:!1},s=void 0,o={permalink:"/blog/sdl-snowpark",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2022-04-06-SDL-snowpark/2022-04-06-SDL-snowpark.md",source:"@site/blog/2022-04-06-SDL-snowpark/2022-04-06-SDL-snowpark.md",title:"Combine Spark and Snowpark to ingest and transform data in one pipeline",description:"An example to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake.",date:"2022-04-06T00:00:00.000Z",formattedDate:"April 6, 2022",tags:[{label:"Snowpark",permalink:"/blog/tags/snowpark"},{label:"Snowflake",permalink:"/blog/tags/snowflake"}],readingTime:7.285,hasTruncateMarker:!0,authors:[{name:"Zach Kull",title:"Data Expert",url:"https://www.linkedin.com/in/zacharias-kull-94705886/"}],frontMatter:{title:"Combine Spark and Snowpark to ingest and transform data in one pipeline",description:"An example to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake.",slug:"sdl-snowpark",authors:[{name:"Zach Kull",title:"Data Expert",url:"https://www.linkedin.com/in/zacharias-kull-94705886/"}],tags:["Snowpark","Snowflake"],hide_table_of_contents:!1},unlisted:!1,prevItem:{title:"Deployment on Databricks",permalink:"/blog/sdl-databricks"},nextItem:{title:"Using Airbyte connector to inspect github data",permalink:"/blog/sdl-airbyte"}},l={authorsImageUrls:[void 0]},d=[{value:"Prerequisites",id:"prerequisites",level:2},{value:"Goal",id:"goal",level:2},{value:"Prepare the Snowflake library",id:"prepare-the-snowflake-library",level:2},{value:"Define Snowflake connection",id:"define-snowflake-connection",level:2},{value:"Migrate DataObjects",id:"migrate-dataobjects",level:2},{value:"Migrating Actions",id:"migrating-actions",level:2},{value:"Compile and run",id:"compile-and-run",level:2}];function c(e){const n={a:"a",code:"code",em:"em",h1:"h1",h2:"h2",li:"li",p:"p",pre:"pre",ul:"ul",...(0,r.R)(),...e.components};return(0,a.jsxs)(a.Fragment,{children:[(0,a.jsx)(n.p,{children:"This article shows how to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake."}),"\n",(0,a.jsxs)(n.p,{children:["Recent developments in Smart Data Lake Builder (SDLB) included refactorings to integrate alternative execution engines to Spark.\r\nIn particular ",(0,a.jsx)(n.a,{href:"https://docs.snowflake.com/en/developer-guide/snowpark/index.html",children:"Snowpark"})," integration was implemented, a Spark like DataFrame API for implementing transformations in Snowflake."]}),"\n",(0,a.jsxs)(n.p,{children:["Implementing transformations in Snowflake has big performance and cost benefits. And using a DataFrame API is much more powerful than coding in SQL, see also ",(0,a.jsx)(n.a,{href:"https://medium.com/towards-data-science/modern-data-stack-which-place-for-spark-8e10365a8772",children:"Modern Data Stack: Which Place for Spark?"}),"."]}),"\n",(0,a.jsx)(n.p,{children:"Snowpark is good for transforming data inside Snowflake, but not all data might be located in Snowflake and suitable for Snowflake.\r\nHere it is interesting to use Spark and its many connectors, in particular to ingest and export data."}),"\n",(0,a.jsx)(n.p,{children:"Combining Spark and Snowpark in a smart data pipeline using a DataFrame API would be the ideal solution.\r\nWith the integration of Snowpark as engine in SDLB we created just that."}),"\n",(0,a.jsxs)(n.p,{children:["This blog post will show how to migrate our example data pipeline of the ",(0,a.jsx)(n.a,{href:"../../docs/getting-started/setup",children:"Getting Started"})," guide Part 1 to use Spark for ingestion and Snowpark for transformation."]}),"\n",(0,a.jsx)(n.h2,{id:"prerequisites",children:"Prerequisites"}),"\n",(0,a.jsxs)(n.ul,{children:["\n",(0,a.jsxs)(n.li,{children:["Create a Snowflake trial account on ",(0,a.jsx)(n.a,{href:"https://signup.snowflake.com/",children:"https://signup.snowflake.com/"})," and note the following connection informations:","\n",(0,a.jsxs)(n.ul,{children:["\n",(0,a.jsx)(n.li,{children:'Account URL (copy by navigating to "Organization" and clicking the link symbol on the right of the account name)'}),"\n",(0,a.jsx)(n.li,{children:"Username"}),"\n",(0,a.jsx)(n.li,{children:"Password"}),"\n"]}),"\n"]}),"\n",(0,a.jsxs)(n.li,{children:['Create database "testdb" in Snowflake: ',(0,a.jsx)(n.code,{children:"create database testdb;"})]}),"\n",(0,a.jsxs)(n.li,{children:['Create schema "testdb.test" in Snowflake: ',(0,a.jsx)(n.code,{children:"create schema testdb.test;"})]}),"\n",(0,a.jsxs)(n.li,{children:["Setup running SDLB docker image with part-1 configuration as described in ",(0,a.jsx)(n.a,{href:"../../docs/getting-started/setup",children:"Getting Started"}),"\n",(0,a.jsxs)(n.ul,{children:["\n",(0,a.jsx)(n.li,{children:"build sdl-spark image"}),"\n",(0,a.jsxs)(n.li,{children:["copy final application.conf of part-1: ",(0,a.jsx)(n.code,{children:"cp config/application.conf.part-1-solution config/application.conf"})]}),"\n",(0,a.jsxs)(n.li,{children:["run download actions with parameter ",(0,a.jsx)(n.code,{children:"--feed-sel download"})]}),"\n",(0,a.jsxs)(n.li,{children:["run compute actions with parameter ",(0,a.jsx)(n.code,{children:"--feed-sel compute"})]}),"\n"]}),"\n"]}),"\n"]}),"\n",(0,a.jsx)(n.h2,{id:"goal",children:"Goal"}),"\n",(0,a.jsx)(n.p,{children:"The example of part-1 has the following DataObjects"}),"\n",(0,a.jsx)(n.p,{children:"Staging Layer"}),"\n",(0,a.jsxs)(n.ul,{children:["\n",(0,a.jsx)(n.li,{children:"stg-departures: JsonFileDataObject"}),"\n",(0,a.jsx)(n.li,{children:"stg-airports: CsvFileDataObject"}),"\n"]}),"\n",(0,a.jsx)(n.p,{children:"Integration Layer"}),"\n",(0,a.jsxs)(n.ul,{children:["\n",(0,a.jsx)(n.li,{children:"int-airports: CsvFileDataObject"}),"\n"]}),"\n",(0,a.jsx)(n.p,{children:"Business Transformation Layer"}),"\n",(0,a.jsxs)(n.ul,{children:["\n",(0,a.jsx)(n.li,{children:"btl-departures-arrivals-airports: CsvFileDataObject"}),"\n",(0,a.jsx)(n.li,{children:"btl-distances: CsvFileDataObject"}),"\n"]}),"\n",(0,a.jsx)(n.p,{children:"In this example we will migrate Integration and Business Transformation Layer to Snowflake.\r\nWe will use Spark to fill Staging and Integration Layer, and Snowpark for transformation from Integration to Business Transformation Layer."}),"\n",(0,a.jsx)(n.h2,{id:"prepare-the-snowflake-library",children:"Prepare the Snowflake library"}),"\n",(0,a.jsx)(n.p,{children:"First we have add SDLBs Snowflake library to the projects pom.xml dependencies section:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  <dependencies>\r\n    ....\r\n    <dependency>\r\n      <groupId>io.smartdatalake</groupId>\r\n      <artifactId>sdl-snowflake_${scala.minor.version}</artifactId>\r\n      <version>${project.parent.version}</version>\r\n    </dependency>\r\n    ...\r\n  </dependencies>\n"})}),"\n",(0,a.jsx)(n.p,{children:"Then SDLB version needs to be updated to version 2.3.0-SNAPSHOT at least in the parent section:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  <parent>\r\n    <groupId>io.smartdatalake</groupId>\r\n    <artifactId>sdl-parent</artifactId>\r\n    <version>2.3.0-SNAPSHOT</version>\r\n  </parent>\n"})}),"\n",(0,a.jsx)(n.h2,{id:"define-snowflake-connection",children:"Define Snowflake connection"}),"\n",(0,a.jsx)(n.p,{children:'To define the Snowflake connection in config/application.conf, add connections section with connection "sf-con", and fill in informations according to prerequisits:'}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:'  connections {\r\n    sf-con {\r\n      type = SnowflakeTableConnection\r\n      url = "<accountUrl>",\r\n      warehouse = "COMPUTE_WH",\r\n      database = "testdb",\r\n      role = "ACCOUNTADMIN",\r\n      authMode = {\r\n        type = BasicAuthMode\r\n        userVariable = "CLEAR#<username>"\r\n        passwordVariable = "CLEAR#<pwd>"\r\n    }\r\n  }\n'})}),"\n",(0,a.jsx)(n.h2,{id:"migrate-dataobjects",children:"Migrate DataObjects"}),"\n",(0,a.jsx)(n.p,{children:"Now we can change the DataObject type to SnowflakeTableDataObject and the new Snowflake connection, adding the definition of the table:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:'  int-airports {\r\n    type = SnowflakeTableDataObject\r\n    connectionId = sf-con\r\n    table {\r\n      db = "test"\r\n      name = "int_airports"\r\n    }\r\n  }\r\n\r\n  btl-departures-arrivals-airports {\r\n    type = SnowflakeTableDataObject\r\n    connectionId = sf-con\r\n    table {\r\n      db = "test"\r\n      name = "btl_departures_arrivals_airports"\r\n    }\r\n  }\r\n\r\n  btl-distances {\r\n    type = SnowflakeTableDataObject\r\n    connectionId = sf-con\r\n    table {\r\n      db = "test"\r\n      name = "btl_distances"\r\n    }\r\n  }\n'})}),"\n",(0,a.jsxs)(n.p,{children:["Note that the attribute ",(0,a.jsx)(n.code,{children:"db"})," of the SnowflakeTableDataObject should be filled with the schema of the Snowflake table and that this is ",(0,a.jsx)(n.em,{children:"not"})," the same as the attribute ",(0,a.jsx)(n.code,{children:"database"})," of SnowflakeTableConnection."]}),"\n",(0,a.jsx)(n.h2,{id:"migrating-actions",children:"Migrating Actions"}),"\n",(0,a.jsx)(n.p,{children:"The new SDLB version introduced some naming changes:"}),"\n",(0,a.jsxs)(n.ul,{children:["\n",(0,a.jsx)(n.li,{children:"The CustomSparkAction can now also process Snowpark-DataFrames and is therefore renamed to CustomDataFrameAction."}),"\n",(0,a.jsx)(n.li,{children:"The ScalaClassDfTransformer was specific for Spark. In the new SDLB version there is a specific scala-class DataFrame transformer for Spark and Snowpark, e.g. ScalaClassSparkDfTransformer and ScalaClassSnowparkDfTransformer. And there is even a ScalaClassGenericDfTransformer to implement transformations using a unified API. In our case we will migrate the transformation to use Snowpark and set the type to ScalaClassSnowparkDfTransformer."}),"\n"]}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  join-departures-airports {\r\n    type = CustomSparkAction -> CustomDataFrameAction\r\n    ...\r\n\r\n  compute-distances {\r\n    ...\r\n    transformers = [{\r\n      type = ScalaClassDfTransformer -> ScalaClassSnowparkDfTransformer\n"})}),"\n",(0,a.jsx)(n.p,{children:"There is no need to change the SQL transformtions of join-departures-airport, as the SQL should run on Snowpark aswell."}),"\n",(0,a.jsx)(n.p,{children:"On the other hand the ComputeDistanceTransformer was implemented with the Spark DataFrame API. We need to migrate it to Snowpark DataFrame API to run this Action with Snowpark. Luckily the API's are very similar. Often it's sufficient to change the import statement, the class we're extending and the session parameters type:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  import com.snowflake.snowpark.functions._\r\n  import com.snowflake.snowpark.{DataFrame, Session}\r\n  import io.smartdatalake.workflow.action.snowflake.customlogic.CustomSnowparkDfTransformer\r\n\r\n  class ComputeDistanceTransformer extends CustomSnowparkDfTransformer {\r\n    def transform(session: Session, options: Map[String, String], df: DataFrame, dataObjectId: String) : DataFrame = {\r\n      ...\n"})}),"\n",(0,a.jsxs)(n.p,{children:["If you have UDFs in your code, it gets trickier. The UDF Code gets serialized to Snowflake, details see ",(0,a.jsx)(n.a,{href:"https://docs.snowflake.com/en/developer-guide/snowpark/scala/creating-udfs.html",children:"Snowpark UDFs"}),". Special care must be taken to minimize the scope the UDF is defined in. Thats why we move the function into the companion object."]}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:'  object ComputeDistanceTransformer {\r\n    def calculateDistanceInKilometer(depLat: Double, depLng: Double, arrLat: Double, arrLng: Double): Double = {\r\n      val AVERAGE_RADIUS_OF_EARTH_KM = 6371\r\n      val latDistance = Math.toRadians(depLat - arrLat)\r\n      val lngDistance = Math.toRadians(depLng - arrLng)\r\n      val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(depLat)) * Math.cos(Math.toRadians(arrLat)) * Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2)\r\n      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))\r\n      AVERAGE_RADIUS_OF_EARTH_KM * c\r\n    }\r\n    def getCalculateDistanceInKilometerUdf(session: Session) = {\r\n      // using only udf(...) function results in "SnowparkClientException: Error Code: 0207, Error message: No default Session found. Use <session>.udf.registerTemporary() to explicitly refer to a session."\r\n      session.udf.registerTemporary(ComputeDistanceTransformer.calculateDistanceInKilometer _)\r\n    }\r\n  }\n'})}),"\n",(0,a.jsx)(n.p,{children:'Note that we need to pass the Session to a function for registering the UDF. There is an Error 0207 if we use "udf" function (at least in snowpark version 1.2.0).\r\nFinally we need to adapt the call of the UDF as follows:'}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:'  df.withColumn("distance", ComputeDistanceTransformer.getCalculateDistanceInKilometerUdf(session)(col("dep_latitude_deg"),col("dep_longitude_deg"),col("arr_latitude_deg"), col("arr_longitude_deg")))\n'})}),"\n",(0,a.jsx)(n.h2,{id:"compile-and-run",children:"Compile and run"}),"\n",(0,a.jsx)(n.p,{children:"Time to see if it works.\r\nLets build an update SDLB docker image with the updated SDLB version:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  podman build -t sdl-spark .\n"})}),"\n",(0,a.jsx)(n.p,{children:"Then compile the code with the UDF:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:'  mkdir .mvnrepo\r\n  podman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\n'})}),"\n",(0,a.jsxs)(n.p,{children:["Download initial data with ",(0,a.jsx)(n.code,{children:"--feed-sel download"}),":"]}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\n"})}),"\n",(0,a.jsxs)(n.p,{children:["Compute with ",(0,a.jsx)(n.code,{children:"--feed-sel compute"}),":"]}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel compute\n"})}),"\n",(0,a.jsx)(n.p,{children:"If the SDLB run was SUCCESSFUL, you should now see TEST.BTL_DISTANCES table in Snowpark.\r\nTo check that Spark was used for Action select-airport-cols and Snowpark for Action compute-distances, look for the following logs, e.g. SnowparkSubFeed for Action~compute-distances:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  INFO  CopyAction - (Action~compute-distances) selected subFeedType SnowparkSubFeed [init-compute-distances]\n"})}),"\n",(0,a.jsx)(n.h1,{id:"engine-selection---uncover-the-magic",children:"Engine selection - uncover the magic"}),"\n",(0,a.jsx)(n.p,{children:"Browsing through the logs it turns out that the Action~join-departures-airports was still executed with Spark (SparkSubFeed)!"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  INFO  CustomDataFrameAction - (Action~join-departures-airports) selected subFeedType SparkSubFeed [init-join-departures-airports]\n"})}),"\n",(0,a.jsx)(n.p,{children:"An Action determines the engine to use in Init-phase by checking the supported types of inputs, outputs and transformations. In our case we have input DataObject stg-departures which is still a JsonFileDataObject, that can not create a Snowpark-DataFrame. As we would like to execute this join as well in Snowflake with Snowpark for performance reasons, lets create a SnowflakeTableDataObject int-departures and use it as input for Action~join-departures-airports."}),"\n",(0,a.jsx)(n.p,{children:"Add a DataObject int-departures:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:'  int-departures {\r\n    type = SnowflakeTableDataObject\r\n    connectionId = sf-con\r\n    table {\r\n      db = "test"\r\n      name = "int_departures"\r\n    }\r\n  }\n'})}),"\n",(0,a.jsx)(n.p,{children:"Add an Action copy-departures:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  copy-departures {\r\n    type = CopyAction\r\n    inputId = stg-departures\r\n    outputId = int-departures\r\n    metadata {\r\n      feed = compute\r\n    }\r\n  }\n"})}),"\n",(0,a.jsx)(n.p,{children:"Fix inputs of Action join-departures-airports:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  inputIds = [int-departures, int-airports]\n"})}),"\n",(0,a.jsx)(n.p,{children:"... and code of the first SQL transformer:"}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:'  code = {\r\n    btl-connected-airports = """\r\n      select int_departures.estdepartureairport, int_departures.estarrivalairport, airports.*\r\n      from int_departures join int_airports airports on int_departures.estArrivalAirport = airports.ident\r\n    """\n'})}),"\n",(0,a.jsxs)(n.p,{children:["Compute with Spark and Snowpark again by using ",(0,a.jsx)(n.code,{children:"--feed-sel compute"})," and browsing the logs, we can see that Action~join-departures-airports was executed with Snowpark:"]}),"\n",(0,a.jsx)(n.pre,{children:(0,a.jsx)(n.code,{children:"  (Action~join-departures-airports) selected subFeedType SnowparkSubFeed [init-join-departures-airports]\n"})}),"\n",(0,a.jsx)(n.h1,{id:"summary",children:"Summary"}),"\n",(0,a.jsx)(n.p,{children:"We have seen that its quite easy to migrate SDLB pipelines to use Snowpark instead of Spark, also only partially for selected Actions. SDLB's support of different DataFrame-API-Engines allows to still benefit of all other features of SDLB, like having full early validation over the whole pipeline by checking the schemas needed by Actions later in the pipeline."}),"\n",(0,a.jsx)(n.p,{children:"Migrating Scala code of custom transformations using Spark DataFrame API needs some adaptions of import statements, but the rest stays mostly 1:1 the same. UDFs are also supported and dont need changes, but there might be surprises regarding data types (Snowparks Variant-type is not the same as Sparks nested datatypes) and deployment of needed libraries. We might investigate that in future blog post."})]})}function p(e={}){const{wrapper:n}={...(0,r.R)(),...e.components};return n?(0,a.jsx)(n,{...e,children:(0,a.jsx)(c,{...e})}):c(e)}},8453:(e,n,t)=>{t.d(n,{R:()=>s,x:()=>o});var a=t(6540);const r={},i=a.createContext(r);function s(e){const n=a.useContext(i);return a.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function o(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:s(e.components),a.createElement(i.Provider,{value:n},e.children)}}}]);