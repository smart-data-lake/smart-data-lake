"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[434],{3905:function(e,t,n){n.d(t,{Zo:function(){return d},kt:function(){return m}});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),p=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},d=function(e){var t=p(e.components);return a.createElement(l.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},u=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,l=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),u=p(n),m=r,k=u["".concat(l,".").concat(m)]||u[m]||c[m]||o;return n?a.createElement(k,i(i({ref:t},d),{},{components:n})):a.createElement(k,i({ref:t},d))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,i=new Array(o);i[0]=u;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:r,i[1]=s;for(var p=2;p<o;p++)i[p]=n[p];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}u.displayName="MDXCreateElement"},9159:function(e,t,n){n.r(t),n.d(t,{assets:function(){return d},contentTitle:function(){return l},default:function(){return m},frontMatter:function(){return s},metadata:function(){return p},toc:function(){return c}});var a=n(7462),r=n(3366),o=(n(7294),n(3905)),i=["components"],s={title:"Combine Spark and Snowpark to ingest and transform data in one pipeline",description:"An example to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake.",slug:"sdl-snowpark",authors:[{name:"Zach Kull",title:"Data Expert",url:"https://www.linkedin.com/in/zacharias-kull-94705886/"}],tags:["Snowpark","Snowflake"],hide_table_of_contents:!1},l=void 0,p={permalink:"/blog/sdl-snowpark",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2022-04-06-SDL-snowpark/2022-04-06-SDL-snowpark.md",source:"@site/blog/2022-04-06-SDL-snowpark/2022-04-06-SDL-snowpark.md",title:"Combine Spark and Snowpark to ingest and transform data in one pipeline",description:"An example to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake.",date:"2022-04-06T00:00:00.000Z",formattedDate:"April 6, 2022",tags:[{label:"Snowpark",permalink:"/blog/tags/snowpark"},{label:"Snowflake",permalink:"/blog/tags/snowflake"}],readingTime:7.095,truncated:!0,authors:[{name:"Zach Kull",title:"Data Expert",url:"https://www.linkedin.com/in/zacharias-kull-94705886/"}],frontMatter:{title:"Combine Spark and Snowpark to ingest and transform data in one pipeline",description:"An example to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake.",slug:"sdl-snowpark",authors:[{name:"Zach Kull",title:"Data Expert",url:"https://www.linkedin.com/in/zacharias-kull-94705886/"}],tags:["Snowpark","Snowflake"],hide_table_of_contents:!1},nextItem:{title:"Using Airbyte connector to inspect github data",permalink:"/blog/sdl-airbyte"}},d={authorsImageUrls:[void 0]},c=[{value:"Prerequisites",id:"prerequisites",children:[],level:2},{value:"Goal",id:"goal",children:[],level:2},{value:"Prepare the Snowflake library",id:"prepare-the-snowflake-library",children:[],level:2},{value:"Define Snowflake connection",id:"define-snowflake-connection",children:[],level:2},{value:"Migrate DataObjects",id:"migrate-dataobjects",children:[],level:2},{value:"Migrating Actions",id:"migrating-actions",children:[],level:2},{value:"Compile and run",id:"compile-and-run",children:[],level:2}],u={toc:c};function m(e){var t=e.components,n=(0,r.Z)(e,i);return(0,o.kt)("wrapper",(0,a.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"This article shows how to create one unified data pipeline that uses Spark to ingest data into Snowflake, and Snowpark to transform data inside Snowflake."),(0,o.kt)("p",null,"Recent developments in Smart Data Lake Builder (SDLB) included refactorings to integrate alternative execution engines to Spark.\nIn particular ",(0,o.kt)("a",{parentName:"p",href:"https://docs.snowflake.com/en/developer-guide/snowpark/index.html"},"Snowpark")," integration was implemented, a Spark like DataFrame API for implementing transformations in Snowflake."),(0,o.kt)("p",null,"Implementing transformations in Snowflake has big performance and cost benefits. And using a DataFrame API is much more powerful than coding in SQL, see also ",(0,o.kt)("a",{parentName:"p",href:"https://medium.com/towards-data-science/modern-data-stack-which-place-for-spark-8e10365a8772"},"Modern Data Stack: Which Place for Spark?"),"."),(0,o.kt)("p",null,"Snowpark is good for transforming data inside Snowflake, but not all data might be located in Snowflake and suitable for Snowflake.\nHere it is interesting to use Spark and its many connectors, in particular to ingest and export data."),(0,o.kt)("p",null,"Combining Spark and Snowpark in a smart data pipeline using a DataFrame API would be the ideal solution.\nWith the integration of Snowpark as engine in SDLB we created just that. "),(0,o.kt)("p",null,"This blog post will show how to migrate our example data pipeline of the ",(0,o.kt)("a",{parentName:"p",href:"../../docs/getting-started/setup"},"Getting Started")," guide Part 1 to use Spark for ingestion and Snowpark for transformation."),(0,o.kt)("h2",{id:"prerequisites"},"Prerequisites"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Create a Snowflake trial account on ",(0,o.kt)("a",{parentName:"li",href:"https://signup.snowflake.com/"},"https://signup.snowflake.com/")," and note the following connection informations:",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},'Account URL (copy by navigating to "Organization" and clicking the link symbol on the right of the account name)'),(0,o.kt)("li",{parentName:"ul"},"Username"),(0,o.kt)("li",{parentName:"ul"},"Password"))),(0,o.kt)("li",{parentName:"ul"},'Create database "testdb" in Snowflake: ',(0,o.kt)("inlineCode",{parentName:"li"},"create database testdb;")),(0,o.kt)("li",{parentName:"ul"},'Create schema "testdb.test" in Snowflake: ',(0,o.kt)("inlineCode",{parentName:"li"},"create schema testdb.test;")),(0,o.kt)("li",{parentName:"ul"},"Setup running SDLB docker image with part-1 configuration as described in ",(0,o.kt)("a",{parentName:"li",href:"../../docs/getting-started/setup"},"Getting Started"),(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"build sdl-spark image"),(0,o.kt)("li",{parentName:"ul"},"copy final application.conf of part-1: ",(0,o.kt)("inlineCode",{parentName:"li"},"cp config/application.conf.part-1-solution config/application.conf")),(0,o.kt)("li",{parentName:"ul"},"run download actions with parameter ",(0,o.kt)("inlineCode",{parentName:"li"},"--feed-sel download")),(0,o.kt)("li",{parentName:"ul"},"run compute actions with parameter ",(0,o.kt)("inlineCode",{parentName:"li"},"--feed-sel compute"))))),(0,o.kt)("h2",{id:"goal"},"Goal"),(0,o.kt)("p",null,"The example of part-1 has the following DataObjects"),(0,o.kt)("p",null,"Staging Layer"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"stg-departures: JsonFileDataObject"),(0,o.kt)("li",{parentName:"ul"},"stg-airports: CsvFileDataObject")),(0,o.kt)("p",null,"Integration Layer"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"int-airports: CsvFileDataObject")),(0,o.kt)("p",null,"Business Transformation Layer"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"btl-departures-arrivals-airports: CsvFileDataObject"),(0,o.kt)("li",{parentName:"ul"},"btl-distances: CsvFileDataObject")),(0,o.kt)("p",null,"In this example we will migrate Integration and Business Transformation Layer to Snowflake.\nWe will use Spark to fill Staging and Integration Layer, and Snowpark for transformation from Integration to Business Transformation Layer."),(0,o.kt)("h2",{id:"prepare-the-snowflake-library"},"Prepare the Snowflake library"),(0,o.kt)("p",null,"First we have add SDLBs Snowflake library to the projects pom.xml dependencies section:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"  <dependencies>\n    ....\n    <dependency>\n      <groupId>io.smartdatalake</groupId>\n      <artifactId>sdl-snowflake_${scala.minor.version}</artifactId>\n      <version>${project.parent.version}</version>\n    </dependency>\n    ...\n  </dependencies>\n")),(0,o.kt)("p",null,"Then SDLB version needs to be updated to version 2.3.0-SNAPSHOT at least in the parent section:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"  <parent>\n    <groupId>io.smartdatalake</groupId>\n    <artifactId>sdl-parent</artifactId>\n    <version>2.3.0-SNAPSHOT</version>\n  </parent>\n")),(0,o.kt)("h2",{id:"define-snowflake-connection"},"Define Snowflake connection"),(0,o.kt)("p",null,'To define the Snowflake connection in config/application.conf, add connections section with connection "sf-con", and fill in informations according to prerequisits:'),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'  connections {\n    sf-con {\n      type = SnowflakeTableConnection\n      url = "<accountUrl>",\n      warehouse = "COMPUTE_WH",\n      database = "testdb",\n      role = "ACCOUNTADMIN",\n      authMode = {\n        type = BasicAuthMode\n        userVariable = "CLEAR#<username>"\n        passwordVariable = "CLEAR#<pwd>"\n    }\n  }\n')),(0,o.kt)("h2",{id:"migrate-dataobjects"},"Migrate DataObjects"),(0,o.kt)("p",null,"Now we can change the DataObject type to SnowflakeTableDataObject and the new Snowflake connection, adding the definition of the table:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'  int-airports {\n    type = SnowflakeTableDataObject\n    connectionId = sf-con\n    table {\n      db = "test"\n      name = "int_airports"\n    }\n  }\n\n  btl-departures-arrivals-airports {\n    type = SnowflakeTableDataObject\n    connectionId = sf-con\n    table {\n      db = "test"\n      name = "btl_departures_arrivals_airports"\n    }\n  }\n\n  btl-distances {\n    type = SnowflakeTableDataObject\n    connectionId = sf-con\n    table {\n      db = "test"\n      name = "btl_distances"\n    }\n  }\n')),(0,o.kt)("p",null,"Note that the attribute ",(0,o.kt)("inlineCode",{parentName:"p"},"db")," of the SnowflakeTableDataObject should be filled with the schema of the Snowflake table and that this is ",(0,o.kt)("em",{parentName:"p"},"not")," the same as the attribute ",(0,o.kt)("inlineCode",{parentName:"p"},"database")," of SnowflakeTableConnection. "),(0,o.kt)("h2",{id:"migrating-actions"},"Migrating Actions"),(0,o.kt)("p",null,"The new SDLB version introduced some naming changes:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"The CustomSparkAction can now also process Snowpark-DataFrames and is therefore renamed to CustomDataFrameAction.")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"The ScalaClassDfTransformer was specific for Spark. In the new SDLB version there is a specific scala-class DataFrame transformer for Spark and Snowpark, e.g. ScalaClassSparkDfTransformer and ScalaClassSnowparkDfTransformer. And there is even a ScalaClassGenericDfTransformer to implement transformations using a unified API. In our case we will migrate the transformation to use Snowpark and set the type to ScalaClassSnowparkDfTransformer."),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre"},"join-departures-airports {\n  type = CustomSparkAction -> CustomDataFrameAction\n  ...\n\ncompute-distances {\n  ...\n  transformers = [{\n    type = ScalaClassDfTransformer -> ScalaClassSnowparkDfTransformer\n")))),(0,o.kt)("p",null,"There is no need to change the SQL transformtions of join-departures-airport, as the SQL should run on Snowpark aswell."),(0,o.kt)("p",null,"On the other hand the ComputeDistanceTransformer was implemented with the Spark DataFrame API. We need to migrate it to Snowpark DataFrame API to run this Action with Snowpark. Luckily the API's are very similar. Often it's sufficient to change the import statement, the class we're extending and the session parameters type:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"  import com.snowflake.snowpark.functions._\n  import com.snowflake.snowpark.{DataFrame, Session}\n  import io.smartdatalake.workflow.action.snowflake.customlogic.CustomSnowparkDfTransformer\n\n  class ComputeDistanceTransformer extends CustomSnowparkDfTransformer {\n    def transform(session: Session, options: Map[String, String], df: DataFrame, dataObjectId: String) : DataFrame = {\n      ...\n")),(0,o.kt)("p",null,"If you have UDFs in your code, it gets trickier. The UDF Code gets serialized to Snowflake, details see ",(0,o.kt)("a",{parentName:"p",href:"https://docs.snowflake.com/en/developer-guide/snowpark/scala/creating-udfs.html"},"Snowpark UDFs"),". Special care must be taken to minimize the scope the UDF is defined in. Thats why we move the function into the companion object."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'  object ComputeDistanceTransformer {\n    def calculateDistanceInKilometer(depLat: Double, depLng: Double, arrLat: Double, arrLng: Double): Double = {\n      val AVERAGE_RADIUS_OF_EARTH_KM = 6371\n      val latDistance = Math.toRadians(depLat - arrLat)\n      val lngDistance = Math.toRadians(depLng - arrLng)\n      val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(depLat)) * Math.cos(Math.toRadians(arrLat)) * Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2)\n      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))\n      AVERAGE_RADIUS_OF_EARTH_KM * c\n    }\n    def getCalculateDistanceInKilometerUdf(session: Session) = {\n      // using only udf(...) function results in "SnowparkClientException: Error Code: 0207, Error message: No default Session found. Use <session>.udf.registerTemporary() to explicitly refer to a session."\n      session.udf.registerTemporary(ComputeDistanceTransformer.calculateDistanceInKilometer _)\n    }\n  }\n')),(0,o.kt)("p",null,'Note that we need to pass the Session to a function for registering the UDF. There is an Error 0207 if we use "udf" function (at least in snowpark version 1.2.0).\nFinally we need to adapt the call of the UDF as follows:'),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'df.withColumn("distance", ComputeDistanceTransformer.getCalculateDistanceInKilometerUdf(session)(col("dep_latitude_deg"),col("dep_longitude_deg"),col("arr_latitude_deg"), col("arr_longitude_deg")))\n')),(0,o.kt)("h2",{id:"compile-and-run"},"Compile and run"),(0,o.kt)("p",null,"Time to see if it works.\nLets build an update SDLB docker image with the updated SDLB version:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"  podman build -t sdl-spark .\n")),(0,o.kt)("p",null,"Then compile the code with the UDF:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'  mkdir .mvnrepo\n  podman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\n')),(0,o.kt)("p",null,"Download initial data with ",(0,o.kt)("inlineCode",{parentName:"p"},"--feed-sel download"),":"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"  podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\n")),(0,o.kt)("p",null,"Compute with ",(0,o.kt)("inlineCode",{parentName:"p"},"--feed-sel compute"),":"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"  podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel compute\n")),(0,o.kt)("p",null,"If the SDLB run was SUCCESSFUL, you should now see TEST.BTL_DISTANCES table in Snowpark.\nTo check that Spark was used for Action select-airport-cols and Snowpark for Action compute-distances, look for the following logs, e.g. SnowparkSubFeed for Action~compute-distances: "),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"  INFO  CopyAction - (Action~compute-distances) selected subFeedType SnowparkSubFeed [init-compute-distances]\n")),(0,o.kt)("h1",{id:"engine-selection---uncover-the-magic"},"Engine selection - uncover the magic"),(0,o.kt)("p",null,"Browsing through the logs it turns out that the Action~join-departures-airports was still executed with Spark (SparkSubFeed)!"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"  INFO  CustomDataFrameAction - (Action~join-departures-airports) selected subFeedType SparkSubFeed [init-join-departures-airports]\n")),(0,o.kt)("p",null,"An Action determines the engine to use in Init-phase by checking the supported types of inputs, outputs and transformations. In our case we have input DataObject stg-departures which is still a JsonFileDataObject, that can not create a Snowpark-DataFrame. As we would like to execute this join as well in Snowflake with Snowpark for performance reasons, lets create a SnowflakeTableDataObject int-departures and use it as input for Action~join-departures-airports."),(0,o.kt)("p",null,"Add a DataObject int-departures:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'  int-departures {\n    type = SnowflakeTableDataObject\n    connectionId = sf-con\n    table {\n      db = "test"\n      name = "int_departures"\n    }\n  }\n')),(0,o.kt)("p",null,"Add an Action copy-departures:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"  copy-departures {\n    type = CopyAction\n    inputId = stg-departures\n    outputId = int-departures\n    metadata {\n      feed = compute\n    }\n  }\n")),(0,o.kt)("p",null,"Fix inputs of Action join-departures-airports:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"  inputIds = [int-departures, int-airports]\n")),(0,o.kt)("p",null,"... and code of the first SQL transformer:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'  code = {\n    btl-connected-airports = """\n      select int_departures.estdepartureairport, int_departures.estarrivalairport, airports.*\n      from int_departures join int_airports airports on int_departures.estArrivalAirport = airports.ident\n    """\n')),(0,o.kt)("p",null,"Compute with Spark and Snowpark again by using ",(0,o.kt)("inlineCode",{parentName:"p"},"--feed-sel compute")," and browsing the logs, we can see that Action~join-departures-airports was executed with Snowpark:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"  (Action~join-departures-airports) selected subFeedType SnowparkSubFeed [init-join-departures-airports]\n")),(0,o.kt)("h1",{id:"summary"},"Summary"),(0,o.kt)("p",null,"We have seen that its quite easy to migrate SDLB pipelines to use Snowpark instead of Spark, also only partially for selected Actions. SDLB's support of different DataFrame-API-Engines allows to still benefit of all other features of SDLB, like having full early validation over the whole pipeline by checking the schemas needed by Actions later in the pipeline."),(0,o.kt)("p",null,"Migrating Scala code of custom transformations using Spark DataFrame API needs some adaptions of import statements, but the rest stays mostly 1:1 the same. UDFs are also supported and dont need changes, but there might be surprises regarding data types (Snowparks Variant-type is not the same as Sparks nested datatypes) and deployment of needed libraries. We might investigate that in future blog post."))}m.isMDXComponent=!0}}]);