"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[594],{3905:function(e,t,a){a.d(t,{Zo:function(){return c},kt:function(){return u}});var n=a(7294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},i=Object.keys(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var l=n.createContext({}),p=function(e){var t=n.useContext(l),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},c=function(e){var t=p(e.components);return n.createElement(l.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,i=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),m=p(a),u=r,h=m["".concat(l,".").concat(u)]||m[u]||d[u]||i;return a?n.createElement(h,o(o({ref:t},c),{},{components:a})):n.createElement(h,o({ref:t},c))}));function u(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=a.length,o=new Array(i);o[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:r,o[1]=s;for(var p=2;p<i;p++)o[p]=a[p];return n.createElement.apply(null,o)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},9395:function(e,t,a){a.r(t),a.d(t,{assets:function(){return c},contentTitle:function(){return l},default:function(){return u},frontMatter:function(){return s},metadata:function(){return p},toc:function(){return d}});var n=a(7462),r=a(3366),i=(a(7294),a(3905)),o=["components"],s={title:"Deployment on Databricks",description:"A brief example of deploying SDL on Databricks",slug:"sdl-databricks",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["Databricks","Cloud"],hide_table_of_contents:!1},l=void 0,p={permalink:"/blog/sdl-databricks",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2022-04-07-SDL_databricks/2022-04-07-Databricks.md",source:"@site/blog/2022-04-07-SDL_databricks/2022-04-07-Databricks.md",title:"Deployment on Databricks",description:"A brief example of deploying SDL on Databricks",date:"2022-04-07T00:00:00.000Z",formattedDate:"April 7, 2022",tags:[{label:"Databricks",permalink:"/blog/tags/databricks"},{label:"Cloud",permalink:"/blog/tags/cloud"}],readingTime:5.74,truncated:!0,authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],frontMatter:{title:"Deployment on Databricks",description:"A brief example of deploying SDL on Databricks",slug:"sdl-databricks",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["Databricks","Cloud"],hide_table_of_contents:!1},prevItem:{title:"Incremental historization using CDC and Airbyte MSSQL connector",permalink:"/blog/sdl-hist"},nextItem:{title:"Combine Spark and Snowpark to ingest and transform data in one pipeline",permalink:"/blog/sdl-snowpark"}},c={authorsImageUrls:[void 0]},d=[{value:"Lessons Learned",id:"lessons-learned",children:[],level:2}],m={toc:d};function u(e){var t=e.components,s=(0,r.Z)(e,o);return(0,i.kt)("wrapper",(0,n.Z)({},m,s,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"Many analytics applications are ported to the cloud, Data Lakes and Lakehouses in the cloud becoming more and more popular.\nThe ",(0,i.kt)("a",{parentName:"p",href:"https://databricks.com"},"Databricks")," platform provides an easy accessible and easy configurable way to implement a modern analytics platform.\nSmart Data Lake Builder on the other hand provides an open source, portable automation tool to load and transform the data."),(0,i.kt)("p",null,"In this article the deployment of Smart Data Lake Builder (SDLB) on ",(0,i.kt)("a",{parentName:"p",href:"https://databricks.com"},"Databricks")," is described. "),(0,i.kt)("p",null,"Before jumping in, it should be mentioned, that there are also many other methods to deploy SDLB in the cloud, e.g. using containers on Azure, Azure Kubernetes Service, Azure Synapse Clusters, Google Dataproc...\nThe present method provides the advantage of having many aspects taken care of by Databricks like Cluster management, Job scheduling and integrated data science notebooks.\nFurther, the presented SDLB pipeline is just a simple example, focusing on the integration into Databricks environment.\nSDLB provides a wide range of features and its full power is not revealed here. "),(0,i.kt)("p",null,"Let's get started:"),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("p",{parentName:"li"},(0,i.kt)("a",{parentName:"p",href:"https://databricks.com"},(0,i.kt)("strong",{parentName:"a"},"Databricks"))," accounts can be created as ",(0,i.kt)("a",{parentName:"p",href:"https://databricks.com/try-databricks"},"Free Trial")," or as ",(0,i.kt)("a",{parentName:"p",href:"https://community.databricks.com/s/login/SelfRegister"},"Community Account")),(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Account and Workspace creation are described in detail ",(0,i.kt)("a",{parentName:"li",href:"https://docs.databricks.com/getting-started/account-setup.html"},"here"),", there are few hints and modifications presented below."),(0,i.kt)("li",{parentName:"ul"},"I selected AWS backend, but there are conceptually no differences to the other providers. If you already have an Azure, AWS or Google Cloud account/subscription this can be used, otherwise you can register a trial subscription there. "))),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("p",{parentName:"li"},(0,i.kt)("strong",{parentName:"p"},"Workspace stack")," is created using the Quickstart as described in the documentation. When finished launch the Workspace.")),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("p",{parentName:"li"},(0,i.kt)("strong",{parentName:"p"},"Databricks CLI"),": for file transfer of configuration files, scripts and data, the ",(0,i.kt)("a",{parentName:"p",href:"https://docs.databricks.com/dev-tools/cli/index.html"},"Databricks CLI")," is installed locally. ",(0,i.kt)("strong",{parentName:"p"},"Configure"),' the CLI, using the Workspace URL and in the Workspace "Settings" -> "User Settings" -> "Access tokens" create a new token.')),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("p",{parentName:"li"},(0,i.kt)("strong",{parentName:"p"},"Cluster")," creation, in the Workspace open the ",(0,i.kt)("em",{parentName:"p"},"Cluster")," Creation form."),(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("p",{parentName:"li"},"Spark version: When selecting the ",(0,i.kt)("em",{parentName:"p"},"Databricks version")," pay attention to the related Spark version.\nThis needs to match the Spark version we build SDLB with later. Here, ",(0,i.kt)("inlineCode",{parentName:"p"},"10.4 LTS")," is selected with ",(0,i.kt)("inlineCode",{parentName:"p"},"Spark 3.2.1")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"Scala 2.12"),".\nAlternatively, SDLB can be build with a different Spark version, see also ",(0,i.kt)("a",{parentName:"p",href:"../../docs/architecture"},"Architecture")," for supported versions. ")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("p",{parentName:"li"},"typesafe library version correction script: the workspace currently includes version 1.2.1 from com.typesafe:config java library.\nSDLB relies on functions of a newer version (>1.3.0) of this library.\nThus, we provide a newer version of the com.typesafe:config java library in an initialization script: ",(0,i.kt)("em",{parentName:"p"},"Advanced options")," -> ",(0,i.kt)("em",{parentName:"p"},"Init Scripts")," specify ",(0,i.kt)("inlineCode",{parentName:"p"},"dbfs:/databricks/scripts/config-install.sh")),(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("p",{parentName:"li"},"Further, the script needs to be created and uploaded. You can use the following script in a local terminal or in a Databricks notebook:"),(0,i.kt)("pre",{parentName:"li"},(0,i.kt)("code",{parentName:"pre"},"cat << EOF >> ./config-install.sh\n#!/bin/bash\nwget -O /databricks/jars/-----config-1.4.1.jar https://repo1.maven.org/maven2/com/typesafe/config/1.4.1/config-1.4.1.jar\nEOF\ndatabricks fs mkdirs dbfs:/databricks/scripts\ndatabricks fs cp ./config-install.sh dbfs:/databricks/scripts/\n")),(0,i.kt)("p",{parentName:"li"},"Note: to double-check the library version I ran ",(0,i.kt)("inlineCode",{parentName:"p"},"grep typesafe pom.xml")," in the ",(0,i.kt)("a",{parentName:"p",href:"https://github.com/smart-data-lake/smart-data-lake.git"},"SmartDataLake")," source"),(0,i.kt)("p",{parentName:"li"},"Note: the added ",(0,i.kt)("inlineCode",{parentName:"p"},"-----")," will ensure that this ",(0,i.kt)("inlineCode",{parentName:"p"},".jar")," is preferred before the default Workspace Spark version (which starts with ",(0,i.kt)("inlineCode",{parentName:"p"},"----"),").\nIf you are curious you could double-check e.g. with a Workspace Shell Notebook running ",(0,i.kt)("inlineCode",{parentName:"p"},"ls /databricks/jars/*config*"))))))),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("p",{parentName:"li"},(0,i.kt)("strong",{parentName:"p"},"fat-jar"),":\nWe need to provide the SDLB sources and all required libraries. Therefore, we compile and pack the Scala code into a Jar including the dependencies. We use the ",(0,i.kt)("a",{parentName:"p",href:"https://github.com/smart-data-lake/getting-started.git"},"getting-started")," as dummy project, which itself pulls the SDLB sources. "),(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"download the ",(0,i.kt)("a",{parentName:"li",href:"https://github.com/smart-data-lake/getting-started.git"},"getting-started")," source and build it with the ",(0,i.kt)("inlineCode",{parentName:"li"},"-P fat-jar")," profile")),(0,i.kt)("pre",{parentName:"li"},(0,i.kt)("code",{parentName:"pre"},'podman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -DskipTests  -P fat-jar  -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\n')),(0,i.kt)("p",{parentName:"li"},"General build instructions can be found in the ",(0,i.kt)("a",{parentName:"p",href:"../../docs/getting-started/setup#compile-scala-classes"},"getting-started")," documentation.\nTherewith, the file ",(0,i.kt)("inlineCode",{parentName:"p"},"target/getting-started-1.0-jar-with-dependencies.jar")," is created.\nThe ",(0,i.kt)("em",{parentName:"p"},"fat-jar")," profile will include all required dependencies. The profile is defined in the ",(0,i.kt)("a",{parentName:"p",href:"https://github.com/smart-data-lake/smart-data-lake"},"smart-data-lake")," pom.xml.")),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("p",{parentName:"li"},"upload files"),(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},'JAR: in the "Workspace" -> your user -> create a directory ',(0,i.kt)("inlineCode",{parentName:"li"},"jars"),' and "import" the library using the link in "(To import a library, such as a jar or egg, click here)" and select the above created fat-jar to upload. As a result the jar will be listed in the Workspace directory. '),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"SDLB application"),": As an example a dataset from Airbnb NYC will be downloaded from Github, first written into a CSV file and later partially ported into a table. Therefore, the pipeline is defined first locally in a new file ",(0,i.kt)("inlineCode",{parentName:"li"},"application.conf"),":")),(0,i.kt)("pre",{parentName:"li"},(0,i.kt)("code",{parentName:"pre"},'dataObjects {\n  ext-ab-csv-web {\n    type = WebserviceFileDataObject\n    url = "https://raw.githubusercontent.com/adishourya/Airbnb/master/new-york-city-airbnb-open-data/AB_NYC_2019.csv"\n    followRedirects = true\n    readTimeoutMs=200000\n  }\n  stg-ab {\n    type = CsvFileDataObject\n    schema = """id integer, name string, host_id integer, host_name string, neighbourhood_group string, neighbourhood string, latitude double, longitude double, room_type  string, price integer, minimum_nights integer, number_of_reviews integer, last_review timestamp, reviews_per_month double, calculated_host_listings_count integer,          availability_365 integer"""\n    path = "file:///dbfs/data/~{id}"\n  }\n  int-ab {\n    type = DeltaLakeTableDataObject\n    path = "~{id}"\n    table {\n      db = "default"\n      name = "int_ab"\n      primaryKey = [id]\n    }\n  }\n}\n\nactions {\n  loadWeb2Csv {\n    type = FileTransferAction\n    inputId = ext-ab-csv-web\n    outputId = stg-ab\n    metadata {\n      feed = download\n    }\n  }\n  loadCsvLoc2Db {\n    type = CopyAction\n    inputId = stg-ab\n    outputId = int-ab\n    transformers = [{\n      type = SQLDfTransformer\n      code = "select id, name, host_id,host_name,neighbourhood_group,neighbourhood,latitude,longitude from stg_ab"\n    }]\n    metadata {\n      feed = copy\n    }\n  }\n}\n')),(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"upload using Databricks CLI ")),(0,i.kt)("pre",{parentName:"li"},(0,i.kt)("code",{parentName:"pre"},"databricks fs mkdirs dbfs:/conf/\ndatabricks fs cp application.conf dbfs:/conf/application.conf\n"))),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("p",{parentName:"li"},(0,i.kt)("strong",{parentName:"p"},"Job creation"),":\nHere, the Databricks job gets defined, specifying the SDL library and, the entry point and the arguments. Here we specify only the download feed.\nTherefore, open in the sidebar ",(0,i.kt)("em",{parentName:"p"},"Jobs")," -> ",(0,i.kt)("em",{parentName:"p"},"Create Job"),": "),(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Type"),": ",(0,i.kt)("inlineCode",{parentName:"li"},"JAR")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Main Class"),": ",(0,i.kt)("inlineCode",{parentName:"li"},"io.smartdatalake.app.LocalSmartDataLakeBuilder")),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"add")," ",(0,i.kt)("em",{parentName:"li"},"Dependent Libraries"),': "Workspace" -> select the file previously uploaded "getting-started..." file in the "jars" directory\n',(0,i.kt)("img",{alt:"jar select",src:a(4414).Z,width:"630",height:"652"})),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Cluster")," select the cluster created above with the corrected typesafe library"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Parameters"),": ",(0,i.kt)("inlineCode",{parentName:"li"},'["-c", "file:///dbfs/conf/", "--feed-sel", "download"]'),', which specifies the location of the SDLB configuration and selects the feed "download"\n',(0,i.kt)("img",{alt:"download task",src:a(9038).Z,width:"589",height:"641"})))),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("p",{parentName:"li"},(0,i.kt)("strong",{parentName:"p"},"Launch"),' the job:\nLaunch the job.\nWhen finished in the "Runs" section of that job we can verify the successful run status')),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("p",{parentName:"li"},(0,i.kt)("strong",{parentName:"p"},"Results"),"\nAfter running the SDLB pipeline the data should be downloaded into the staging file ",(0,i.kt)("inlineCode",{parentName:"p"},"stg_ab/result.csv")," and selected parts into the table ",(0,i.kt)("inlineCode",{parentName:"p"},"int_ab")),(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"csv file: in the first step we downloaded the CSV file. This can be verified, e.g. by inspecting the data directory in the Databricks CLI using ",(0,i.kt)("inlineCode",{parentName:"li"},"databricks fs ls dbfs:/data/stg-ab")," or running in a Workspace shell notebook ",(0,i.kt)("inlineCode",{parentName:"li"},"ls /dbfs/data/stg-ab")),(0,i.kt)("li",{parentName:"ul"},"database: in the second phase specific columns are put into the database. This can be verified in the Workspace -> Data -> default -> int_ab\n",(0,i.kt)("img",{alt:"select table",src:a(5192).Z,width:"726",height:"476"}),(0,i.kt)("img",{alt:"table",src:a(4254).Z,width:"1694",height:"821"}))),(0,i.kt)("div",{parentName:"li",className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"Note that our final table was defined as ",(0,i.kt)("inlineCode",{parentName:"p"},"DeltaLakeTableDataObject"),".\nWith that, Smart Data Lake Builder automatically generates a Delta Lake Table in your Databricks workspace. "))))),(0,i.kt)("h2",{id:"lessons-learned"},"Lessons Learned"),(0,i.kt)("p",null,"There are a few steps necessary, including building and uploading SDLB.\nFurther, we need to be careful with the used versions of the underlying libraries.\nWith these few steps we can reveal the power of SDLB and Databricks, creating a portable and reproducible pipeline into a Databricks Lakehouse."))}u.isMDXComponent=!0},4414:function(e,t,a){t.Z=a.p+"assets/images/add_library-004c4b45355447e0191352a0c3d1e26c.png"},9038:function(e,t,a){t.Z=a.p+"assets/images/download_task-104bd82fd84e5fb593b161282586942e.png"},5192:function(e,t,a){t.Z=a.p+"assets/images/select_table-7e7e1d0657aff229863bfe5d87b16c2e.png"},4254:function(e,t,a){t.Z=a.p+"assets/images/table-f62d6019e5cbbb9c404209061ceb6b80.png"}}]);