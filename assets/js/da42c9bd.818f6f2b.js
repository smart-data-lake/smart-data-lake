"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[5513],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>u});var n=a(7294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},i=Object.keys(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var l=n.createContext({}),d=function(e){var t=n.useContext(l),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},c=function(e){var t=d(e.components);return n.createElement(l.Provider,{value:t},e.children)},p="mdxType",h={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,i=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),p=d(a),m=r,u=p["".concat(l,".").concat(m)]||p[m]||h[m]||i;return a?n.createElement(u,o(o({ref:t},c),{},{components:a})):n.createElement(u,o({ref:t},c))}));function u(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=a.length,o=new Array(i);o[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[p]="string"==typeof e?e:r,o[1]=s;for(var d=2;d<i;d++)o[d]=a[d];return n.createElement.apply(null,o)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},3824:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>h,frontMatter:()=>i,metadata:()=>s,toc:()=>d});var n=a(7462),r=(a(7294),a(3905));const i={title:"Incremental historization using CDC and Airbyte MSSQL connector",description:"Tracking Data Changes of MSSQL databases with and without CDC",slug:"sdl-hist",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["historization","MSSQL","incremental","CDC"],hide_table_of_contents:!1},o=void 0,s={permalink:"/blog/sdl-hist",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2022-05-11-SDL-historization/2022-05-11-historization.md",source:"@site/blog/2022-05-11-SDL-historization/2022-05-11-historization.md",title:"Incremental historization using CDC and Airbyte MSSQL connector",description:"Tracking Data Changes of MSSQL databases with and without CDC",date:"2022-05-11T00:00:00.000Z",formattedDate:"May 11, 2022",tags:[{label:"historization",permalink:"/blog/tags/historization"},{label:"MSSQL",permalink:"/blog/tags/mssql"},{label:"incremental",permalink:"/blog/tags/incremental"},{label:"CDC",permalink:"/blog/tags/cdc"}],readingTime:12.525,hasTruncateMarker:!0,authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],frontMatter:{title:"Incremental historization using CDC and Airbyte MSSQL connector",description:"Tracking Data Changes of MSSQL databases with and without CDC",slug:"sdl-hist",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["historization","MSSQL","incremental","CDC"],hide_table_of_contents:!1},prevItem:{title:"Housekeeping",permalink:"/blog/sdl-housekeeping"},nextItem:{title:"Deployment on Databricks",permalink:"/blog/sdl-databricks"}},l={authorsImageUrls:[void 0]},d=[{value:"Test Case",id:"test-case",level:2},{value:"Prerequisites",id:"prerequisites",level:2},{value:"Prepare Source Database",id:"prepare-source-database",level:2},{value:"Define Workflow",id:"define-workflow",level:2},{value:"Spark Settings",id:"spark-settings",level:3},{value:"Connection",id:"connection",level:3},{value:"DataObjects",id:"dataobjects",level:3},{value:"Actions",id:"actions",level:3},{value:"Run",id:"run",level:2},{value:"Change Data Capture",id:"change-data-capture",level:2},{value:"Enable CDC on the SQL Server",id:"enable-cdc-on-the-sql-server",level:3},{value:"Airbyte MSSQL connector",id:"airbyte-mssql-connector",level:3},{value:"SDLB configuration",id:"sdlb-configuration",level:3},{value:"DataObject",id:"dataobject",level:4},{value:"Action",id:"action",level:4},{value:"Summary",id:"summary",level:2}],c={toc:d},p="wrapper";function h(e){let{components:t,...i}=e;return(0,r.kt)(p,(0,n.Z)({},c,i,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("p",null,"In many cases datasets have no constant live. New data points are created, values changed and data expires. We are interested in keeping track of all these changes.\nThis article first presents collecting data utilizing ",(0,r.kt)("strong",{parentName:"p"},"JDBC")," and ",(0,r.kt)("strong",{parentName:"p"},"deduplication on the fly"),". Then, a ",(0,r.kt)("strong",{parentName:"p"},"Change Data Capture")," (CDC) enabled (MS)SQL table will be transferred and historized in the data lake using the ",(0,r.kt)("strong",{parentName:"p"},"Airbyte MS SQL connector")," supporting CDC. Methods for reducing the computational and storage efforts are mentioned."),(0,r.kt)("p",null,"In the ",(0,r.kt)("a",{parentName:"p",href:"../../docs/getting-started/part-2/historical-data"},"getting-started -> part2 -> keeping historical data")," historization is already introduced briefly. Here, we go in slightly more detail and track data originating from an MS SQL database. For the sake of simplicity, the tools and systems are deployed in Podman containers, including SDLB, MSSQL server, as well as the metastore and polynote. "),(0,r.kt)("p",null,"Here, a workflow is modeled, gathering data from a (MS)SQL database to the Data Lake. Therefore, the following steps will be performed:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"initializing a MS SQL server"),(0,r.kt)("li",{parentName:"ul"},"importing data into MS SQL table"),(0,r.kt)("li",{parentName:"ul"},"injecting data into the data lake"),(0,r.kt)("li",{parentName:"ul"},"modifying data on the SQL server side"),(0,r.kt)("li",{parentName:"ul"},"re-copy / update data into data lake")),(0,r.kt)("p",null,"The data will be inspected and monitored using a Polynote notebook."),(0,r.kt)("h2",{id:"test-case"},"Test Case"),(0,r.kt)("p",null,"As a test case a ",(0,r.kt)("a",{parentName:"p",href:"https://www.kaggle.com/datasets/datasnaek/chess"},"Chess Game Dataset")," is selected. This data is a set of 20058 rows, in total 7MB. This is still faily small, but should be kind of representative."),(0,r.kt)("p",null,"The dataset will be imported into the SQL server using the ",(0,r.kt)("a",{target:"_blank",href:a(7860).Z},"db_init_chess.sql")," script, which should be copied into the ",(0,r.kt)("inlineCode",{parentName:"p"},"config")," directory. "),(0,r.kt)("p",null,"It should be noted that there are a duplicates in the dataset. In the first case, the ",(0,r.kt)("em",{parentName:"p"},"deduplication")," will be performed when the data is ported into the data lake (see configuration below). The procedure for table creation and modification is described below. "),(0,r.kt)("h2",{id:"prerequisites"},"Prerequisites"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Podman installation"),(0,r.kt)("li",{parentName:"ul"},"SDLB with metastore and Polynote by cloning the getting-started example: ",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"git clone https://github.com/smart-data-lake/getting-started.git SDL_sql\ncd SDL_sql\nunzip part2.additional-files.zip\n")),(0,r.kt)("admonition",{parentName:"li",title:'"Directory Naming"',type:"note"},(0,r.kt)("p",{parentName:"admonition"},'Note: The directory name "SDL_sql" will be related to the pod created later and thus to the specified commands below.'))),(0,r.kt)("li",{parentName:"ul"},"Utilizing JDBC to MS SQL server, SDLB required this additional dependency. Therefore, add the following dependency to the ",(0,r.kt)("inlineCode",{parentName:"li"},"pom.xml"),":",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-xml"},"<dependency>\n    <groupId>com.microsoft.sqlserver</groupId>\n    <artifactId>mssql-jdbc</artifactId>\n    <version>10.2.0.jre11</version>\n</dependency>\n"))),(0,r.kt)("li",{parentName:"ul"},"build sdl-spark: ",(0,r.kt)("inlineCode",{parentName:"li"},"podman build -t sdl-spark .")),(0,r.kt)("li",{parentName:"ul"},"build the SDLB objects: ",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},'mkdir .mvnrepo\npodman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\n'))),(0,r.kt)("li",{parentName:"ul"},"download the test case data from ",(0,r.kt)("a",{parentName:"li",href:"https://www.kaggle.com/datasets/datasnaek/chess/download"},"Kaggle")," and unzip into ",(0,r.kt)("inlineCode",{parentName:"li"},"SDL_sql/data")," directory"),(0,r.kt)("li",{parentName:"ul"},"copy polynote notebook ",(0,r.kt)("a",{target:"_blank",href:a(6238).Z},"sql_data_monitor.ipynb")," for later inspection into the ",(0,r.kt)("inlineCode",{parentName:"li"},"polynote/notebooks")," directory")),(0,r.kt)("admonition",{type:"warning"},(0,r.kt)("p",{parentName:"admonition"},"  The notebook will only be editable if the permissions are changed to be writable by other users ",(0,r.kt)("inlineCode",{parentName:"p"},"chmod -R 777 polynote/notebooks"))),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"script for creating table on the SQL server: ",(0,r.kt)("a",{target:"_blank",href:a(7860).Z},"db_init_chess.sql")," into the ",(0,r.kt)("inlineCode",{parentName:"li"},"config")," directory"),(0,r.kt)("li",{parentName:"ul"},"script for modifying the table on the SQL server: ",(0,r.kt)("a",{target:"_blank",href:a(4413).Z},"db_mod_chess.sql")," into the ",(0,r.kt)("inlineCode",{parentName:"li"},"config")," directory"),(0,r.kt)("li",{parentName:"ul"},"a restart script ",(0,r.kt)("a",{target:"_blank",href:a(8215).Z},"restart_databases.sh")," is provided to clean and restart from scratch, including: stopping the containers, cleaning databases, freshly starting the containers and initializing the SQL database ")),(0,r.kt)("h2",{id:"prepare-source-database"},"Prepare Source Database"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"start the pod with the metastore and polynote: ",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"mkdir -p data/_metastore\n./part2/podman-compose.sh #use the script from the getting-started guide\n"))),(0,r.kt)("li",{parentName:"ul"},"start the MS SQL server: ",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},'podman run -d --pod sdl_sql --hostname mssqlserver --add-host mssqlserver:127.0.0.1 --name mssql -v ${PWD}/data:/data  -v ${PWD}/config:/config -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=%abcd1234%" mcr.microsoft.com/mssql/server:2017-latest\n'))),(0,r.kt)("li",{parentName:"ul"},"initialize the database: ",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -i /config/db_init_chess.sql\n"))),(0,r.kt)("li",{parentName:"ul"},"list the table: ",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -Q \"SELECT count(*) FROM foobar.dbo.chess\"\npodman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -Q \"SELECT * FROM foobar.dbo.chess WHERE id = '079kHDqh'\"\n")),"This should report 20058 row and we see an example of duplicates.")),(0,r.kt)("admonition",{type:"note"},(0,r.kt)("p",{parentName:"admonition"},"  This could be shortened, by just calling the ",(0,r.kt)("a",{target:"_blank",href:a(8215).Z},(0,r.kt)("code",null,"bash restart_databases.sh")),", which contains the above commands, after stopping the containers and cleaning directories. ")),(0,r.kt)("h2",{id:"define-workflow"},"Define Workflow"),(0,r.kt)("p",null,"The SDLB configuration file consists of:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"global settings for the metastore "),(0,r.kt)("li",{parentName:"ul"},"connection details "),(0,r.kt)("li",{parentName:"ul"},"data objects and"),(0,r.kt)("li",{parentName:"ul"},"action")),(0,r.kt)("p",null,"Create the ",(0,r.kt)("inlineCode",{parentName:"p"},"config/chess.conf")," file with the following described sections or copy the ",(0,r.kt)("a",{target:"_blank",href:a(4150).Z},"full script"),"."),(0,r.kt)("h3",{id:"spark-settings"},"Spark Settings"),(0,r.kt)("p",null,"For the metastore, the location, driver and access is defined. Further, the amount of tasks and partitions are limited, due to our reasonable small problem size."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-hocon"},'global {\n  spark-options {\n    "spark.hadoop.javax.jdo.option.ConnectionURL" = "jdbc:derby://metastore:1527/db;create=true"\n    "spark.hadoop.javax.jdo.option.ConnectionDriverName" = "org.apache.derby.jdbc.ClientDriver"\n    "spark.hadoop.javax.jdo.option.ConnectionUserName" = "sa"\n    "spark.hadoop.javax.jdo.option.ConnectionPassword" = "1234"\n    "spark.databricks.delta.snapshotPartitions" = 2\n    "spark.sql.shuffle.partitions" = 2\n  }\n}  \n')),(0,r.kt)("h3",{id:"connection"},"Connection"),(0,r.kt)("p",null,"The connection to the local MS SQL server is specified using JDBC settings and clear text authentication specification. In practice, a more secure authentication mode should be selected, e.g. injection by environment variables. "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-hocon"},'connections {\n  localSql {\n    type = JdbcTableConnection\n    url = "jdbc:sqlserver://mssqlserver:1433;encrypt=true;trustServerCertificate=true"\n    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver\n    authMode {\n      type = BasicAuthMode\n      userVariable = "CLEAR#sa"\n      passwordVariable = "CLEAR#%abcd1234%"\n    }\n  }\n}\n')),(0,r.kt)("h3",{id:"dataobjects"},"DataObjects"),(0,r.kt)("p",null,"In a first place, two DataObjects are defined. The ",(0,r.kt)("inlineCode",{parentName:"p"},"ext-chess")," defining the external source (the table on the MS SQL server), using JDBC connection. The ",(0,r.kt)("inlineCode",{parentName:"p"},"int-chess")," defines a delta lake table object as integration layer as targets for our ingestion/historization action. "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-hocon"},'dataObjects {\n  ext-chess {\n    type = JdbcTableDataObject\n    connectionId = localSql\n    table = {\n      name = "dbo.chess"\n      db = "foobar"\n    }\n  }\n  int-chess {\n    type = DeltaLakeTableDataObject\n    path = "~{id}"\n    table {\n      db = "default"\n      name = "int_chess"\n      primaryKey = [id]\n    }\n  }\n  #...\n}\n')),(0,r.kt)("h3",{id:"actions"},"Actions"),(0,r.kt)("p",null,"The ",(0,r.kt)("inlineCode",{parentName:"p"},"histData")," action specifies the copy and historization of data, by setting the type ",(0,r.kt)("strong",{parentName:"p"},"HistorizeAction"),". Therewith, the incoming data will be joined with the existing data. Each data point gets two additional values: ",(0,r.kt)("strong",{parentName:"p"},"dl_ts_captured")," (time the data is captured in the data lake) and ",(0,r.kt)("strong",{parentName:"p"},"dl_ts_delimited")," (time of invalidation). In case of a data record change, the original row gets invalidated (with the current time) and a new row is added with the current time as capturing value. As long as the data point is active, ",(0,r.kt)("strong",{parentName:"p"},"dl_ts_delimited")," is set to max date `9999-12-31 23:59:59.999999."),(0,r.kt)("p",null,"The default HistorizationAction algorithm compares all new data with all the existing data, row by row ",(0,r.kt)("strong",{parentName:"p"},"AND")," column by column. Further, the complete joined table is re-written to the data lake.\nFor DataObjects supporting transactions HistorizeAction provides an algorithm using a merge operation to do the historization. By selecting ",(0,r.kt)("inlineCode",{parentName:"p"},"mergeModeEnable = true")," the resulting table gets another column with ",(0,r.kt)("inlineCode",{parentName:"p"},"dl_hash"),". This hash is used to compare rows much more efficiently. Not every column need to be compared, only the hash. Further, already existing rows (identified by the hash), do not need to be re-written. The merge operation applies only the needed inserts and updates to the output DataObject."),(0,r.kt)("p",null,"Furthermore, a ",(0,r.kt)("em",{parentName:"p"},"transformer")," needs to be added to deduplicate the input data, which has duplicated rows with slightly different game times. This is needed as HistorizationAction expects the input to be unique over the primary key of the output DataObject."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-hocon"},'actions {\n  histData {\n    type = HistorizeAction\n    mergeModeEnable = true\n    inputId = ext-chess\n    outputId = int-chess\n    transformers = [{\n      type = ScalaCodeDfTransformer\n      code = """\n        import org.apache.spark.sql.{DataFrame, SparkSession}\n        (session:SparkSession, options:Map[String, String], df:DataFrame, dataObjectId:String) => {\n          import session.implicits._\n          df.dropDuplicates(Seq("id"))\n        }\n      """\n   }]\n   metadata {\n      feed = download\n    }\n  }\n  #...\n}\n')),(0,r.kt)("p",null,"The full configuration looks like ",(0,r.kt)("a",{target:"_blank",href:a(4150).Z},"chess.conf"),". Note that there are already further DataObjects and Actions defined, described and used later. "),(0,r.kt)("h2",{id:"run"},"Run"),(0,r.kt)("p",null,'The Pod with metastore, polynote and the "external" SQL server should already being running. Now the SDLB container is launched within the same POD and the action histData ingests the data into the data lake:'),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman run --hostname localhost -e SPARK_LOCAL_HOSTNAME=localhost --rm --pod sdl_sql -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark --config /mnt/config --feed-sel ids:histData\n")),(0,r.kt)("p",null,"The data can be inspected using the Polynote notebook (",(0,r.kt)("inlineCode",{parentName:"p"},"sql_data_monitor.ipynb")," downloaded above), which can be launched using ",(0,r.kt)("a",{parentName:"p",href:"http://localhost:8192/notebook/sql_data_monitor.ipynb"},"Polynote (click here)"),". "),(0,r.kt)("p",null,"Now, let's assume a change in the source database. Here the ",(0,r.kt)("inlineCode",{parentName:"p"},"victory_status")," ",(0,r.kt)("inlineCode",{parentName:"p"},"outoftime")," is renamed to ",(0,r.kt)("inlineCode",{parentName:"p"},"overtime"),". Furthermore one entry is deleted. Run script using:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -i /config/db_mod_chess.sql\n")),(0,r.kt)("p",null,"Note: 1680 rows were changed + 1 row deleted."),(0,r.kt)("p",null,"Furthermore, the data lake gets updated using the same command as above:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman run --hostname localhost -e SPARK_LOCAL_HOSTNAME=localhost --rm --pod sdl_sql -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark --config /mnt/config --feed-sel ids:histData\n")),(0,r.kt)("p",null,"In the ",(0,r.kt)("a",{parentName:"p",href:"http://localhost:8192/notebook/sql_data_monitor.ipynb"},"Polynote (click here)")," sql_data_monitor.ipynb, the data lake table can be inspected again. The table and its additional columns are presented, as well as the original and updated rows for a modified row (",(0,r.kt)("em",{parentName:"p"},"id = QQ3iIM2V"),") and the deleted row (",(0,r.kt)("em",{parentName:"p"},"id = 009mKOEz"),")."),(0,r.kt)("p",null,(0,r.kt)("img",{alt:"polynote example output",src:a(310).Z,width:"911",height:"385"})),(0,r.kt)("h2",{id:"change-data-capture"},"Change Data Capture"),(0,r.kt)("p",null,"So far the whole table is collected and compared with the existing data lake table.\nVarious databases support Change Data Capture (CDC). CDC already keeps track of changes similar to the comparision done by the historization feature of SDL.\nThis can be used to optimize performance of the historization feature, gathering only database updates, reducing the amount of transferred and compared data significantly. Therewith, only data changed (created, modified, or deleted) since the last synchronisation will be read from the database. It needs the status of the last synchronisation to be attached to the successful stream. This ",(0,r.kt)("strong",{parentName:"p"},"state")," is handled in SDLB. "),(0,r.kt)("p",null,"In the following, an example is presented utilizing the MS SQL server CDC feature. Since the implemented JDBC connector cannot handle CDC data, an Airbyte connector is utilized, see ",(0,r.kt)("a",{parentName:"p",href:"https://docs.airbyte.com/understanding-airbyte/cdc/"},"Airbyte CDC")," for details."),(0,r.kt)("h3",{id:"enable-cdc-on-the-sql-server"},"Enable CDC on the SQL Server"),(0,r.kt)("p",null,"First the SQL server need to be configured to have a table ",(0,r.kt)("a",{parentName:"p",href:"https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver15"},"CDC enabled"),". Additionally to the used table ",(0,r.kt)("em",{parentName:"p"},"chess"),", another table is created with the CDC feature enabled. This table ",(0,r.kt)("em",{parentName:"p"},"chess_cdc")," is created by copying and deduplicating the original table. The table creation is already performed in the above introduced and used ",(0,r.kt)("a",{target:"_blank",href:a(7860).Z},"MS SQL initalization")," script. In practice, further SQL settings should be considered, including proper user and permissions specification, and an adaptation of the retention period (3 days default). "),(0,r.kt)("p",null,"Furthermore, the SQL agent need to be enabled. Therefore, the database need to be restarted (here container is restarted). This is handled in the above mentioned ",(0,r.kt)("a",{target:"_blank",href:a(8215).Z},"restart_databases.sh")," script. If not already used above, run:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"bash restart_databases.sh\n")),(0,r.kt)("p",null,"Let's double check the CDC enabled table:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -Q \"SELECT * FROM foobar.cdc.dbo_chess_cdc_CT where id = '079kHDqh'\"\n")),(0,r.kt)("p",null,"Here the first 5 columns are added for the CDC.\nIt should be noted that CDC additional data vary by implementation from different DB products."),(0,r.kt)("h3",{id:"airbyte-mssql-connector"},"Airbyte MSSQL connector"),(0,r.kt)("p",null,"Since Sparks JDBC data source (used above) does not support CDC data, the connector is changed to ",(0,r.kt)("a",{parentName:"p",href:"https://docs.airbyte.com/integrations/sources/mssql"},"Airbyte MSSQL"),".\nIn contrast to the article ",(0,r.kt)("a",{parentName:"p",href:"sdl-airbyte"},"Using Airbyte connector to inspect github data"),", where Airbyte github connector ran as python script, here the connector runs as a container within the SDLB container.\nI targeted a setup using: WSL2, SDLB with podman, and the Airbyte container with podman in the SDLB container. Unfortunately, there are issues with fuse overlay filesystem when using container in container with podman. Therefore, I switched to ",(0,r.kt)("a",{parentName:"p",href:"https://buildah.io/"},"buildah")," in the SDLB container. Unfortunately, the entrypoint is not recognized as expected. As a workaround the following script corrects this.\nI guess in another environment, e.g. in a cloud environment or just using docker in docker, this would work out of the box. "),(0,r.kt)("p",null,"Here are my steps to get it running:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"add buildah and the Airbyte container to the SDLB ",(0,r.kt)("a",{target:"_blank",href:a(3665).Z},"Dockerfile")," (just before the entrypoint):",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"RUN apt-get update\nRUN apt-get -y install buildah\nRUN echo 'unqualified-search-registries=[\"docker.io\"]' >> /etc/containers/registries.conf\nRUN buildah --storage-driver=vfs from --name airbyte-mssql docker.io/airbyte/source-mssql\n"))),(0,r.kt)("li",{parentName:"ul"},"rebuild the SDLB container: ",(0,r.kt)("inlineCode",{parentName:"li"},"podman build -t sdl-spark .")),(0,r.kt)("li",{parentName:"ul"},"workaround script to parse all arguments correctly in the Airbyte container while using buildah without the proper entrypoint. Copy ",(0,r.kt)("a",{target:"_blank",href:a(7752).Z},"start_buildah.sh")," script into ",(0,r.kt)("inlineCode",{parentName:"li"},"config")," directory")),(0,r.kt)("h3",{id:"sdlb-configuration"},"SDLB configuration"),(0,r.kt)("p",null,"Now, the related DataObjects and Action are added to the SDLB configuration ",(0,r.kt)("a",{target:"_blank",href:a(4150).Z},"config/chess.conf"),"."),(0,r.kt)("h4",{id:"dataobject"},"DataObject"),(0,r.kt)("p",null,"As a source object the AirbyteDataObject is used. Again the MSSQL server with the user credentials are specified. Further, in the streamName the table is selected. The cmd specifies how to run Airbyte. Here the mentioned workaround script is called in the container. As target, again a Delta Lake table is chosen, here called ",(0,r.kt)("inlineCode",{parentName:"p"},"int_chess_cdc"),". Practically, we would prevent of duplicating tables, here we create a new one to provide the possibility to compare both results. "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-hocon"},'dataObjects {\n    ext-chess-cdc {\n    type = AirbyteDataObject\n    config = {\n      host = "mssqlserver"\n      port = 1433\n      database = "foobar"\n      username = "sa"\n      password = "%abcd1234%"\n      replication_method = "CDC"\n    },\n    streamName = "chess_cdc",\n    cmd = {\n      type = DockerRunScript\n      name = "airbyte_source-mssql"\n      image = "airbyte-mssql"\n      linuxDockerCmd = "bash /mnt/config/start_buildah.sh"\n    }\n  }\n  int-chess-cdc {\n    type = DeltaLakeTableDataObject\n    path = "~{id}"\n    table {\n      db = "default"\n      name = "int_chess_cdc"\n      primaryKey = [id]\n    }\n  }\n}\n')),(0,r.kt)("h4",{id:"action"},"Action"),(0,r.kt)("p",null,"Again the SDLB action ",(0,r.kt)("em",{parentName:"p"},"HistorizeAction")," with ",(0,r.kt)("em",{parentName:"p"},"mergeModeEnabled")," is selected. Further, the incremental execution mode is enabled. With CDC we get only changed data, which will be merged with the SDL existing table. Further, the column and value need to be specified to identify deleted data points. Depending on the CDC implementation this could be the deletion date (like we have here) or an operation flag mentioning the deletion operation as code, or even differently. SDLB expects a fixed value for deletion. That's why we specify an ",(0,r.kt)("inlineCode",{parentName:"p"},"AdditionalColumnsTransformer")," transformer to first create an intermediate column mapping any date to ",(0,r.kt)("em",{parentName:"p"},"true"),". "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-hocon"},'actions {\n  histDataAirbyte {\n    type = HistorizeAction\n    mergeModeEnable = true\n    executionMode = { type = DataObjectStateIncrementalMode }\n    inputId = ext-chess-cdc\n    outputId = int-chess-cdc\n    mergeModeCDCColumn = "cdc_deleted"\n    mergeModeCDCDeletedValue = true\n    transformers = [{\n      type = AdditionalColumnsTransformer\n      additionalDerivedColumns = {cdc_deleted = "_ab_cdc_deleted_at is not null"}\n    }]\n    metadata {\n      feed = download\n    }\n  }\n}\n')),(0,r.kt)("p",null,"Finally, SDLB is launched using the same settings as above, now with the feed ",(0,r.kt)("inlineCode",{parentName:"p"},"ids:histDataAirbyte")," and specifying the ",(0,r.kt)("em",{parentName:"p"},"state")," directory and name:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman run --hostname localhost -e SPARK_LOCAL_HOSTNAME=localhost --rm --pod sdl_sql -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark --config /mnt/config --feed-sel ids:histDataAirbyte --state-path /mnt/data/state -n SDL_sql\n")),(0,r.kt)("p",null,"Again the SQL database modification is processed using ",(0,r.kt)("a",{target:"_blank",href:a(8007).Z},"config/db_mod_chess_cdc.sql"),": "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -i /config/db_mod_chess_cdc.sql\n")),(0,r.kt)("p",null,"And the SDL update with the same command as just used:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman run --hostname localhost -e SPARK_LOCAL_HOSTNAME=localhost --rm --pod sdl_sql -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark --config /mnt/config --feed-sel ids:histDataAirbyte --state-path /mnt/data/state -n SDL_sql\n")),(0,r.kt)("p",null,"Again the delta lake tables can be inspected using ",(0,r.kt)("a",{parentName:"p",href:"http://localhost:8192/notebook/sql_data_monitor.ipynb"},"Polynote (click here)"),"."),(0,r.kt)("h2",{id:"summary"},"Summary"),(0,r.kt)("p",null,"Smart Data Lake Builder (SDLB) provides a powerful tool to capture data from SQL databases and provides features to track changes, as well as optimized procedures to process Change Data Capture (CDC) data. There are various connectors to interact with SQL databases (e.g. JDBC and Airbyte). Powerful transformers help to handle the data stream within the action, e.g. for deduplication. Furthermore, the join and merge of new data with existing tables, as well as the writing of the data is optimized, reducing the computational an IO efforts with standard HistorizeAction."))}h.isMDXComponent=!0},3665:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/Dockerfile-106772189af8db20db3f24d48737b13f.txt"},4150:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/chess-49c77f7e01ad6fb42540b06f4a93ac75.conf"},7860:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/db_init_chess-f66d528d1f63d688cc156a7fb0e9516c.sql"},4413:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/db_mod_chess-b1cad59e3d4df7cc8a098caf0b9f405e.sql"},8007:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/db_mod_chess_cdc-11e5ec52cc1ebb7c34d5ae65ed898038.sql"},8215:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/restart_databases-dee8cd590e385d628d1f8df38960a7c0.sh"},6238:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/sql_data_monitor-aa012c7e78c37559e44fcc25991e38a9.ipynb"},7752:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/start_buildah-8928533426fd919b1146d8ddf307201b.sh"},310:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/images/historization_res-dbcab5954c3121455ffdc2dd351ccf93.png"}}]);