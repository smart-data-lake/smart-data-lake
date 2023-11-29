"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[5513],{4951:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>d,contentTitle:()=>r,default:()=>h,frontMatter:()=>i,metadata:()=>o,toc:()=>l});var a=n(5893),s=n(1151);const i={title:"Incremental historization using CDC and Airbyte MSSQL connector",description:"Tracking Data Changes of MSSQL databases with and without CDC",slug:"sdl-hist",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["historization","MSSQL","incremental","CDC"],hide_table_of_contents:!1},r=void 0,o={permalink:"/blog/sdl-hist",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2022-05-11-SDL-historization/2022-05-11-historization.md",source:"@site/blog/2022-05-11-SDL-historization/2022-05-11-historization.md",title:"Incremental historization using CDC and Airbyte MSSQL connector",description:"Tracking Data Changes of MSSQL databases with and without CDC",date:"2022-05-11T00:00:00.000Z",formattedDate:"May 11, 2022",tags:[{label:"historization",permalink:"/blog/tags/historization"},{label:"MSSQL",permalink:"/blog/tags/mssql"},{label:"incremental",permalink:"/blog/tags/incremental"},{label:"CDC",permalink:"/blog/tags/cdc"}],readingTime:12.525,hasTruncateMarker:!0,authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],frontMatter:{title:"Incremental historization using CDC and Airbyte MSSQL connector",description:"Tracking Data Changes of MSSQL databases with and without CDC",slug:"sdl-hist",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["historization","MSSQL","incremental","CDC"],hide_table_of_contents:!1},unlisted:!1,prevItem:{title:"Housekeeping",permalink:"/blog/sdl-housekeeping"},nextItem:{title:"Deployment on Databricks",permalink:"/blog/sdl-databricks"}},d={authorsImageUrls:[void 0]},l=[{value:"Test Case",id:"test-case",level:2},{value:"Prerequisites",id:"prerequisites",level:2},{value:"Prepare Source Database",id:"prepare-source-database",level:2},{value:"Define Workflow",id:"define-workflow",level:2},{value:"Spark Settings",id:"spark-settings",level:3},{value:"Connection",id:"connection",level:3},{value:"DataObjects",id:"dataobjects",level:3},{value:"Actions",id:"actions",level:3},{value:"Run",id:"run",level:2},{value:"Change Data Capture",id:"change-data-capture",level:2},{value:"Enable CDC on the SQL Server",id:"enable-cdc-on-the-sql-server",level:3},{value:"Airbyte MSSQL connector",id:"airbyte-mssql-connector",level:3},{value:"SDLB configuration",id:"sdlb-configuration",level:3},{value:"DataObject",id:"dataobject",level:4},{value:"Action",id:"action",level:4},{value:"Summary",id:"summary",level:2}];function c(e){const t={a:"a",admonition:"admonition",code:"code",em:"em",h2:"h2",h3:"h3",h4:"h4",img:"img",li:"li",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,s.a)(),...e.components};return(0,a.jsxs)(a.Fragment,{children:[(0,a.jsxs)(t.p,{children:["In many cases datasets have no constant live. New data points are created, values changed and data expires. We are interested in keeping track of all these changes.\nThis article first presents collecting data utilizing ",(0,a.jsx)(t.strong,{children:"JDBC"})," and ",(0,a.jsx)(t.strong,{children:"deduplication on the fly"}),". Then, a ",(0,a.jsx)(t.strong,{children:"Change Data Capture"})," (CDC) enabled (MS)SQL table will be transferred and historized in the data lake using the ",(0,a.jsx)(t.strong,{children:"Airbyte MS SQL connector"})," supporting CDC. Methods for reducing the computational and storage efforts are mentioned."]}),"\n",(0,a.jsxs)(t.p,{children:["In the ",(0,a.jsx)(t.a,{href:"../../docs/getting-started/part-2/historical-data",children:"getting-started -> part2 -> keeping historical data"})," historization is already introduced briefly. Here, we go in slightly more detail and track data originating from an MS SQL database. For the sake of simplicity, the tools and systems are deployed in Podman containers, including SDLB, MSSQL server, as well as the metastore and polynote."]}),"\n",(0,a.jsx)(t.p,{children:"Here, a workflow is modeled, gathering data from a (MS)SQL database to the Data Lake. Therefore, the following steps will be performed:"}),"\n",(0,a.jsxs)(t.ul,{children:["\n",(0,a.jsx)(t.li,{children:"initializing a MS SQL server"}),"\n",(0,a.jsx)(t.li,{children:"importing data into MS SQL table"}),"\n",(0,a.jsx)(t.li,{children:"injecting data into the data lake"}),"\n",(0,a.jsx)(t.li,{children:"modifying data on the SQL server side"}),"\n",(0,a.jsx)(t.li,{children:"re-copy / update data into data lake"}),"\n"]}),"\n",(0,a.jsx)(t.p,{children:"The data will be inspected and monitored using a Polynote notebook."}),"\n",(0,a.jsx)(t.h2,{id:"test-case",children:"Test Case"}),"\n",(0,a.jsxs)(t.p,{children:["As a test case a ",(0,a.jsx)(t.a,{href:"https://www.kaggle.com/datasets/datasnaek/chess",children:"Chess Game Dataset"})," is selected. This data is a set of 20058 rows, in total 7MB. This is still faily small, but should be kind of representative."]}),"\n",(0,a.jsxs)(t.p,{children:["The dataset will be imported into the SQL server using the ",(0,a.jsx)(t.a,{target:"_blank",href:n(7860).Z+"",children:"db_init_chess.sql"})," script, which should be copied into the ",(0,a.jsx)(t.code,{children:"config"})," directory."]}),"\n",(0,a.jsxs)(t.p,{children:["It should be noted that there are a duplicates in the dataset. In the first case, the ",(0,a.jsx)(t.em,{children:"deduplication"})," will be performed when the data is ported into the data lake (see configuration below). The procedure for table creation and modification is described below."]}),"\n",(0,a.jsx)(t.h2,{id:"prerequisites",children:"Prerequisites"}),"\n",(0,a.jsxs)(t.ul,{children:["\n",(0,a.jsx)(t.li,{children:"Podman installation"}),"\n",(0,a.jsxs)(t.li,{children:["SDLB with metastore and Polynote by cloning the getting-started example:","\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"git clone https://github.com/smart-data-lake/getting-started.git SDL_sql\n  cd SDL_sql\n  unzip part2.additional-files.zip\n"})}),"\n",':::note "Directory Naming"\nNote: The directory name "SDL_sql" will be related to the pod created later and thus to the specified commands below.\n:::']}),"\n",(0,a.jsxs)(t.li,{children:["Utilizing JDBC to MS SQL server, SDLB required this additional dependency. Therefore, add the following dependency to the ",(0,a.jsx)(t.code,{children:"pom.xml"}),":","\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-xml",children:"<dependency>\n\t<groupId>com.microsoft.sqlserver</groupId>\n\t<artifactId>mssql-jdbc</artifactId>\n\t<version>10.2.0.jre11</version>\n</dependency>\n"})}),"\n"]}),"\n",(0,a.jsxs)(t.li,{children:["build sdl-spark: ",(0,a.jsx)(t.code,{children:"podman build -t sdl-spark ."})]}),"\n",(0,a.jsxs)(t.li,{children:["build the SDLB objects:","\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:'mkdir .mvnrepo\npodman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\n'})}),"\n"]}),"\n",(0,a.jsxs)(t.li,{children:["download the test case data from ",(0,a.jsx)(t.a,{href:"https://www.kaggle.com/datasets/datasnaek/chess/download",children:"Kaggle"})," and unzip into ",(0,a.jsx)(t.code,{children:"SDL_sql/data"})," directory"]}),"\n",(0,a.jsxs)(t.li,{children:["copy polynote notebook ",(0,a.jsx)(t.a,{target:"_blank",href:n(6238).Z+"",children:"sql_data_monitor.ipynb"})," for later inspection into the ",(0,a.jsx)(t.code,{children:"polynote/notebooks"})," directory"]}),"\n"]}),"\n",(0,a.jsx)(t.admonition,{type:"danger",children:(0,a.jsxs)(t.p,{children:["The notebook will only be editable if the permissions are changed to be writable by other users ",(0,a.jsx)(t.code,{children:"chmod -R 777 polynote/notebooks"})]})}),"\n",(0,a.jsxs)(t.ul,{children:["\n",(0,a.jsxs)(t.li,{children:["script for creating table on the SQL server: ",(0,a.jsx)(t.a,{target:"_blank",href:n(7860).Z+"",children:"db_init_chess.sql"})," into the ",(0,a.jsx)(t.code,{children:"config"})," directory"]}),"\n",(0,a.jsxs)(t.li,{children:["script for modifying the table on the SQL server: ",(0,a.jsx)(t.a,{target:"_blank",href:n(4413).Z+"",children:"db_mod_chess.sql"})," into the ",(0,a.jsx)(t.code,{children:"config"})," directory"]}),"\n",(0,a.jsxs)(t.li,{children:["a restart script ",(0,a.jsx)(t.a,{target:"_blank",href:n(8215).Z+"",children:"restart_databases.sh"})," is provided to clean and restart from scratch, including: stopping the containers, cleaning databases, freshly starting the containers and initializing the SQL database"]}),"\n"]}),"\n",(0,a.jsx)(t.h2,{id:"prepare-source-database",children:"Prepare Source Database"}),"\n",(0,a.jsxs)(t.ul,{children:["\n",(0,a.jsxs)(t.li,{children:["start the pod with the metastore and polynote:","\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"mkdir -p data/_metastore\n./part2/podman-compose.sh #use the script from the getting-started guide\n"})}),"\n"]}),"\n",(0,a.jsxs)(t.li,{children:["start the MS SQL server:","\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:'podman run -d --pod sdl_sql --hostname mssqlserver --add-host mssqlserver:127.0.0.1 --name mssql -v ${PWD}/data:/data  -v ${PWD}/config:/config -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=%abcd1234%" mcr.microsoft.com/mssql/server:2017-latest\n'})}),"\n"]}),"\n",(0,a.jsxs)(t.li,{children:["initialize the database:","\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -i /config/db_init_chess.sql\n"})}),"\n"]}),"\n",(0,a.jsxs)(t.li,{children:["list the table:","\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -Q \"SELECT count(*) FROM foobar.dbo.chess\"\npodman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -Q \"SELECT * FROM foobar.dbo.chess WHERE id = '079kHDqh'\"\n"})}),"\n","This should report 20058 row and we see an example of duplicates."]}),"\n"]}),"\n",(0,a.jsx)(t.admonition,{type:"note",children:(0,a.jsxs)(t.p,{children:["This could be shortened, by just calling the ",(0,a.jsx)(t.a,{target:"_blank",href:n(8215).Z+"",children:(0,a.jsx)(t.code,{children:"bash restart_databases.sh"})}),", which contains the above commands, after stopping the containers and cleaning directories."]})}),"\n",(0,a.jsx)(t.h2,{id:"define-workflow",children:"Define Workflow"}),"\n",(0,a.jsx)(t.p,{children:"The SDLB configuration file consists of:"}),"\n",(0,a.jsxs)(t.ul,{children:["\n",(0,a.jsx)(t.li,{children:"global settings for the metastore"}),"\n",(0,a.jsx)(t.li,{children:"connection details"}),"\n",(0,a.jsx)(t.li,{children:"data objects and"}),"\n",(0,a.jsx)(t.li,{children:"action"}),"\n"]}),"\n",(0,a.jsxs)(t.p,{children:["Create the ",(0,a.jsx)(t.code,{children:"config/chess.conf"})," file with the following described sections or copy the ",(0,a.jsx)(t.a,{target:"_blank",href:n(4150).Z+"",children:"full script"}),"."]}),"\n",(0,a.jsx)(t.h3,{id:"spark-settings",children:"Spark Settings"}),"\n",(0,a.jsx)(t.p,{children:"For the metastore, the location, driver and access is defined. Further, the amount of tasks and partitions are limited, due to our reasonable small problem size."}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-hocon",children:'global {\n  spark-options {\n    "spark.hadoop.javax.jdo.option.ConnectionURL" = "jdbc:derby://metastore:1527/db;create=true"\n    "spark.hadoop.javax.jdo.option.ConnectionDriverName" = "org.apache.derby.jdbc.ClientDriver"\n    "spark.hadoop.javax.jdo.option.ConnectionUserName" = "sa"\n    "spark.hadoop.javax.jdo.option.ConnectionPassword" = "1234"\n    "spark.databricks.delta.snapshotPartitions" = 2\n    "spark.sql.shuffle.partitions" = 2\n  }\n}  \n'})}),"\n",(0,a.jsx)(t.h3,{id:"connection",children:"Connection"}),"\n",(0,a.jsx)(t.p,{children:"The connection to the local MS SQL server is specified using JDBC settings and clear text authentication specification. In practice, a more secure authentication mode should be selected, e.g. injection by environment variables."}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-hocon",children:'connections {\n  localSql {\n    type = JdbcTableConnection\n    url = "jdbc:sqlserver://mssqlserver:1433;encrypt=true;trustServerCertificate=true"\n    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver\n    authMode {\n      type = BasicAuthMode\n      userVariable = "CLEAR#sa"\n      passwordVariable = "CLEAR#%abcd1234%"\n    }\n  }\n}\n'})}),"\n",(0,a.jsx)(t.h3,{id:"dataobjects",children:"DataObjects"}),"\n",(0,a.jsxs)(t.p,{children:["In a first place, two DataObjects are defined. The ",(0,a.jsx)(t.code,{children:"ext-chess"})," defining the external source (the table on the MS SQL server), using JDBC connection. The ",(0,a.jsx)(t.code,{children:"int-chess"})," defines a delta lake table object as integration layer as targets for our ingestion/historization action."]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-hocon",children:'dataObjects {\n  ext-chess {\n    type = JdbcTableDataObject\n    connectionId = localSql\n    table = {\n      name = "dbo.chess"\n      db = "foobar"\n    }\n  }\n  int-chess {\n    type = DeltaLakeTableDataObject\n    path = "~{id}"\n    table {\n      db = "default"\n      name = "int_chess"\n      primaryKey = [id]\n    }\n  }\n  #...\n}\n'})}),"\n",(0,a.jsx)(t.h3,{id:"actions",children:"Actions"}),"\n",(0,a.jsxs)(t.p,{children:["The ",(0,a.jsx)(t.code,{children:"histData"})," action specifies the copy and historization of data, by setting the type ",(0,a.jsx)(t.strong,{children:"HistorizeAction"}),". Therewith, the incoming data will be joined with the existing data. Each data point gets two additional values: ",(0,a.jsx)(t.strong,{children:"dl_ts_captured"})," (time the data is captured in the data lake) and ",(0,a.jsx)(t.strong,{children:"dl_ts_delimited"})," (time of invalidation). In case of a data record change, the original row gets invalidated (with the current time) and a new row is added with the current time as capturing value. As long as the data point is active, ",(0,a.jsx)(t.strong,{children:"dl_ts_delimited"})," is set to max date `9999-12-31 23:59:59.999999."]}),"\n",(0,a.jsxs)(t.p,{children:["The default HistorizationAction algorithm compares all new data with all the existing data, row by row ",(0,a.jsx)(t.strong,{children:"AND"})," column by column. Further, the complete joined table is re-written to the data lake.\nFor DataObjects supporting transactions HistorizeAction provides an algorithm using a merge operation to do the historization. By selecting ",(0,a.jsx)(t.code,{children:"mergeModeEnable = true"})," the resulting table gets another column with ",(0,a.jsx)(t.code,{children:"dl_hash"}),". This hash is used to compare rows much more efficiently. Not every column need to be compared, only the hash. Further, already existing rows (identified by the hash), do not need to be re-written. The merge operation applies only the needed inserts and updates to the output DataObject."]}),"\n",(0,a.jsxs)(t.p,{children:["Furthermore, a ",(0,a.jsx)(t.em,{children:"transformer"})," needs to be added to deduplicate the input data, which has duplicated rows with slightly different game times. This is needed as HistorizationAction expects the input to be unique over the primary key of the output DataObject."]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-hocon",children:'actions {\n  histData {\n    type = HistorizeAction\n    mergeModeEnable = true\n    inputId = ext-chess\n    outputId = int-chess\n    transformers = [{\n      type = ScalaCodeDfTransformer\n      code = """\n        import org.apache.spark.sql.{DataFrame, SparkSession}\n        (session:SparkSession, options:Map[String, String], df:DataFrame, dataObjectId:String) => {\n          import session.implicits._\n          df.dropDuplicates(Seq("id"))\n        }\n      """\n   }]\n   metadata {\n      feed = download\n    }\n  }\n  #...\n}\n'})}),"\n",(0,a.jsxs)(t.p,{children:["The full configuration looks like ",(0,a.jsx)(t.a,{target:"_blank",href:n(4150).Z+"",children:"chess.conf"}),". Note that there are already further DataObjects and Actions defined, described and used later."]}),"\n",(0,a.jsx)(t.h2,{id:"run",children:"Run"}),"\n",(0,a.jsx)(t.p,{children:'The Pod with metastore, polynote and the "external" SQL server should already being running. Now the SDLB container is launched within the same POD and the action histData ingests the data into the data lake:'}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"podman run --hostname localhost -e SPARK_LOCAL_HOSTNAME=localhost --rm --pod sdl_sql -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark --config /mnt/config --feed-sel ids:histData\n"})}),"\n",(0,a.jsxs)(t.p,{children:["The data can be inspected using the Polynote notebook (",(0,a.jsx)(t.code,{children:"sql_data_monitor.ipynb"})," downloaded above), which can be launched using ",(0,a.jsx)(t.a,{href:"http://localhost:8192/notebook/sql_data_monitor.ipynb",children:"Polynote (click here)"}),"."]}),"\n",(0,a.jsxs)(t.p,{children:["Now, let's assume a change in the source database. Here the ",(0,a.jsx)(t.code,{children:"victory_status"})," ",(0,a.jsx)(t.code,{children:"outoftime"})," is renamed to ",(0,a.jsx)(t.code,{children:"overtime"}),". Furthermore one entry is deleted. Run script using:"]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -i /config/db_mod_chess.sql\n"})}),"\n",(0,a.jsx)(t.p,{children:"Note: 1680 rows were changed + 1 row deleted."}),"\n",(0,a.jsx)(t.p,{children:"Furthermore, the data lake gets updated using the same command as above:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"podman run --hostname localhost -e SPARK_LOCAL_HOSTNAME=localhost --rm --pod sdl_sql -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark --config /mnt/config --feed-sel ids:histData\n"})}),"\n",(0,a.jsxs)(t.p,{children:["In the ",(0,a.jsx)(t.a,{href:"http://localhost:8192/notebook/sql_data_monitor.ipynb",children:"Polynote (click here)"})," sql_data_monitor.ipynb, the data lake table can be inspected again. The table and its additional columns are presented, as well as the original and updated rows for a modified row (",(0,a.jsx)(t.em,{children:"id = QQ3iIM2V"}),") and the deleted row (",(0,a.jsx)(t.em,{children:"id = 009mKOEz"}),")."]}),"\n",(0,a.jsx)(t.p,{children:(0,a.jsx)(t.img,{alt:"polynote example output",src:n(310).Z+"",width:"911",height:"385"})}),"\n",(0,a.jsx)(t.h2,{id:"change-data-capture",children:"Change Data Capture"}),"\n",(0,a.jsxs)(t.p,{children:["So far the whole table is collected and compared with the existing data lake table.\nVarious databases support Change Data Capture (CDC). CDC already keeps track of changes similar to the comparision done by the historization feature of SDL.\nThis can be used to optimize performance of the historization feature, gathering only database updates, reducing the amount of transferred and compared data significantly. Therewith, only data changed (created, modified, or deleted) since the last synchronisation will be read from the database. It needs the status of the last synchronisation to be attached to the successful stream. This ",(0,a.jsx)(t.strong,{children:"state"})," is handled in SDLB."]}),"\n",(0,a.jsxs)(t.p,{children:["In the following, an example is presented utilizing the MS SQL server CDC feature. Since the implemented JDBC connector cannot handle CDC data, an Airbyte connector is utilized, see ",(0,a.jsx)(t.a,{href:"https://docs.airbyte.com/understanding-airbyte/cdc/",children:"Airbyte CDC"})," for details."]}),"\n",(0,a.jsx)(t.h3,{id:"enable-cdc-on-the-sql-server",children:"Enable CDC on the SQL Server"}),"\n",(0,a.jsxs)(t.p,{children:["First the SQL server need to be configured to have a table ",(0,a.jsx)(t.a,{href:"https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver15",children:"CDC enabled"}),". Additionally to the used table ",(0,a.jsx)(t.em,{children:"chess"}),", another table is created with the CDC feature enabled. This table ",(0,a.jsx)(t.em,{children:"chess_cdc"})," is created by copying and deduplicating the original table. The table creation is already performed in the above introduced and used ",(0,a.jsx)(t.a,{target:"_blank",href:n(7860).Z+"",children:"MS SQL initalization"})," script. In practice, further SQL settings should be considered, including proper user and permissions specification, and an adaptation of the retention period (3 days default)."]}),"\n",(0,a.jsxs)(t.p,{children:["Furthermore, the SQL agent need to be enabled. Therefore, the database need to be restarted (here container is restarted). This is handled in the above mentioned ",(0,a.jsx)(t.a,{target:"_blank",href:n(8215).Z+"",children:"restart_databases.sh"})," script. If not already used above, run:"]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"bash restart_databases.sh\n"})}),"\n",(0,a.jsx)(t.p,{children:"Let's double check the CDC enabled table:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -Q \"SELECT * FROM foobar.cdc.dbo_chess_cdc_CT where id = '079kHDqh'\"\n"})}),"\n",(0,a.jsx)(t.p,{children:"Here the first 5 columns are added for the CDC.\nIt should be noted that CDC additional data vary by implementation from different DB products."}),"\n",(0,a.jsx)(t.h3,{id:"airbyte-mssql-connector",children:"Airbyte MSSQL connector"}),"\n",(0,a.jsxs)(t.p,{children:["Since Sparks JDBC data source (used above) does not support CDC data, the connector is changed to ",(0,a.jsx)(t.a,{href:"https://docs.airbyte.com/integrations/sources/mssql",children:"Airbyte MSSQL"}),".\nIn contrast to the article ",(0,a.jsx)(t.a,{href:"sdl-airbyte",children:"Using Airbyte connector to inspect github data"}),", where Airbyte github connector ran as python script, here the connector runs as a container within the SDLB container.\nI targeted a setup using: WSL2, SDLB with podman, and the Airbyte container with podman in the SDLB container. Unfortunately, there are issues with fuse overlay filesystem when using container in container with podman. Therefore, I switched to ",(0,a.jsx)(t.a,{href:"https://buildah.io/",children:"buildah"})," in the SDLB container. Unfortunately, the entrypoint is not recognized as expected. As a workaround the following script corrects this.\nI guess in another environment, e.g. in a cloud environment or just using docker in docker, this would work out of the box."]}),"\n",(0,a.jsx)(t.p,{children:"Here are my steps to get it running:"}),"\n",(0,a.jsxs)(t.ul,{children:["\n",(0,a.jsxs)(t.li,{children:["add buildah and the Airbyte container to the SDLB ",(0,a.jsx)(t.a,{target:"_blank",href:n(3665).Z+"",children:"Dockerfile"})," (just before the entrypoint):","\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"RUN apt-get update\nRUN apt-get -y install buildah\nRUN echo 'unqualified-search-registries=[\"docker.io\"]' >> /etc/containers/registries.conf\nRUN buildah --storage-driver=vfs from --name airbyte-mssql docker.io/airbyte/source-mssql\n"})}),"\n"]}),"\n",(0,a.jsxs)(t.li,{children:["rebuild the SDLB container: ",(0,a.jsx)(t.code,{children:"podman build -t sdl-spark ."})]}),"\n",(0,a.jsxs)(t.li,{children:["workaround script to parse all arguments correctly in the Airbyte container while using buildah without the proper entrypoint. Copy ",(0,a.jsx)(t.a,{target:"_blank",href:n(7752).Z+"",children:"start_buildah.sh"})," script into ",(0,a.jsx)(t.code,{children:"config"})," directory"]}),"\n"]}),"\n",(0,a.jsx)(t.h3,{id:"sdlb-configuration",children:"SDLB configuration"}),"\n",(0,a.jsxs)(t.p,{children:["Now, the related DataObjects and Action are added to the SDLB configuration ",(0,a.jsx)(t.a,{target:"_blank",href:n(4150).Z+"",children:"config/chess.conf"}),"."]}),"\n",(0,a.jsx)(t.h4,{id:"dataobject",children:"DataObject"}),"\n",(0,a.jsxs)(t.p,{children:["As a source object the AirbyteDataObject is used. Again the MSSQL server with the user credentials are specified. Further, in the streamName the table is selected. The cmd specifies how to run Airbyte. Here the mentioned workaround script is called in the container. As target, again a Delta Lake table is chosen, here called ",(0,a.jsx)(t.code,{children:"int_chess_cdc"}),". Practically, we would prevent of duplicating tables, here we create a new one to provide the possibility to compare both results."]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-hocon",children:'dataObjects {\n    ext-chess-cdc {\n    type = AirbyteDataObject\n    config = {\n      host = "mssqlserver"\n      port = 1433\n      database = "foobar"\n      username = "sa"\n      password = "%abcd1234%"\n      replication_method = "CDC"\n    },\n    streamName = "chess_cdc",\n    cmd = {\n      type = DockerRunScript\n      name = "airbyte_source-mssql"\n      image = "airbyte-mssql"\n      linuxDockerCmd = "bash /mnt/config/start_buildah.sh"\n    }\n  }\n  int-chess-cdc {\n    type = DeltaLakeTableDataObject\n    path = "~{id}"\n    table {\n      db = "default"\n      name = "int_chess_cdc"\n      primaryKey = [id]\n    }\n  }\n}\n'})}),"\n",(0,a.jsx)(t.h4,{id:"action",children:"Action"}),"\n",(0,a.jsxs)(t.p,{children:["Again the SDLB action ",(0,a.jsx)(t.em,{children:"HistorizeAction"})," with ",(0,a.jsx)(t.em,{children:"mergeModeEnabled"})," is selected. Further, the incremental execution mode is enabled. With CDC we get only changed data, which will be merged with the SDL existing table. Further, the column and value need to be specified to identify deleted data points. Depending on the CDC implementation this could be the deletion date (like we have here) or an operation flag mentioning the deletion operation as code, or even differently. SDLB expects a fixed value for deletion. That's why we specify an ",(0,a.jsx)(t.code,{children:"AdditionalColumnsTransformer"})," transformer to first create an intermediate column mapping any date to ",(0,a.jsx)(t.em,{children:"true"}),"."]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-hocon",children:'actions {\n  histDataAirbyte {\n    type = HistorizeAction\n    mergeModeEnable = true\n    executionMode = { type = DataObjectStateIncrementalMode }\n    inputId = ext-chess-cdc\n    outputId = int-chess-cdc\n    mergeModeCDCColumn = "cdc_deleted"\n    mergeModeCDCDeletedValue = true\n    transformers = [{\n      type = AdditionalColumnsTransformer\n      additionalDerivedColumns = {cdc_deleted = "_ab_cdc_deleted_at is not null"}\n    }]\n    metadata {\n      feed = download\n    }\n  }\n}\n'})}),"\n",(0,a.jsxs)(t.p,{children:["Finally, SDLB is launched using the same settings as above, now with the feed ",(0,a.jsx)(t.code,{children:"ids:histDataAirbyte"})," and specifying the ",(0,a.jsx)(t.em,{children:"state"})," directory and name:"]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"podman run --hostname localhost -e SPARK_LOCAL_HOSTNAME=localhost --rm --pod sdl_sql -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark --config /mnt/config --feed-sel ids:histDataAirbyte --state-path /mnt/data/state -n SDL_sql\n"})}),"\n",(0,a.jsxs)(t.p,{children:["Again the SQL database modification is processed using ",(0,a.jsx)(t.a,{target:"_blank",href:n(8007).Z+"",children:"config/db_mod_chess_cdc.sql"}),":"]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"podman exec -it mssql /opt/mssql-tools/bin/sqlcmd -S mssqlserver -U sa -P '%abcd1234%' -i /config/db_mod_chess_cdc.sql\n"})}),"\n",(0,a.jsx)(t.p,{children:"And the SDL update with the same command as just used:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-Bash",children:"podman run --hostname localhost -e SPARK_LOCAL_HOSTNAME=localhost --rm --pod sdl_sql -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark --config /mnt/config --feed-sel ids:histDataAirbyte --state-path /mnt/data/state -n SDL_sql\n"})}),"\n",(0,a.jsxs)(t.p,{children:["Again the delta lake tables can be inspected using ",(0,a.jsx)(t.a,{href:"http://localhost:8192/notebook/sql_data_monitor.ipynb",children:"Polynote (click here)"}),"."]}),"\n",(0,a.jsx)(t.h2,{id:"summary",children:"Summary"}),"\n",(0,a.jsx)(t.p,{children:"Smart Data Lake Builder (SDLB) provides a powerful tool to capture data from SQL databases and provides features to track changes, as well as optimized procedures to process Change Data Capture (CDC) data. There are various connectors to interact with SQL databases (e.g. JDBC and Airbyte). Powerful transformers help to handle the data stream within the action, e.g. for deduplication. Furthermore, the join and merge of new data with existing tables, as well as the writing of the data is optimized, reducing the computational an IO efforts with standard HistorizeAction."})]})}function h(e={}){const{wrapper:t}={...(0,s.a)(),...e.components};return t?(0,a.jsx)(t,{...e,children:(0,a.jsx)(c,{...e})}):c(e)}},3665:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/Dockerfile-106772189af8db20db3f24d48737b13f.txt"},4150:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/chess-49c77f7e01ad6fb42540b06f4a93ac75.conf"},7860:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/db_init_chess-f66d528d1f63d688cc156a7fb0e9516c.sql"},4413:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/db_mod_chess-b1cad59e3d4df7cc8a098caf0b9f405e.sql"},8007:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/db_mod_chess_cdc-11e5ec52cc1ebb7c34d5ae65ed898038.sql"},8215:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/restart_databases-dee8cd590e385d628d1f8df38960a7c0.sh"},6238:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/sql_data_monitor-aa012c7e78c37559e44fcc25991e38a9.ipynb"},7752:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/start_buildah-8928533426fd919b1146d8ddf307201b.sh"},310:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/images/historization_res-dbcab5954c3121455ffdc2dd351ccf93.png"},1151:(e,t,n)=>{n.d(t,{Z:()=>o,a:()=>r});var a=n(7294);const s={},i=a.createContext(s);function r(e){const t=a.useContext(i);return a.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function o(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(s):e.components||s:r(e.components),a.createElement(i.Provider,{value:t},e.children)}}}]);