"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[1673],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>m});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),c=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=c(e.components);return a.createElement(l.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},u=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),u=c(n),m=r,f=u["".concat(l,".").concat(m)]||u[m]||d[m]||o;return n?a.createElement(f,i(i({ref:t},p),{},{components:n})):a.createElement(f,i({ref:t},p))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,i=new Array(o);i[0]=u;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:r,i[1]=s;for(var c=2;c<o;c++)i[c]=n[c];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}u.displayName="MDXCreateElement"},1487:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>i,default:()=>d,frontMatter:()=>o,metadata:()=>s,toc:()=>c});var a=n(7462),r=(n(7294),n(3905));const o={id:"hoconOverview",title:"Hocon Configurations"},i=void 0,s={unversionedId:"reference/hoconOverview",id:"reference/hoconOverview",title:"Hocon Configurations",description:"Overview",source:"@site/docs/reference/hoconOverview.md",sourceDirName:"reference",slug:"/reference/hoconOverview",permalink:"/docs/reference/hoconOverview",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/hoconOverview.md",tags:[],version:"current",frontMatter:{id:"hoconOverview",title:"Hocon Configurations"},sidebar:"docs",previous:{title:"Actions",permalink:"/docs/reference/actions"},next:{title:"Hocon Secrets",permalink:"/docs/reference/hoconSecrets"}},l={},c=[{value:"Overview",id:"overview",level:2},{value:"Global Options",id:"global-options",level:2},{value:"Connections",id:"connections",level:2},{value:"Data Objects",id:"data-objects",level:2},{value:"Actions",id:"actions",level:2},{value:"Metadata",id:"metadata",level:2},{value:"Description files",id:"description-files",level:3},{value:"Example",id:"example",level:2}],p={toc:c};function d(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h2",{id:"overview"},"Overview"),(0,r.kt)("p",null,"Data piplines in SmartDataLakeBuilder are configured in the ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/lightbend/config/blob/master/HOCON.md"},"Human-Optimized Config Object Notation (HOCON)")," file format, which is a superset of JSON. Beside being less picky about syntax (semicolons, quotation marks) it supports advanced features as substitutions, merging config from different files and inheritance."),(0,r.kt)("p",null,"Data pipelines are defined with four sections:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"#global-options"},(0,r.kt)("strong",{parentName:"a"},"global options"))," variables e.g. Spark settings"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"#connections"},(0,r.kt)("strong",{parentName:"a"},"connections")),": defining the settings for external sources or targets"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"#data-objects"},(0,r.kt)("strong",{parentName:"a"},"data objects")),": including type, format, location etc., and "),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"#actions"},(0,r.kt)("strong",{parentName:"a"},"actions")),", which describes how to get from one DataObject to another, including transformations ")),(0,r.kt)("p",null,"The settings are structured in a hierarchy. "),(0,r.kt)("p",null,"The ",(0,r.kt)("a",{parentName:"p",href:"../../JsonSchemaViewer"},(0,r.kt)("strong",{parentName:"a"},"Configuration Schema Viewer"))," presents all available options in each category with all the available parameters for each element. "),(0,r.kt)("p",null,"Furthermore, there are the following general (Hocon) features:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"hoconSecrets"},"handling of secrets")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"hoconVariables"},"handling of variables"))),(0,r.kt)("p",null,"The pipeline definition can be separated in multiple files and directories, specified with the SDLB option ",(0,r.kt)("inlineCode",{parentName:"p"},"-c, --config <file1>[,<file2>...]"),". This could also be a list of directories or a mixture of directories and files. All configuration files (",(0,r.kt)("inlineCode",{parentName:"p"},"*.conf"),") within the specified directories and its subdirectories are taken into account. "),(0,r.kt)("blockquote",null,(0,r.kt)("p",{parentName:"blockquote"},"Note: Also ",(0,r.kt)("inlineCode",{parentName:"p"},".properties")," and ",(0,r.kt)("inlineCode",{parentName:"p"},".json")," are accepted file extensions. Files with other extensions are disregarded. ")),(0,r.kt)("h2",{id:"global-options"},"Global Options"),(0,r.kt)("p",null,"Options listed in the ",(0,r.kt)("strong",{parentName:"p"},"global")," section are used by all executions. These include Spark options, UDFs, ",(0,r.kt)("a",{parentName:"p",href:"hoconSecrets"},"secretProviders")," and more."),(0,r.kt)("p",null,"As an example:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'global {\n  spark-options {\n    "spark.sql.shuffle.partitions" = 2\n    "spark.databricks.delta.snapshotPartitions" = 2\n  }\n  synchronousStreamingTriggerIntervalSec = 2\n}\n')),(0,r.kt)("h2",{id:"connections"},"Connections"),(0,r.kt)("p",null,"Some Data Objects need a connection, e.g. JdbcTableDataObject, as they need to know how to connect to a source or target, e.g. a database.\nInstead of defining the connection information for every data object, you can conveniently define it in one place and just use the reference in the data objects.\nThe possible parameters depend on the connection type. Please note the page ",(0,r.kt)("a",{parentName:"p",href:"hoconSecrets"},"hocon secrets"),"  on usernames and password handling."),(0,r.kt)("p",null,"As an example:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'connections {\n  MyTestSql {\n    type = JdbcTableConnection\n    url = "jdbc:sqlserver://mssqlserver:1433;encrypt=true;trustServerCertificate=true;database=testdb"\n    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver\n    authMode {\n      type = BasicAuthMode\n      userVariable = "ENV#MSSQLUSER"\n      passwordVariable = "ENV#MSSQLPW"\n    }\n  }\n}\n')),(0,r.kt)("p",null,"Here the connection to an MS SQL server is defined using JDBC protocol. Besides driver and location, the authentication is handled. "),(0,r.kt)("p",null,"All available connections and available parameters are listed in the ",(0,r.kt)("a",{parentName:"p",href:"https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=1"},"Configuration Schema Viewer"),"."),(0,r.kt)("h2",{id:"data-objects"},"Data Objects"),(0,r.kt)("p",null,"DataObjects are the core element of Smart data Lake Builder. This could be a file type, a database object, a general connector, or a custom defined type. It includes properties about type, location, etc."),(0,r.kt)("p",null,"See ",(0,r.kt)("a",{parentName:"p",href:"/docs/reference/dataObjects"},"Data Objects")),(0,r.kt)("h2",{id:"actions"},"Actions"),(0,r.kt)("p",null,"Actions describe the dependencies between two or more data objects and may include one or more transformer."),(0,r.kt)("p",null,"See ",(0,r.kt)("a",{parentName:"p",href:"/docs/reference/actions"},"Actions")),(0,r.kt)("h2",{id:"metadata"},"Metadata"),(0,r.kt)("p",null,"The SDLB configuration, described in these Hocon files, includes all information about the data objects, connections between them, and transformations. It can be considered as a data catalog. Thus, we suggest to include sufficient Metadata in there, this may include beside a ",(0,r.kt)("strong",{parentName:"p"},"description")," also, ",(0,r.kt)("em",{parentName:"p"},"layer"),", ",(0,r.kt)("em",{parentName:"p"},"tags"),", etc. "),(0,r.kt)("p",null,"A Metadata section is available in all elements of all categories beside of global. See example in the ",(0,r.kt)("inlineCode",{parentName:"p"},"stg-airports")," below.\nBest practice would be to add Metadata to all elements to ensure good documentation for further usage. "),(0,r.kt)("h3",{id:"description-files"},"Description files"),(0,r.kt)("p",null,"Alternatively, the description can be provided in a Markdown files. The ",(0,r.kt)("inlineCode",{parentName:"p"},".md")," files must be located in the SDLB directory ",(0,r.kt)("inlineCode",{parentName:"p"},"description/<elementType>/<elementName>.md"),",  whereas ",(0,r.kt)("em",{parentName:"p"},"elementType")," can be actions, dataObjects or connections.\nSee ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/smart-data-lake/sdl-visualization"},"SDLB Viewer")," for more details. "),(0,r.kt)("h2",{id:"example"},"Example"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'global {\n  spark-options {\n    "spark.sql.shuffle.partitions" = 2\n    "spark.databricks.delta.snapshotPartitions" = 2\n  }\n}\n\ndataObjects {\n\n  ext-airports {\n    type = WebserviceFileDataObject\n    url = "https://ourairports.com/data/airports.csv"\n    followRedirects = true\n    readTimeoutMs=200000\n  }\n\n  stg-airports {\n    type = CsvFileDataObject\n    path = "~{id}"\n    metadata {\n      name = "Staging file of Airport location data"\n      description = "contains beside GPS coordiantes, elevation, continent, country, region"\n      layer = "staging"\n      subjectArea = "airports"\n      tags = ["aviation", "airport", "location"]\n    }\n  }\n\n  int-airports {\n    type = DeltaLakeTableDataObject\n    path = "~{id}"\n    table {\n      db = "default"\n      name = "int_airports"\n      primaryKey = [ident]\n    }\n  }\n}\n\nactions {\n  download-airports {\n    type = FileTransferAction\n    inputId = ext-airports\n    outputId = stg-airports\n    metadata {\n      feed = download\n    }\n  }\n\n  historize-airports {\n    type = HistorizeAction\n    inputId = stg-airports\n    outputId = int-airports\n    transformers = [{\n      type = SQLDfTransformer\n      code = "select ident, name, latitude_deg, longitude_deg from stg_airports"\n    }]\n    metadata {\n      feed = compute\n    }\n  }\n}\n')))}d.isMDXComponent=!0}}]);