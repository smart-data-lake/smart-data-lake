"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2546],{128:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>c,contentTitle:()=>r,default:()=>h,frontMatter:()=>s,metadata:()=>o,toc:()=>l});var i=t(4848),a=t(8453);const s={id:"hoconOverview",title:"Hocon Configurations"},r=void 0,o={id:"reference/hoconOverview",title:"Hocon Configurations",description:"Overview",source:"@site/docs/reference/hoconOverview.md",sourceDirName:"reference",slug:"/reference/hoconOverview",permalink:"/docs/reference/hoconOverview",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/hoconOverview.md",tags:[],version:"current",frontMatter:{id:"hoconOverview",title:"Hocon Configurations"},sidebar:"tutorialSidebar",previous:{title:"Actions",permalink:"/docs/reference/actions"},next:{title:"Hocon Variables",permalink:"/docs/reference/hoconVariables"}},c={},l=[{value:"Overview",id:"overview",level:2},{value:"Global Options",id:"global-options",level:2},{value:"Connections",id:"connections",level:2},{value:"Data Objects",id:"data-objects",level:2},{value:"Actions",id:"actions",level:2},{value:"Metadata",id:"metadata",level:2},{value:"Description files",id:"description-files",level:3},{value:"Example",id:"example",level:2}];function d(e){const n={a:"a",blockquote:"blockquote",code:"code",em:"em",h2:"h2",h3:"h3",li:"li",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,a.R)(),...e.components};return(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(n.h2,{id:"overview",children:"Overview"}),"\n",(0,i.jsxs)(n.p,{children:["Data piplines in SmartDataLakeBuilder are configured in the ",(0,i.jsx)(n.a,{href:"https://github.com/lightbend/config/blob/master/HOCON.md",children:"Human-Optimized Config Object Notation (HOCON)"})," file format, which is a superset of JSON. Beside being less picky about syntax (semicolons, quotation marks) it supports advanced features as substitutions, merging config from different files and inheritance."]}),"\n",(0,i.jsx)(n.p,{children:"Data pipelines are defined with four sections:"}),"\n",(0,i.jsxs)(n.ul,{children:["\n",(0,i.jsxs)(n.li,{children:[(0,i.jsx)(n.a,{href:"#global-options",children:(0,i.jsx)(n.strong,{children:"global options"})})," variables e.g. Spark settings"]}),"\n",(0,i.jsxs)(n.li,{children:[(0,i.jsx)(n.a,{href:"#connections",children:(0,i.jsx)(n.strong,{children:"connections"})}),": defining the settings for external sources or targets"]}),"\n",(0,i.jsxs)(n.li,{children:[(0,i.jsx)(n.a,{href:"#data-objects",children:(0,i.jsx)(n.strong,{children:"data objects"})}),": including type, format, location etc., and"]}),"\n",(0,i.jsxs)(n.li,{children:[(0,i.jsx)(n.a,{href:"#actions",children:(0,i.jsx)(n.strong,{children:"actions"})}),", which describes how to get from one DataObject to another, including transformations"]}),"\n"]}),"\n",(0,i.jsx)(n.p,{children:"The settings are structured in a hierarchy."}),"\n",(0,i.jsxs)(n.p,{children:["The ",(0,i.jsx)(n.a,{href:"../../json-schema-viewer",children:(0,i.jsx)(n.strong,{children:"Configuration Schema Viewer"})})," presents all available options in each category with all the available parameters for each element."]}),"\n",(0,i.jsx)(n.p,{children:"Furthermore, there are the following general (Hocon) features:"}),"\n",(0,i.jsxs)(n.ul,{children:["\n",(0,i.jsx)(n.li,{children:(0,i.jsx)(n.a,{href:"hoconSecrets",children:"handling of secrets"})}),"\n",(0,i.jsx)(n.li,{children:(0,i.jsx)(n.a,{href:"hoconVariables",children:"handling of variables"})}),"\n"]}),"\n",(0,i.jsxs)(n.p,{children:["The pipeline definition can be separated in multiple files and directories, specified with the SDLB option ",(0,i.jsx)(n.code,{children:"-c, --config <file1>[,<file2>...]"}),". This could also be a list of directories or a mixture of directories and files. All configuration files (",(0,i.jsx)(n.code,{children:"*.conf"}),") within the specified directories and its subdirectories are taken into account."]}),"\n",(0,i.jsxs)(n.blockquote,{children:["\n",(0,i.jsxs)(n.p,{children:["Note: Also ",(0,i.jsx)(n.code,{children:".properties"})," and ",(0,i.jsx)(n.code,{children:".json"})," are accepted file extensions. Files with other extensions are disregarded."]}),"\n"]}),"\n",(0,i.jsx)(n.h2,{id:"global-options",children:"Global Options"}),"\n",(0,i.jsxs)(n.p,{children:["Options listed in the ",(0,i.jsx)(n.strong,{children:"global"})," section are used by all executions. These include Spark options, UDFs, ",(0,i.jsx)(n.a,{href:"hoconSecrets",children:"secretProviders"})," and more."]}),"\n",(0,i.jsx)(n.p,{children:"As an example:"}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{children:'global {\n  spark-options {\n    "spark.sql.shuffle.partitions" = 2\n    "spark.databricks.delta.snapshotPartitions" = 2\n  }\n  synchronousStreamingTriggerIntervalSec = 2\n}\n'})}),"\n",(0,i.jsx)(n.h2,{id:"connections",children:"Connections"}),"\n",(0,i.jsxs)(n.p,{children:["Some Data Objects need a connection, e.g. JdbcTableDataObject, as they need to know how to connect to a source or target, e.g. a database.\nInstead of defining the connection information for every data object, you can conveniently define it in one place and just use the reference in the data objects.\nThe possible parameters depend on the connection type. Please note the page ",(0,i.jsx)(n.a,{href:"hoconSecrets",children:"hocon secrets"}),"  on usernames and password handling."]}),"\n",(0,i.jsx)(n.p,{children:"As an example:"}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{children:'connections {\n  MyTestSql {\n    type = JdbcTableConnection\n    url = "jdbc:sqlserver://mssqlserver:1433;encrypt=true;trustServerCertificate=true;database=testdb"\n    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver\n    authMode {\n      type = BasicAuthMode\n      userVariable = "ENV#MSSQLUSER"\n      passwordVariable = "ENV#MSSQLPW"\n    }\n  }\n}\n'})}),"\n",(0,i.jsx)(n.p,{children:"Here the connection to an MS SQL server is defined using JDBC protocol. Besides driver and location, the authentication is handled."}),"\n",(0,i.jsxs)(n.p,{children:["All available connections and available parameters are listed in the ",(0,i.jsx)(n.a,{href:"https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=1",children:"Configuration Schema Viewer"}),"."]}),"\n",(0,i.jsx)(n.h2,{id:"data-objects",children:"Data Objects"}),"\n",(0,i.jsx)(n.p,{children:"DataObjects are the core element of Smart data Lake Builder. This could be a file type, a database object, a general connector, or a custom defined type. It includes properties about type, location, etc."}),"\n",(0,i.jsxs)(n.p,{children:["See ",(0,i.jsx)(n.a,{href:"/docs/reference/dataObjects",children:"Data Objects"})]}),"\n",(0,i.jsx)(n.h2,{id:"actions",children:"Actions"}),"\n",(0,i.jsx)(n.p,{children:"Actions describe the dependencies between two or more data objects and may include one or more transformer."}),"\n",(0,i.jsxs)(n.p,{children:["See ",(0,i.jsx)(n.a,{href:"/docs/reference/actions",children:"Actions"})]}),"\n",(0,i.jsx)(n.h2,{id:"metadata",children:"Metadata"}),"\n",(0,i.jsxs)(n.p,{children:["The SDLB configuration, described in these Hocon files, includes all information about the data objects, connections between them, and transformations. It can be considered as a data catalog. Thus, we suggest to include sufficient Metadata in there, this may include beside a ",(0,i.jsx)(n.strong,{children:"description"})," also, ",(0,i.jsx)(n.em,{children:"layer"}),", ",(0,i.jsx)(n.em,{children:"tags"}),", etc."]}),"\n",(0,i.jsxs)(n.p,{children:["A Metadata section is available in all elements of all categories beside of global. See example in the ",(0,i.jsx)(n.code,{children:"stg-airports"})," below.\nBest practice would be to add Metadata to all elements to ensure good documentation for further usage."]}),"\n",(0,i.jsx)(n.h3,{id:"description-files",children:"Description files"}),"\n",(0,i.jsxs)(n.p,{children:["Alternatively, the description can be provided in a Markdown files. The ",(0,i.jsx)(n.code,{children:".md"})," files must be located in the SDLB directory ",(0,i.jsx)(n.code,{children:"description/<elementType>/<elementName>.md"}),",  whereas ",(0,i.jsx)(n.em,{children:"elementType"})," can be actions, dataObjects or connections.\nSee ",(0,i.jsx)(n.a,{href:"https://github.com/smart-data-lake/sdl-visualization",children:"SDLB Viewer"})," for more details."]}),"\n",(0,i.jsx)(n.h2,{id:"example",children:"Example"}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{children:'global {\n  spark-options {\n    "spark.sql.shuffle.partitions" = 2\n    "spark.databricks.delta.snapshotPartitions" = 2\n  }\n}\n\ndataObjects {\n\n  ext-airports {\n    type = WebserviceFileDataObject\n    url = "https://ourairports.com/data/airports.csv"\n    followRedirects = true\n    readTimeoutMs=200000\n  }\n\n  stg-airports {\n    type = CsvFileDataObject\n    path = "~{id}"\n    metadata {\n      name = "Staging file of Airport location data"\n      description = "contains beside GPS coordiantes, elevation, continent, country, region"\n      layer = "staging"\n      subjectArea = "airports"\n      tags = ["aviation", "airport", "location"]\n    }\n  }\n\n  int-airports {\n    type = DeltaLakeTableDataObject\n    path = "~{id}"\n    table {\n      db = "default"\n      name = "int_airports"\n      primaryKey = [ident]\n    }\n  }\n}\n\nactions {\n  download-airports {\n    type = FileTransferAction\n    inputId = ext-airports\n    outputId = stg-airports\n    metadata {\n      feed = download\n    }\n  }\n\n  historize-airports {\n    type = HistorizeAction\n    inputId = stg-airports\n    outputId = int-airports\n    transformers = [{\n      type = SQLDfTransformer\n      code = "select ident, name, latitude_deg, longitude_deg from stg_airports"\n    }]\n    metadata {\n      feed = compute\n    }\n  }\n}\n'})})]})}function h(e={}){const{wrapper:n}={...(0,a.R)(),...e.components};return n?(0,i.jsx)(n,{...e,children:(0,i.jsx)(d,{...e})}):d(e)}},8453:(e,n,t)=>{t.d(n,{R:()=>r,x:()=>o});var i=t(6540);const a={},s=i.createContext(a);function r(e){const n=i.useContext(s);return i.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function o(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(a):e.components||a:r(e.components),i.createElement(s.Provider,{value:n},e.children)}}}]);