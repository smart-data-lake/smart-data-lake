"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2019],{4357:(e,n,i)=>{i.r(n),i.d(n,{assets:()=>o,contentTitle:()=>s,default:()=>u,frontMatter:()=>r,metadata:()=>l,toc:()=>c});var t=i(5893),a=i(1151);const r={id:"features",title:"Features"},s=void 0,l={id:"features",title:"Features",description:"Smart Data Lake Builder is still under heavy development so new features are added all the time.",source:"@site/docs/features.md",sourceDirName:".",slug:"/features",permalink:"/docs/features",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/features.md",tags:[],version:"current",frontMatter:{id:"features",title:"Features"},sidebar:"tutorialSidebar",previous:{title:"Introduction",permalink:"/docs/"},next:{title:"Architecture",permalink:"/docs/architecture"}},o={},c=[{value:"Filebased metadata",id:"filebased-metadata",level:2},{value:"Support for complex workflows &amp; streaming",id:"support-for-complex-workflows--streaming",level:2},{value:"Execution Engines",id:"execution-engines",level:2},{value:"Connectivity",id:"connectivity",level:2},{value:"Generic Transformations",id:"generic-transformations",level:2},{value:"Customizable Transformations",id:"customizable-transformations",level:2},{value:"Early Validation",id:"early-validation",level:2},{value:"Execution Modes",id:"execution-modes",level:2},{value:"Schema Evolution",id:"schema-evolution",level:2},{value:"Metrics",id:"metrics",level:2},{value:"Data Catalog",id:"data-catalog",level:2},{value:"Lineage",id:"lineage",level:2},{value:"Data Quality",id:"data-quality",level:2},{value:"Testing",id:"testing",level:2},{value:"Spark Performance",id:"spark-performance",level:2},{value:"Housekeeping",id:"housekeeping",level:2}];function d(e){const n={a:"a",h2:"h2",li:"li",p:"p",ul:"ul",...(0,a.a)(),...e.components};return(0,t.jsxs)(t.Fragment,{children:[(0,t.jsx)(n.p,{children:"Smart Data Lake Builder is still under heavy development so new features are added all the time.\nThe following list will give you a rough overview of current and planned features.\nMore details on the roadmap will follow shortly."}),"\n",(0,t.jsx)(n.h2,{id:"filebased-metadata",children:"Filebased metadata"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Easy to version with a VCS for DevOps"}),"\n",(0,t.jsx)(n.li,{children:"Flexible structure by splitting over multiple files and subdirectories"}),"\n",(0,t.jsx)(n.li,{children:"Easy to generate from third party metadata (e.g. source system table catalog) to automate transformation of large number of DataObjects"}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"support-for-complex-workflows--streaming",children:"Support for complex workflows & streaming"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Fork, join, parallel execution, multiple start- & end-nodes possible"}),"\n",(0,t.jsx)(n.li,{children:"Recovery of failed runs"}),"\n",(0,t.jsx)(n.li,{children:"Switch a workflow between batch or streaming execution by using just a command line switch"}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"execution-engines",children:"Execution Engines"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Spark (DataFrames)"}),"\n",(0,t.jsx)(n.li,{children:"Snowflake (DataFrames)"}),"\n",(0,t.jsx)(n.li,{children:"File (Input&OutputStream)"}),"\n",(0,t.jsx)(n.li,{children:"Future: Kafka Streams, Flink, \u2026"}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"connectivity",children:"Connectivity"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Spark: diverse connectors (HadoopFS, Hive, DeltaLake, JDBC, Kafka, Splunk, Webservice, JMS) and formats (CSV, JSON, XML, Avro, Parquet, Excel, Access \u2026)"}),"\n",(0,t.jsx)(n.li,{children:"File: SFTP, Local, Webservice"}),"\n",(0,t.jsx)(n.li,{children:"Easy to extend by implementing predefined scala traits"}),"\n",(0,t.jsx)(n.li,{children:"Support for getting secrets from different secret providers"}),"\n",(0,t.jsx)(n.li,{children:"Support for SQL update & merge (Jdbc, DeltaLake)"}),"\n",(0,t.jsxs)(n.li,{children:["Support for integration of ",(0,t.jsx)(n.a,{href:"https://docs.airbyte.com/category/sources",children:"Airbyte sources"})]}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"generic-transformations",children:"Generic Transformations"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Spark based: Copy, Historization, Deduplication (incl. incremental update/merge mode for streaming)"}),"\n",(0,t.jsx)(n.li,{children:"File based: FileTransfer"}),"\n",(0,t.jsx)(n.li,{children:"Easy to extend by implementing predefined scala traits"}),"\n",(0,t.jsx)(n.li,{children:"Future: applying MLFlow machine learning models"}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"customizable-transformations",children:"Customizable Transformations"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsxs)(n.li,{children:["Spark Transformations:","\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Chain predefined standard transformations (e.g. filter, row level data validation and more) and custom transformations within the same action"}),"\n",(0,t.jsx)(n.li,{children:"Custom Transformation Languages: SQL, Scala (Class, compile from config), Python"}),"\n",(0,t.jsx)(n.li,{children:"Many input DataFrames to many outputs DataFrames (but only one output recommended normally, in order to define dependencies as detailed as possible for the lineage)"}),"\n",(0,t.jsx)(n.li,{children:"Add metadata to each transformation to explain your data pipeline."}),"\n"]}),"\n"]}),"\n",(0,t.jsxs)(n.li,{children:["File Transformations:","\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Language: Scala"}),"\n",(0,t.jsx)(n.li,{children:"Only one to one (one InputStream to one OutputStream)"}),"\n"]}),"\n"]}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"early-validation",children:"Early Validation"}),"\n",(0,t.jsxs)(n.p,{children:["(see ",(0,t.jsx)(n.a,{href:"/docs/reference/executionPhases",children:"execution phases"})," for details)"]}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsxs)(n.li,{children:["Execution in 3 phases before execution","\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Load Config: validate configuration"}),"\n",(0,t.jsx)(n.li,{children:"Prepare: validate connections"}),"\n",(0,t.jsx)(n.li,{children:"Init: validate Spark DataFrame Lineage (missing columns in transformations of later actions will stop the execution)"}),"\n"]}),"\n"]}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"execution-modes",children:"Execution Modes"}),"\n",(0,t.jsxs)(n.p,{children:["(see ",(0,t.jsx)(n.a,{href:"/docs/reference/executionModes",children:"execution Modes"})," for details)"]}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Process all data"}),"\n",(0,t.jsx)(n.li,{children:"Partition parameters: give partition values to process for start nodes as parameter"}),"\n",(0,t.jsx)(n.li,{children:"Partition Diff: search missing partitions and use as parameter"}),"\n",(0,t.jsx)(n.li,{children:"Incremental: use stateful input DataObject, or compare sortable column between source and target and load the difference"}),"\n",(0,t.jsx)(n.li,{children:"Spark Streaming: asynchronous incremental processing by using Spark Structured Streaming"}),"\n",(0,t.jsx)(n.li,{children:"Spark Streaming Once: synchronous incremental processing by using Spark Structured Streaming with Trigger=Once mode"}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"schema-evolution",children:"Schema Evolution"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Automatic evolution of data schemas (new column, removed column, changed datatype)"}),"\n",(0,t.jsx)(n.li,{children:"Support for changes in complex datatypes (e.g. new column in array of struct)"}),"\n",(0,t.jsx)(n.li,{children:"Automatic adaption of DataObjects with fixed schema (Jdbc, DeltaLake)"}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"metrics",children:"Metrics"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Number of rows written per DataObject"}),"\n",(0,t.jsx)(n.li,{children:"Execution duration per Action"}),"\n",(0,t.jsx)(n.li,{children:"Arbitrary custom metrics defined by aggregation expressions"}),"\n",(0,t.jsx)(n.li,{children:"StateListener interface to get notified about progress & metrics"}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"data-catalog",children:"Data Catalog"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Report all DataObjects attributes (incl. foreign keys if defined) for visualisation of data catalog in BI tool"}),"\n",(0,t.jsx)(n.li,{children:"Metadata support for categorizing Actions and DataObjects"}),"\n",(0,t.jsx)(n.li,{children:"Custom metadata attributes"}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"lineage",children:"Lineage"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Report all dependencies between DataObjects for visualisation of lineage in BI tool"}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"data-quality",children:"Data Quality"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Metadata support for primary & foreign keys"}),"\n",(0,t.jsx)(n.li,{children:"Check & report primary key violations by executing primary key checker action"}),"\n",(0,t.jsx)(n.li,{children:"Define and validate row-level Constraints before writing DataObject"}),"\n",(0,t.jsx)(n.li,{children:"Define and evaluate Expectations when writing DataObject, trigger warning or error, collect result as custom metric"}),"\n",(0,t.jsx)(n.li,{children:"Future: Report data quality (foreign key matching & expectations) by executing data quality reporter action"}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"testing",children:"Testing"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsxs)(n.li,{children:["Support for CI","\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Config validation"}),"\n",(0,t.jsx)(n.li,{children:"Custom transformation unit tests"}),"\n",(0,t.jsx)(n.li,{children:"Spark data pipeline simulation (acceptance tests)"}),"\n"]}),"\n"]}),"\n",(0,t.jsxs)(n.li,{children:["Support for Deployment","\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Dry-run"}),"\n"]}),"\n"]}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"spark-performance",children:"Spark Performance"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Execute multiple Spark jobs in parallel within the same Spark Session to save resources"}),"\n",(0,t.jsx)(n.li,{children:"Automatically cache and release intermediate results (DataFrames)"}),"\n"]}),"\n",(0,t.jsx)(n.h2,{id:"housekeeping",children:"Housekeeping"}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsx)(n.li,{children:"Delete, or archive & compact partitions according to configurable expressions"}),"\n"]})]})}function u(e={}){const{wrapper:n}={...(0,a.a)(),...e.components};return n?(0,t.jsx)(n,{...e,children:(0,t.jsx)(d,{...e})}):d(e)}},1151:(e,n,i)=>{i.d(n,{Z:()=>l,a:()=>s});var t=i(7294);const a={},r=t.createContext(a);function s(e){const n=t.useContext(r);return t.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function l(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(a):e.components||a:s(e.components),t.createElement(r.Provider,{value:n},e.children)}}}]);