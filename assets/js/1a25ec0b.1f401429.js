"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2019],{3905:function(e,t,a){a.d(t,{Zo:function(){return c},kt:function(){return d}});var r=a(7294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},i=Object.keys(e);for(r=0;r<i.length;r++)a=i[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)a=i[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var u=r.createContext({}),s=function(e){var t=r.useContext(u),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},c=function(e){var t=s(e.components);return r.createElement(u.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,i=e.originalType,u=e.parentName,c=o(e,["components","mdxType","originalType","parentName"]),m=s(a),d=n,f=m["".concat(u,".").concat(d)]||m[d]||p[d]||i;return a?r.createElement(f,l(l({ref:t},c),{},{components:a})):r.createElement(f,l({ref:t},c))}));function d(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var i=a.length,l=new Array(i);l[0]=m;var o={};for(var u in t)hasOwnProperty.call(t,u)&&(o[u]=t[u]);o.originalType=e,o.mdxType="string"==typeof e?e:n,l[1]=o;for(var s=2;s<i;s++)l[s]=a[s];return r.createElement.apply(null,l)}return r.createElement.apply(null,a)}m.displayName="MDXCreateElement"},2672:function(e,t,a){a.r(t),a.d(t,{contentTitle:function(){return u},default:function(){return m},frontMatter:function(){return o},metadata:function(){return s},toc:function(){return c}});var r=a(7462),n=a(3366),i=(a(7294),a(3905)),l=["components"],o={id:"features",title:"Features"},u=void 0,s={unversionedId:"features",id:"features",title:"Features",description:"Smart Data Lake Builder is still under heavy development so new features are added all the time.",source:"@site/docs/features.md",sourceDirName:".",slug:"/features",permalink:"/docs/features",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/features.md",tags:[],version:"current",frontMatter:{id:"features",title:"Features"},sidebar:"docs",previous:{title:"Introduction",permalink:"/docs/"},next:{title:"Technical Setup",permalink:"/docs/getting-started/setup"}},c=[{value:"Filebased metadata",id:"filebased-metadata",children:[],level:2},{value:"Support for complex workflows &amp; streaming",id:"support-for-complex-workflows--streaming",children:[],level:2},{value:"Execution Engines",id:"execution-engines",children:[],level:2},{value:"Connectivity",id:"connectivity",children:[],level:2},{value:"Generic Transformations",id:"generic-transformations",children:[],level:2},{value:"Customizable Transformations",id:"customizable-transformations",children:[],level:2},{value:"Early Validation",id:"early-validation",children:[],level:2},{value:"Execution Modes",id:"execution-modes",children:[],level:2},{value:"Schema Evolution",id:"schema-evolution",children:[],level:2},{value:"Metrics",id:"metrics",children:[],level:2},{value:"Data Catalog",id:"data-catalog",children:[],level:2},{value:"Lineage",id:"lineage",children:[],level:2},{value:"Data Quality",id:"data-quality",children:[],level:2},{value:"Testing",id:"testing",children:[],level:2},{value:"Spark Performance",id:"spark-performance",children:[],level:2},{value:"Housekeeping",id:"housekeeping",children:[],level:2}],p={toc:c};function m(e){var t=e.components,a=(0,n.Z)(e,l);return(0,i.kt)("wrapper",(0,r.Z)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"Smart Data Lake Builder is still under heavy development so new features are added all the time.\nThe following list will give you a rough overview of current and planned features.\nMore details on the roadmap will follow shortly."),(0,i.kt)("h2",{id:"filebased-metadata"},"Filebased metadata"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Easy to version with a VCS for DevOps"),(0,i.kt)("li",{parentName:"ul"},"Flexible structure by splitting over multiple files and subdirectories"),(0,i.kt)("li",{parentName:"ul"},"Easy to generate from third party metadata (e.g. source system table catalog) to automate transformation of large number of DataObjects")),(0,i.kt)("h2",{id:"support-for-complex-workflows--streaming"},"Support for complex workflows & streaming"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Fork, join, parallel execution, multiple start- & end-nodes possible"),(0,i.kt)("li",{parentName:"ul"},"Recovery of failed runs"),(0,i.kt)("li",{parentName:"ul"},"Switch a workflow between batch or streaming execution by using just a command line switch")),(0,i.kt)("h2",{id:"execution-engines"},"Execution Engines"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Spark (DataFrames)"),(0,i.kt)("li",{parentName:"ul"},"File (Input&OutputStream)"),(0,i.kt)("li",{parentName:"ul"},"Future: Kafka Streams, Flink, \u2026")),(0,i.kt)("h2",{id:"connectivity"},"Connectivity"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Spark: diverse connectors (HadoopFS, Hive, DeltaLake, JDBC, Kafka, Splunk, Webservice, JMS) and formats (CSV, JSON, XML, Avro, Parquet, Excel, Access \u2026)"),(0,i.kt)("li",{parentName:"ul"},"File: SFTP, Local, Webservice"),(0,i.kt)("li",{parentName:"ul"},"Easy to extend by implementing predefined scala traits"),(0,i.kt)("li",{parentName:"ul"},"Support for getting secrets from different secret providers"),(0,i.kt)("li",{parentName:"ul"},"Support for SQL update & merge (Jdbc, DeltaLake)")),(0,i.kt)("h2",{id:"generic-transformations"},"Generic Transformations"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Spark based: Copy, Historization, Deduplication (incl. incremental update/merge mode for streaming)"),(0,i.kt)("li",{parentName:"ul"},"File based: FileTransfer"),(0,i.kt)("li",{parentName:"ul"},"Easy to extend by implementing predefined scala traits"),(0,i.kt)("li",{parentName:"ul"},"Future: applying MLFlow machine learning models")),(0,i.kt)("h2",{id:"customizable-transformations"},"Customizable Transformations"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Spark Transformations:",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Chain predefined standard transformations (e.g. filter, row level data validation and more) and custom transformations within the same action"),(0,i.kt)("li",{parentName:"ul"},"Custom Transformation Languages: SQL, Scala (Class, compile from config), Python"),(0,i.kt)("li",{parentName:"ul"},"Many input DataFrames to many outputs DataFrames (but only one output recommended normally, in order to define dependencies as detailed as possible for the lineage)"),(0,i.kt)("li",{parentName:"ul"},"Add metadata to each transformation to explain your data pipeline."))),(0,i.kt)("li",{parentName:"ul"},"File Transformations:",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Language: Scala"),(0,i.kt)("li",{parentName:"ul"},"Only one to one (one InputStream to one OutputStream)")))),(0,i.kt)("h2",{id:"early-validation"},"Early Validation"),(0,i.kt)("p",null,"(see ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/executionPhases"},"execution phases")," for details)"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Execution in 3 phases before execution",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Load Config: validate configuration"),(0,i.kt)("li",{parentName:"ul"},"Prepare: validate connections"),(0,i.kt)("li",{parentName:"ul"},"Init: validate Spark DataFrame Lineage (missing columns in transformations of later actions will stop the execution)")))),(0,i.kt)("h2",{id:"execution-modes"},"Execution Modes"),(0,i.kt)("p",null,"(see ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/executionModes"},"execution Modes")," for details)"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Process all data"),(0,i.kt)("li",{parentName:"ul"},"Partition parameters: give partition values to process for start nodes as parameter"),(0,i.kt)("li",{parentName:"ul"},"Partition Diff: search missing partitions and use as parameter"),(0,i.kt)("li",{parentName:"ul"},"Spark Incremental: compare sortable column between source and target, load the difference"),(0,i.kt)("li",{parentName:"ul"},"Spark Streaming: asynchronous incremental processing by using Spark Structured Streaming"),(0,i.kt)("li",{parentName:"ul"},"Spark Streaming Once: synchronous incremental processing by using Spark Structured Streaming with Trigger=Once mode")),(0,i.kt)("h2",{id:"schema-evolution"},"Schema Evolution"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Automatic evolution of data schemas (new column, removed column, changed datatype)"),(0,i.kt)("li",{parentName:"ul"},"Support for changes in complex datatypes (e.g. new column in array of struct)"),(0,i.kt)("li",{parentName:"ul"},"Automatic adaption of DataObjects with fixed schema (Jdbc, DeltaLake)")),(0,i.kt)("h2",{id:"metrics"},"Metrics"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Number of rows written per DataObject"),(0,i.kt)("li",{parentName:"ul"},"Execution duration per Action"),(0,i.kt)("li",{parentName:"ul"},"StateListener interface to get notified about progress & metrics")),(0,i.kt)("h2",{id:"data-catalog"},"Data Catalog"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Report all DataObjects attributes (incl. foreign keys if defined) for visualisation of data catalog in BI tool"),(0,i.kt)("li",{parentName:"ul"},"Metadata support for categorizing Actions and DataObjects"),(0,i.kt)("li",{parentName:"ul"},"Custom metadata attributes")),(0,i.kt)("h2",{id:"lineage"},"Lineage"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Report all dependencies between DataObjects for visualisation of lineage in BI tool")),(0,i.kt)("h2",{id:"data-quality"},"Data Quality"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Metadata support for primary & foreign keys"),(0,i.kt)("li",{parentName:"ul"},"Check & report primary key violations by executing primary key checker action"),(0,i.kt)("li",{parentName:"ul"},"Future: Metadata support for arbitrary data quality checks"),(0,i.kt)("li",{parentName:"ul"},"Future: Report data quality (foreign key matching & arbitrary data quality checks) by executing data quality reporter action")),(0,i.kt)("h2",{id:"testing"},"Testing"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Support for CI",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Config validation"),(0,i.kt)("li",{parentName:"ul"},"Custom transformation unit tests"),(0,i.kt)("li",{parentName:"ul"},"Spark data pipeline simulation (acceptance tests)"))),(0,i.kt)("li",{parentName:"ul"},"Support for Deployment",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Dry-run")))),(0,i.kt)("h2",{id:"spark-performance"},"Spark Performance"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Execute multiple Spark jobs in parallel within the same Spark Session to save resources"),(0,i.kt)("li",{parentName:"ul"},"Automatically cache and release intermediate results (DataFrames)")),(0,i.kt)("h2",{id:"housekeeping"},"Housekeeping"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Delete, or archive & compact partitions according to configurable expressions")))}m.isMDXComponent=!0}}]);