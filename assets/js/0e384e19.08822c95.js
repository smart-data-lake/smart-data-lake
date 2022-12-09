"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[9671],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>m});var r=a(7294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},o=Object.keys(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var d=r.createContext({}),s=function(e){var t=r.useContext(d),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},c=function(e){var t=s(e.components);return r.createElement(d.Provider,{value:t},e.children)},A="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},p=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,o=e.originalType,d=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),A=s(a),p=n,m=A["".concat(d,".").concat(p)]||A[p]||u[p]||o;return a?r.createElement(m,i(i({ref:t},c),{},{components:a})):r.createElement(m,i({ref:t},c))}));function m(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=a.length,i=new Array(o);i[0]=p;var l={};for(var d in t)hasOwnProperty.call(t,d)&&(l[d]=t[d]);l.originalType=e,l[A]="string"==typeof e?e:n,i[1]=l;for(var s=2;s<o;s++)i[s]=a[s];return r.createElement.apply(null,i)}return r.createElement.apply(null,a)}p.displayName="MDXCreateElement"},9881:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>d,contentTitle:()=>i,default:()=>A,frontMatter:()=>o,metadata:()=>l,toc:()=>s});var r=a(7462),n=(a(7294),a(3905));const o={id:"intro",title:"Introduction",sidebar_label:"Introduction",slug:"/"},i=void 0,l={unversionedId:"intro",id:"intro",title:"Introduction",description:"Smart Data Lake Builder (SDL for short) is a data lake automation framework that makes loading and transforming data a breeze.",source:"@site/docs/intro.md",sourceDirName:".",slug:"/",permalink:"/docs/",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/intro.md",tags:[],version:"current",frontMatter:{id:"intro",title:"Introduction",sidebar_label:"Introduction",slug:"/"},sidebar:"docs",next:{title:"Features",permalink:"/docs/features"}},d={},s=[{value:"A Data Lake",id:"a-data-lake",level:3},{value:"The Smart Data Lake adds",id:"the-smart-data-lake-adds",level:3},{value:"Benefits of Smart Data Lake Builder",id:"benefits-of-smart-data-lake-builder",level:3},{value:"When should you consider using Smart Data Lake Builder ?",id:"when-should-you-consider-using-smart-data-lake-builder-",level:3},{value:"How it works",id:"how-it-works",level:2},{value:"Data object",id:"data-object",level:3},{value:"Action",id:"action",level:3},{value:"Feed",id:"feed",level:3},{value:"Configuration",id:"configuration",level:3},{value:"Getting Started",id:"getting-started",level:3},{value:"Get in touch",id:"get-in-touch",level:3}],c={toc:s};function A(e){let{components:t,...o}=e;return(0,n.kt)("wrapper",(0,r.Z)({},c,o,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("p",null,"Smart Data Lake Builder (SDL for short) is a data lake automation framework that makes loading and transforming data a breeze.\nIt is implemented in Scala and builds on top of open-source big data technologies like ",(0,n.kt)("a",{parentName:"p",href:"https://hadoop.apache.org/"},"Apache Hadoop")," and ",(0,n.kt)("a",{parentName:"p",href:"https://spark.apache.org/"},"Apache Spark"),", including connectors for diverse data sources (HadoopFS, Hive, DeltaLake, JDBC, Splunk,  Webservice, SFTP, JMS, Excel, Access) and file formats."),(0,n.kt)("h3",{id:"a-data-lake"},"A Data Lake"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"is a central raw data store for analytics"),(0,n.kt)("li",{parentName:"ul"},"facilitates cheap raw storage to handle growing volumes of data"),(0,n.kt)("li",{parentName:"ul"},"enables topnotch artificial intelligence (AI) and machine learning (ML) technologies for data-driven enterprises")),(0,n.kt)("h3",{id:"the-smart-data-lake-adds"},"The Smart Data Lake adds"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"a layered data architecture to provide not only raw data, but prepared, secured, high quality data according to business entities, ready to use for analytical use cases, also called \xabSmart Data\xbb. This is comparable to Databricks Lake House architecture, in fact Smart Data Lake Builder is a very good choice to automate a Lake House, also on Databricks."),(0,n.kt)("li",{parentName:"ul"},"a declarative, configuration-driven approach to creating data pipelines. Metadata about data pipelines allows for efficient operations, maintenance and more business self-service.")),(0,n.kt)("h3",{id:"benefits-of-smart-data-lake-builder"},"Benefits of Smart Data Lake Builder"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Cheaper implementation of data lakes"),(0,n.kt)("li",{parentName:"ul"},"Increased productivity of data scientists"),(0,n.kt)("li",{parentName:"ul"},"Higher level of self-service"),(0,n.kt)("li",{parentName:"ul"},"Decreased operations and maintenance costs"),(0,n.kt)("li",{parentName:"ul"},"Fully open source, no vendor lock-in")),(0,n.kt)("h3",{id:"when-should-you-consider-using-smart-data-lake-builder-"},"When should you consider using Smart Data Lake Builder ?"),(0,n.kt)("p",null,"Some common use cases include:"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Building Data Lakes, drastically increasing productivity and usability"),(0,n.kt)("li",{parentName:"ul"},"Data Apps - building complex data processing apps"),(0,n.kt)("li",{parentName:"ul"},"DWH automation - reading and writing to relational databases via SQL"),(0,n.kt)("li",{parentName:"ul"},"Data migration - Efficiently create one-time data pipelines"),(0,n.kt)("li",{parentName:"ul"},"Data Catalog / Data Lineage - Generated automatically from metadata")),(0,n.kt)("p",null,"See (docs/Features.md) for a comprehensive list of Smart Data Lake Builder features."),(0,n.kt)("h2",{id:"how-it-works"},"How it works"),(0,n.kt)("p",null,"The following diagram shows the core concepts:"),(0,n.kt)("p",null,(0,n.kt)("img",{alt:"How it works",src:a(5932).Z,width:"1407",height:"562"})),(0,n.kt)("h3",{id:"data-object"},"Data object"),(0,n.kt)("p",null,"A data object defines the location and format of data.\nSome data objects require a connection to access remote data (e.g. a database connection)."),(0,n.kt)("h3",{id:"action"},"Action"),(0,n.kt)("p",null,'The "data processors" are called actions.\nAn action requires at least one input and output data object.\nAn action reads the data from the input data object, processes and writes it to the output data object.\nMany actions are predefined e.g. transform data from json to csv but you can also define your custom transformer action.'),(0,n.kt)("h3",{id:"feed"},"Feed"),(0,n.kt)("p",null,"Actions connect different Data Object and implicitly define a directed acyclic graph, as they model the dependencies needed to fill a Data Object.\nThis automatically generated, arbitrary complex data flow can be divided up into Feed's (subgraphs) for execution and monitoring."),(0,n.kt)("h3",{id:"configuration"},"Configuration"),(0,n.kt)("p",null,"All metadata i.e. connections, data objects and actions are defined in a central configuration file, usually called application.conf.\nThe file format used is ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/lightbend/config/blob/master/HOCON.md"},"HOCON")," which makes it easy to edit."),(0,n.kt)("h3",{id:"getting-started"},"Getting Started"),(0,n.kt)("p",null,"To see how all this works in action, head over to the ",(0,n.kt)("a",{parentName:"p",href:"/docs/getting-started/setup"},"Getting Started Guide")," page."),(0,n.kt)("h3",{id:"get-in-touch"},"Get in touch"),(0,n.kt)("p",null,"If you have issues, comments or feedback, please see ",(0,n.kt)("a",{parentName:"p",href:"/docs/contribution"},"Contributing")," on how to get in touch."),(0,n.kt)("h1",{id:"major-contributors"},"Major Contributors"),(0,n.kt)("p",null,(0,n.kt)("img",{alt:"SBB",src:a(675).Z,width:"250",height:"26"}),(0,n.kt)("br",{parentName:"p"}),"\n",(0,n.kt)("a",{parentName:"p",href:"http://www.sbb.ch"},"www.sbb.ch")," : Provided the previously developed software as a foundation for the open source project"),(0,n.kt)("p",null,(0,n.kt)("img",{alt:"ELCA",src:a(6746).Z,width:"72",height:"74"}),(0,n.kt)("br",{parentName:"p"}),"\n",(0,n.kt)("a",{parentName:"p",href:"http://www.elca.ch"},"www.elca.ch")," : Did the comprehensive revision and provision as open source project"))}A.isMDXComponent=!0},6746:(e,t,a)=>{a.d(t,{Z:()=>r});const r="data:image/png;base64,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"},675:(e,t,a)=>{a.d(t,{Z:()=>r});const r="data:image/png;base64,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"},5932:(e,t,a)=>{a.d(t,{Z:()=>r});const r=a.p+"assets/images/feed-f0558fa085f3f246013bc3189b4426ea.png"}}]);