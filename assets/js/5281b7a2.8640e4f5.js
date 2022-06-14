"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[5927],{3905:function(e,t,n){n.d(t,{Zo:function(){return u},kt:function(){return m}});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=r.createContext({}),c=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},u=function(e){var t=c(e.components);return r.createElement(s.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,i=e.originalType,s=e.parentName,u=o(e,["components","mdxType","originalType","parentName"]),d=c(n),m=a,g=d["".concat(s,".").concat(m)]||d[m]||p[m]||i;return n?r.createElement(g,l(l({ref:t},u),{},{components:n})):r.createElement(g,l({ref:t},u))}));function m(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=n.length,l=new Array(i);l[0]=d;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o.mdxType="string"==typeof e?e:a,l[1]=o;for(var c=2;c<i;c++)l[c]=n[c];return r.createElement.apply(null,l)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},1527:function(e,t,n){n.r(t),n.d(t,{contentTitle:function(){return s},default:function(){return d},frontMatter:function(){return o},metadata:function(){return c},toc:function(){return u}});var r=n(7462),a=n(3366),i=(n(7294),n(3905)),l=["components"],o={id:"architecture",title:"Architecture"},s=void 0,c={unversionedId:"architecture",id:"architecture",title:"Architecture",description:"Smart Data Lake Builder (SDLB) is basically a Java application which is started on the command line.",source:"@site/docs/architecture.md",sourceDirName:".",slug:"/architecture",permalink:"/docs/architecture",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/architecture.md",tags:[],version:"current",frontMatter:{id:"architecture",title:"Architecture"},sidebar:"docs",previous:{title:"Features",permalink:"/docs/features"},next:{title:"Technical Setup",permalink:"/docs/getting-started/setup"}},u=[{value:"Basic Requirements",id:"basic-requirements",children:[],level:2},{value:"Versions and supported configuration",id:"versions-and-supported-configuration",children:[],level:2},{value:"Release Notes",id:"release-notes",children:[],level:2},{value:"Logging",id:"logging",children:[],level:2}],p={toc:u};function d(e){var t=e.components,n=(0,a.Z)(e,l);return(0,i.kt)("wrapper",(0,r.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"Smart Data Lake Builder (SDLB) is basically a Java application which is started on the ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/commandLine"},"command line"),".\nIt can run in many environments and platforms like a Databricks cluster, Azure Synapse, Google Dataproc, and also on your local machine, see ",(0,i.kt)("a",{parentName:"p",href:"getting-started/setup"},"Getting Started"),"."),(0,i.kt)("p",null,"Find below an overview of requirements, versions and supported configurations."),(0,i.kt)("h2",{id:"basic-requirements"},"Basic Requirements"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Needs Java 8+ to run"),(0,i.kt)("li",{parentName:"ul"},"Uses Hadoop Java library to read local and remote files (S3, ADLS, HDFS, ...)"),(0,i.kt)("li",{parentName:"ul"},"Is programmed in Scala"),(0,i.kt)("li",{parentName:"ul"},"Uses Maven 3+ as build system")),(0,i.kt)("h2",{id:"versions-and-supported-configuration"},"Versions and supported configuration"),(0,i.kt)("p",null,"SDLB currently maintains the following two major versions, which are published as Maven artifacts on maven central:"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:null},"SDL Version"),(0,i.kt)("th",{parentName:"tr",align:null},"Java/Scala/Hadoop Version"),(0,i.kt)("th",{parentName:"tr",align:null},"File Engine"),(0,i.kt)("th",{parentName:"tr",align:null},"Spark Engine"),(0,i.kt)("th",{parentName:"tr",align:null},"Snowflake/Snowpark Engine"),(0,i.kt)("th",{parentName:"tr",align:null},"Comments"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"1.x, branch master/develop-spark2"),(0,i.kt)("td",{parentName:"tr",align:null},"Java 8, Scala 2.11 & 2.12"),(0,i.kt)("td",{parentName:"tr",align:null},"Hadoop 2.7.x"),(0,i.kt)("td",{parentName:"tr",align:null},"Spark 2.4.x"),(0,i.kt)("td",{parentName:"tr",align:null},"not supported"),(0,i.kt)("td",{parentName:"tr",align:null},"Delta lake has limited functionality in Spark 2.x")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:null},"2.x, branch master/develop-spark3"),(0,i.kt)("td",{parentName:"tr",align:null},"Java 8+, Scala 2.12"),(0,i.kt)("td",{parentName:"tr",align:null},"Hadoop 3.3.x (2.7.x)"),(0,i.kt)("td",{parentName:"tr",align:null},"Spark 3.2.x (3.1.x)"),(0,i.kt)("td",{parentName:"tr",align:null},"Snowpark 1.2.x"),(0,i.kt)("td",{parentName:"tr",align:null},"Delta lake, spark-snowflake and spark-extensions need specific library versions matching the corresponding spark minor version")))),(0,i.kt)("p",null,"Configurations using alternative versions mentioned in parentheses can be build manually by setting corresponding maven profiles."),(0,i.kt)("p",null,"It's possible to customize dependencies and make Smart Data Lake Builder work with other version combinations, but this needs manual tuning of dependencies in your own maven project."),(0,i.kt)("p",null,"In general, Java library versions are held as close as possible to the ones used in the corresponding Spark version."),(0,i.kt)("h2",{id:"release-notes"},"Release Notes"),(0,i.kt)("p",null,"See SDBL Release Notes including breaking changes on ",(0,i.kt)("a",{parentName:"p",href:"https://github.com/smart-data-lake/smart-data-lake/releases"},"Github")),(0,i.kt)("h2",{id:"logging"},"Logging"),(0,i.kt)("p",null,"By default, SDLB uses the logging libraries included in the corresponding Spark version. This is Log4j 1.2.x for Spark 2.4.x up to Spark 3.2.x.\nStarting from Spark 3.3.x it will use Log4j 2.x, see ",(0,i.kt)("a",{parentName:"p",href:"https://issues.apache.org/jira/browse/SPARK-6305"},"SPARK-6305"),"."),(0,i.kt)("p",null,"You can customize logging dependencies manually by creating your own maven project."))}d.isMDXComponent=!0}}]);