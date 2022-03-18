"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[8245],{3905:function(e,t,n){n.d(t,{Zo:function(){return p},kt:function(){return d}});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),c=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=c(e.components);return a.createElement(l.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),m=c(n),d=r,f=m["".concat(l,".").concat(d)]||m[d]||u[d]||i;return n?a.createElement(f,o(o({ref:t},p),{},{components:n})):a.createElement(f,o({ref:t},p))}));function d(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:r,o[1]=s;for(var c=2;c<i;c++)o[c]=n[c];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},2057:function(e,t,n){n.r(t),n.d(t,{contentTitle:function(){return l},default:function(){return m},frontMatter:function(){return s},metadata:function(){return c},toc:function(){return p}});var a=n(7462),r=n(3366),i=(n(7294),n(3905)),o=["components"],s={id:"commandLine",title:"Command Line"},l="Command Line",c={unversionedId:"reference/commandLine",id:"reference/commandLine",title:"Command Line",description:"SmartDataLakeBuilder is a java application.",source:"@site/docs/reference/commandLine.md",sourceDirName:"reference",slug:"/reference/commandLine",permalink:"/docs/reference/commandLine",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/commandLine.md",tags:[],version:"current",frontMatter:{id:"commandLine",title:"Command Line"},sidebar:"docs",previous:{title:"Build SDL",permalink:"/docs/reference/build"},next:{title:"Execution Phases",permalink:"/docs/reference/executionPhases"}},p=[],u={toc:p};function m(e){var t=e.components,n=(0,r.Z)(e,o);return(0,i.kt)("wrapper",(0,a.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"command-line"},"Command Line"),(0,i.kt)("p",null,"SmartDataLakeBuilder is a java application.\nTo run on a cluster with spark-submit, use ",(0,i.kt)("strong",{parentName:"p"},"DefaultSmartDataLakeBuilder")," application.\nIt can be started with the following command line options (for details, see ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/deployYarn"},"YARN"),")."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-bash"},"spark-submit --master yarn --deploy-mode client --class io.smartdatalake.app.DefaultSmartDataLakeBuilder target/smartdatalake_2.11-1.0.3-jar-with-dependencies.jar [arguments]\n")),(0,i.kt)("p",null,"and takes the following arguments:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"Usage: DefaultSmartDataLakeBuilder [options]\n  -f, --feed-sel &ltoperation?&gt&ltprefix:?&gt&ltregex&gt[,&ltoperation?&gt&ltprefix:?&gt&ltregex&gt...]\n    Select actions to execute by one or multiple expressions separated by comma (,). Results from multiple expressions are combined from left to right.\n    Operations:\n    - pipe symbol (|): the two sets are combined by union operation (default)\n    - ampersand symbol (&): the two sets are combined by intersection operation\n    - minus symbol (-): the second set is subtracted from the first set\n    Prefixes:\n    - 'feeds': select actions where metadata.feed is matched by regex pattern (default)\n    - 'names': select actions where metadata.name is matched by regex pattern\n    - 'ids': select actions where id is matched by regex pattern\n    - 'layers': select actions where metadata.layer of all output DataObjects is matched by regex pattern\n    - 'startFromActionIds': select actions which with id is matched by regex pattern and any dependent action (=successors)\n    - 'endWithActionIds': select actions which with id is matched by regex pattern and their predecessors\n    - 'startFromDataObjectIds': select actions which have an input DataObject with id is matched by regex pattern and any dependent action (=successors)\n    - 'endWithDataObjectIds': select actions which have an output DataObject with id is matched by regex pattern and their predecessors\n    All matching is done case-insensitive.\n    Example: to filter action 'A' and its successors but only in layer L1 and L2, use the following pattern: \"startFromActionIds:a,&layers:(l1|l2)\"\n  -n, --name &ltvalue&gt       Optional name of the application. If not specified feed-sel is used.\n  -c, --config &ltfile1&gt[,&ltfile2&gt...]\n    One or multiple configuration files or directories containing configuration files, separated by comma. Entries must be valid Hadoop URIs or a special URI with scheme \"cp\" which is treated as classpath entry.\n  --partition-values &ltpartitionColName&gt=&ltpartitionValue&gt[,&ltpartitionValue&gt,...]\n    Partition values to process for one single partition column.\n  --multi-partition-values &ltpartitionColName1&gt=&ltpartitionValue&gt,&ltpartitionColName2&gt=&ltpartitionValue&gt[;(&ltpartitionColName1&gt=&ltpartitionValue&gt,&ltpartitionColName2&gt=&ltpartitionValue&gt;...]\n    Partition values to process for multiple partitoin columns.\n  -s, --streaming          Enable streaming mode for continuous processing.\n  --parallelism &ltint&gt      Parallelism for DAG run.\n  --state-path &ltpath&gt      Path to save run state files. Must be set to enable recovery in case of failures.\n  --override-jars &ltjar1&gt[,&ltjar2&gt...]\n    Comma separated list of jar filenames for child-first class loader. The jars must be present in classpath.\n  --test &ltconfig|dry-run&gt  Run in test mode: config -&gt validate configuration, dry-run -&gt execute prepare- and init-phase only to check environment and spark lineage\n  --help                   Display the help text.\n  --version                Display version information.\n")),(0,i.kt)("p",null,"The ",(0,i.kt)("strong",{parentName:"p"},"DefaultSmartDataLakeBuilder")," class should be fine in most situations.\nThere are two additional, adapted application versions you can use:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"LocalSmartDataLakeBuilder"),": default for Spark master is ",(0,i.kt)("inlineCode",{parentName:"li"},"local[*]")," in this case, and it has additional properties to configure Kerberos authentication.\nUse can use this application to run in a local environment (e.g. IntelliJ) without cluster deployment."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"DatabricksSmartDataLakeBuilder"),": see ",(0,i.kt)("a",{parentName:"li",href:"/docs/reference/deploy-microsoft-azure"},"Microsoft Azure"),", special class when running a Databricks Cluster.")))}m.isMDXComponent=!0}}]);