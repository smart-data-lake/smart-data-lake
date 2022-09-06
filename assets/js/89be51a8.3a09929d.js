"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[7749],{3905:(e,t,n)=>{n.d(t,{Zo:()=>d,kt:()=>m});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function c(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=r.createContext({}),p=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},d=function(e){var t=p(e.components);return r.createElement(s.Provider,{value:t},e.children)},l={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},u=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,s=e.parentName,d=c(e,["components","mdxType","originalType","parentName"]),u=p(n),m=a,f=u["".concat(s,".").concat(m)]||u[m]||l[m]||o;return n?r.createElement(f,i(i({ref:t},d),{},{components:n})):r.createElement(f,i({ref:t},d))}));function m(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,i=new Array(o);i[0]=u;var c={};for(var s in t)hasOwnProperty.call(t,s)&&(c[s]=t[s]);c.originalType=e,c.mdxType="string"==typeof e?e:a,i[1]=c;for(var p=2;p<o;p++)i[p]=n[p];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}u.displayName="MDXCreateElement"},2692:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>i,default:()=>l,frontMatter:()=>o,metadata:()=>c,toc:()=>p});var r=n(7462),a=(n(7294),n(3905));const o={id:"actions",title:"Actions"},i=void 0,c={unversionedId:"reference/actions",id:"reference/actions",title:"Actions",description:"Actions describe dependencies between input and output dataObjects and necessary transformation to connect them.",source:"@site/docs/reference/actions.md",sourceDirName:"reference",slug:"/reference/actions",permalink:"/docs/reference/actions",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/actions.md",tags:[],version:"current",frontMatter:{id:"actions",title:"Actions"},sidebar:"docs",previous:{title:"Data Objects",permalink:"/docs/reference/dataObjects"},next:{title:"Hocon Configurations",permalink:"/docs/reference/hoconOverview"}},s={},p=[{value:"Transformations",id:"transformations",level:2},{value:"MetaData",id:"metadata",level:2},{value:"ExecutionMode",id:"executionmode",level:2},{value:"ExecutionCondition",id:"executioncondition",level:2},{value:"recursiveInputIds",id:"recursiveinputids",level:2}],d={toc:p};function l(e){let{components:t,...n}=e;return(0,a.kt)("wrapper",(0,r.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("p",null,"Actions describe dependencies between input and output dataObjects and necessary transformation to connect them. "),(0,a.kt)("h2",{id:"transformations"},"Transformations"),(0,a.kt)("p",null,"These can be custom transformers in SQL, Scala/Spark, or Python, OR predefined transformations like Copy, Historization and Deduplication, see ",(0,a.kt)("a",{parentName:"p",href:"transformations"},"Transformations"),". "),(0,a.kt)("h2",{id:"metadata"},"MetaData"),(0,a.kt)("p",null,"As well as dataObject and connections, various metadata can be provided for action items. These help manage and explore data in the Smart Data Lake. Beside ",(0,a.kt)("em",{parentName:"p"},"name")," and ",(0,a.kt)("em",{parentName:"p"},"description"),", a ",(0,a.kt)("em",{parentName:"p"},"feed")," and a list of ",(0,a.kt)("em",{parentName:"p"},"tags")," can be specified. "),(0,a.kt)("h2",{id:"executionmode"},"ExecutionMode"),(0,a.kt)("p",null,"By default all data in the specified dataObjects are processes. The option execution mode provides the possibility to e.g. partially process them. This could be specific partitions or incrementally process, see ",(0,a.kt)("a",{parentName:"p",href:"executionModes"},"ExecutionMode"),". "),(0,a.kt)("h2",{id:"executioncondition"},"ExecutionCondition"),(0,a.kt)("p",null,"A condition can be specified in SQL format, which can be further used in other options, e.g. failCondition of PartitionDiffMode. "),(0,a.kt)("h2",{id:"recursiveinputids"},"recursiveInputIds"),(0,a.kt)("p",null,"In general we want to avoid cyclic graph of action. This option enables updating dataObjects. Therewith, the dataObject is input and output at the same time. It needs to be specified in as output, but not as input."),(0,a.kt)("p",null,"Example: assuming an object ",(0,a.kt)("inlineCode",{parentName:"p"},"stg-src"),", which data should be added to an growing table ",(0,a.kt)("inlineCode",{parentName:"p"},"int-tgt"),"."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"  action1 {\n    type = CustomDataFrameAction\n    inputIds = [stg-src]\n    outputIds = [int-tgt]\n    recursiveInputIds = [int-tgt]\n    ...\n")))}l.isMDXComponent=!0}}]);