"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[1380],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>h});var a=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,i=function(e,t){if(null==e)return{};var n,a,i={},r=Object.keys(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var s=a.createContext({}),c=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=c(e.components);return a.createElement(s.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},d=a.forwardRef((function(e,t){var n=e.components,i=e.mdxType,r=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),d=c(n),h=i,m=d["".concat(s,".").concat(h)]||d[h]||u[h]||r;return n?a.createElement(m,o(o({ref:t},p),{},{components:n})):a.createElement(m,o({ref:t},p))}));function h(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var r=n.length,o=new Array(r);o[0]=d;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:i,o[1]=l;for(var c=2;c<r;c++)o[c]=n[c];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}d.displayName="MDXCreateElement"},3763:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>o,default:()=>u,frontMatter:()=>r,metadata:()=>l,toc:()=>c});var a=n(7462),i=(n(7294),n(3905));const r={id:"executionPhases",title:"Execution Phases"},o=void 0,l={unversionedId:"reference/executionPhases",id:"reference/executionPhases",title:"Execution Phases",description:"Early validation",source:"@site/docs/reference/executionPhases.md",sourceDirName:"reference",slug:"/reference/executionPhases",permalink:"/docs/reference/executionPhases",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/executionPhases.md",tags:[],version:"current",frontMatter:{id:"executionPhases",title:"Execution Phases"},sidebar:"docs",previous:{title:"Data Quality",permalink:"/docs/reference/dataQuality"},next:{title:"Execution Engines",permalink:"/docs/reference/executionEngines"}},s={},c=[{value:"Early validation",id:"early-validation",level:3},{value:"Implications",id:"implications",level:3},{value:"Early validation",id:"early-validation-1",level:4},{value:"No data during Init Phase",id:"no-data-during-init-phase",level:4},{value:"Watch the log output",id:"watch-the-log-output",level:4}],p={toc:c};function u(e){let{components:t,...n}=e;return(0,i.kt)("wrapper",(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h3",{id:"early-validation"},"Early validation"),(0,i.kt)("p",null,'Execution of a SmartDataLakeBuilder run is designed with "early validation" in mind. This means it tries to fail as early as possible if something is wrong.'),(0,i.kt)("p",null,"The following phases are involved during each execution:"),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("strong",{parentName:"li"},"Parse configuration"),":",(0,i.kt)("br",{parentName:"li"}),"Parses and validates your configuration files",(0,i.kt)("br",{parentName:"li"}),"This step fails if there is anything wrong with your configuration, i.e. if a required attribute is missing or a whole block like ",(0,i.kt)("inlineCode",{parentName:"li"},"actions {}")," is missing or misspelled.\nThere's also a neat feature that will warn you of typos and will suggest spelling corrections if it can."),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("strong",{parentName:"li"},"DAG prepare:"),(0,i.kt)("br",{parentName:"li"}),"Preconditions are validated",(0,i.kt)("br",{parentName:"li"}),"This includes testing Connections and DataObject structures that must exists."),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("strong",{parentName:"li"},"DAG init:"),(0,i.kt)("br",{parentName:"li"}),"Creates and validates the whole lineage of Actions according to the DAG",(0,i.kt)("br",{parentName:"li"}),"For Spark Actions this involves the validation of the DataFrame lineage.\nA column which doesn't exist but is referenced in a later Action will fail the execution."),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("strong",{parentName:"li"},"DAG exec"),":",(0,i.kt)("br",{parentName:"li"}),"Execution of Actions",(0,i.kt)("br",{parentName:"li"}),"Data is effectively transferred in this phase (and only in this phase!).")),(0,i.kt)("h3",{id:"implications"},"Implications"),(0,i.kt)("h4",{id:"early-validation-1"},"Early validation"),(0,i.kt)("p",null,"As mentioned, especially the init phase is very powerful as SDL will validate your whole lineage.\nThis even includes custom transformations you have in your pipeline.",(0,i.kt)("br",{parentName:"p"}),"\n","So if you have a typo in a column name or reference a column that will not exist at that state in the pipeline,\nSDL will report this error and fail within seconds.\nThis saves a lot of time as you don't have to wait for the whole pipeline to execute to catch these errors."),(0,i.kt)("h4",{id:"no-data-during-init-phase"},"No data during Init Phase"),(0,i.kt)("p",null,"At one point you will start implementing your own transformers.\nWhen analyzing problems you will want to debug them and most likely set break points somewhere in the ",(0,i.kt)("inlineCode",{parentName:"p"},"transform")," method.\nIt's important to know, that execution will pass your break point twice:",(0,i.kt)("br",{parentName:"p"}),"\n","Once during the ",(0,i.kt)("inlineCode",{parentName:"p"},"init")," phase, once during ",(0,i.kt)("inlineCode",{parentName:"p"},"exec")," phase.\nIn the ",(0,i.kt)("inlineCode",{parentName:"p"},"init")," phase, the whole execution is validated but without actually moving any data.\nIf you take a look at your DataFrames at this point, it will be empty.\nWe can guarantee that you will fall into this trap at least once. ;-)"),(0,i.kt)("admonition",{type:"caution"},(0,i.kt)("p",{parentName:"admonition"},"If you debug your code and wonder why your DataFrame is completely empty,\nyou probably stopped execution during init phase."),(0,i.kt)("p",{parentName:"admonition"},"Continue execution and make sure you're in the exec phase before taking a look at data in your DataFrame.")),(0,i.kt)("h4",{id:"watch-the-log-output"},"Watch the log output"),(0,i.kt)("p",null,"The stages are also clearly marked in the log output.\nHere is the sample output of ",(0,i.kt)("a",{parentName:"p",href:"/docs/getting-started/part-3/custom-webservice"},"part-3 of the gettings-started guide")," again with a few things removed:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"Action~download-departures[CopyAction]: Prepare started\nAction~download-departures[CopyAction]: Prepare succeeded\nAction~download-departures[CopyAction]: Init started\nAction~download-departures[CopyAction]: Init succeeded\nAction~download-departures[CopyAction]: Exec started\n...\n")),(0,i.kt)("p",null,"If execution stops, always check during which phase that happens.\nIf it happens while still in the init phase, it probably has nothing to do with the data itself but more with the structure of your DataObjects."))}u.isMDXComponent=!0}}]);