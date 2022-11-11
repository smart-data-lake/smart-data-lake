"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[6864],{3905:(e,n,t)=>{t.d(n,{Zo:()=>l,kt:()=>p});var r=t(7294);function o(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function a(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);n&&(r=r.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,r)}return t}function i(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?a(Object(t),!0).forEach((function(n){o(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):a(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function s(e,n){if(null==e)return{};var t,r,o=function(e,n){if(null==e)return{};var t,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)t=a[r],n.indexOf(t)>=0||(o[t]=e[t]);return o}(e,n);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)t=a[r],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(o[t]=e[t])}return o}var c=r.createContext({}),u=function(e){var n=r.useContext(c),t=n;return e&&(t="function"==typeof e?e(n):i(i({},n),e)),t},l=function(e){var n=u(e.components);return r.createElement(c.Provider,{value:n},e.children)},m={inlineCode:"code",wrapper:function(e){var n=e.children;return r.createElement(r.Fragment,{},n)}},d=r.forwardRef((function(e,n){var t=e.components,o=e.mdxType,a=e.originalType,c=e.parentName,l=s(e,["components","mdxType","originalType","parentName"]),d=u(t),p=o,f=d["".concat(c,".").concat(p)]||d[p]||m[p]||a;return t?r.createElement(f,i(i({ref:n},l),{},{components:t})):r.createElement(f,i({ref:n},l))}));function p(e,n){var t=arguments,o=n&&n.mdxType;if("string"==typeof e||o){var a=t.length,i=new Array(a);i[0]=d;var s={};for(var c in n)hasOwnProperty.call(n,c)&&(s[c]=n[c]);s.originalType=e,s.mdxType="string"==typeof e?e:o,i[1]=s;for(var u=2;u<a;u++)i[u]=t[u];return r.createElement.apply(null,i)}return r.createElement.apply(null,t)}d.displayName="MDXCreateElement"},5852:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>c,contentTitle:()=>i,default:()=>m,frontMatter:()=>a,metadata:()=>s,toc:()=>u});var r=t(7462),o=(t(7294),t(3905));const a={id:"streaming",title:"Streaming"},i=void 0,s={unversionedId:"reference/streaming",id:"reference/streaming",title:"Streaming",description:"This page is under review and currently not visible in the menu.",source:"@site/docs/reference/streaming.md",sourceDirName:"reference",slug:"/reference/streaming",permalink:"/docs/reference/streaming",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/streaming.md",tags:[],version:"current",frontMatter:{id:"streaming",title:"Streaming"},sidebar:"docs",previous:{title:"Transformations",permalink:"/docs/reference/transformations"},next:{title:"Deployment options",permalink:"/docs/reference/deploymentOptions"}},c={},u=[{value:"Streaming",id:"streaming",level:2}],l={toc:u};function m(e){let{components:n,...t}=e;return(0,o.kt)("wrapper",(0,r.Z)({},l,t,{components:n,mdxType:"MDXLayout"}),(0,o.kt)("admonition",{type:"warning"},(0,o.kt)("p",{parentName:"admonition"},"This page is under review and currently not visible in the menu.")),(0,o.kt)("h2",{id:"streaming"},"Streaming"),(0,o.kt)("p",null,"You can execute any DAG in streaming mode by using commandline option ",(0,o.kt)("inlineCode",{parentName:"p"},"--streaming"),".\nIn streaming mode SDL executes the Exec-phase of the same DAG continuously, processing your data incrementally.\nSDL discerns between synchronous and asynchronous actions:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Synchronous actions are executed one after another in the DAG, they are synchronized with their predecessors and successors."),(0,o.kt)("li",{parentName:"ul"},"Asynchronous actions have their own rhythm. They are executed not synchronized from the other actions, except for the first increment. In the first increment their start is synchronized with their predecessors and the first execution is waited for before starting their successors. This allows to maintain execution order for initial loads, where tables and directories might need to be created one after another.")),(0,o.kt)("p",null,"You can mix synchronous and asynchronous actions in the same DAG. Asynchronous actions are started in the first increment. Synchronous actions are executed in each execution of the DAG."),(0,o.kt)("p",null,'Whether an action is synchronous or asynchronous depends on the execution engine used. For now only "Spark Structured Streaming" is an asynchronous execution engine. It is configured by setting execution mode SparkStreamingMode to an action.'),(0,o.kt)("p",null,"You can control the minimum delay between synchronous streaming runs by setting configuration ",(0,o.kt)("inlineCode",{parentName:"p"},"global.synchronousStreamingTriggerIntervalSec")," to a certain amount of seconds.\nFor asynchronous streaming actions this is controlled by the corresponding streaming mode, e.g. SparkStreamingMode."))}m.isMDXComponent=!0}}]);