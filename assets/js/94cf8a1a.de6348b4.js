"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2194],{3905:(e,t,n)=>{n.d(t,{Zo:()=>d,kt:()=>f});var o=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,o)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,o,r=function(e,t){if(null==e)return{};var n,o,r={},a=Object.keys(e);for(o=0;o<a.length;o++)n=a[o],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(o=0;o<a.length;o++)n=a[o],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=o.createContext({}),c=function(e){var t=o.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},d=function(e){var t=c(e.components);return o.createElement(l.Provider,{value:t},e.children)},u="mdxType",p={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},m=o.forwardRef((function(e,t){var n=e.components,r=e.mdxType,a=e.originalType,l=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),u=c(n),m=r,f=u["".concat(l,".").concat(m)]||u[m]||p[m]||a;return n?o.createElement(f,i(i({ref:t},d),{},{components:n})):o.createElement(f,i({ref:t},d))}));function f(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var a=n.length,i=new Array(a);i[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[u]="string"==typeof e?e:r,i[1]=s;for(var c=2;c<a;c++)i[c]=n[c];return o.createElement.apply(null,i)}return o.createElement.apply(null,n)}m.displayName="MDXCreateElement"},1324:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>i,default:()=>u,frontMatter:()=>a,metadata:()=>s,toc:()=>c});var o=n(7462),r=(n(7294),n(3905));const a={id:"common-problems",title:"Common Problems"},i=void 0,s={unversionedId:"getting-started/troubleshooting/common-problems",id:"getting-started/troubleshooting/common-problems",title:"Common Problems",description:"This page lists a couple of common pitfalls that you may encounter in this guide as well as their solutions.",source:"@site/docs/getting-started/troubleshooting/common-problems.md",sourceDirName:"getting-started/troubleshooting",slug:"/getting-started/troubleshooting/common-problems",permalink:"/docs/getting-started/troubleshooting/common-problems",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/troubleshooting/common-problems.md",tags:[],version:"current",frontMatter:{id:"common-problems",title:"Common Problems"},sidebar:"docs",previous:{title:"Incremental Mode",permalink:"/docs/getting-started/part-3/incremental-mode"},next:{title:"Notes for Windows Users",permalink:"/docs/getting-started/troubleshooting/docker-on-windows"}},l={},c=[{value:"Missing files / DataObject schema is undefined",id:"missing-files--dataobject-schema-is-undefined",level:2},{value:"download-departures fails because of a Timeout",id:"download-departures-fails-because-of-a-timeout",level:2},{value:"How to kill SDLB if it hangs",id:"how-to-kill-sdlb-if-it-hangs",level:2}],d={toc:c};function u(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,o.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("p",null,"This page lists a couple of common pitfalls that you may encounter in this guide as well as their solutions."),(0,r.kt)("h2",{id:"missing-files--dataobject-schema-is-undefined"},"Missing files / DataObject schema is undefined"),(0,r.kt)("p",null,"If you encounter an error that looks like this:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"Exception in thread \"main\" io.smartdatalake.util.dag.TaskFailedException: Task select-airport-cols failed. Root cause is 'IllegalArgumentException: requirement failed: (DataObject~stg-airports) DataObject schema is undefined. A sche\nma must be defined if there are no existing files.'\nCaused by: java.lang.IllegalArgumentException: requirement failed: (DataObject~stg-airports) DataObject schema is undefined. A schema must be defined if there are no existing files.\n")),(0,r.kt)("p",null,"The init-phase will require to know the schema for all Data Objects to check for inconsistencies. If downloaded files already exist, the schema can be inferred.\nIn SDL version ",(0,r.kt)("em",{parentName:"p"},"< 2.3.0")," the ",(0,r.kt)("em",{parentName:"p"},"download")," action was typically defined in a separate feed, separated from further transformation actions. This download feeds needed to be run upfront, before running further feeds like ",(0,r.kt)("em",{parentName:"p"},"compute"),". Once the downloaded files are present, the schema can be inferred. "),(0,r.kt)("p",null,"To work around this issue, execute the feed ",(0,r.kt)("inlineCode",{parentName:"p"},"download")," again. After that feed was successfully executed, the execution of\nthe feed ",(0,r.kt)("inlineCode",{parentName:"p"},".*")," or ",(0,r.kt)("inlineCode",{parentName:"p"},"compute")," will work.\nOne way to prevent this problem is to explicitly provide the schema for the JSON and for the CSV-File."),(0,r.kt)("p",null,"This issue should not occur in SDL versions ",(0,r.kt)("em",{parentName:"p"},"> 2.3.0"),"."),(0,r.kt)("h2",{id:"download-departures-fails-because-of-a-timeout"},"download-departures fails because of a Timeout"),(0,r.kt)("p",null,"If you encounter an error that looks like this:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"                                      \u250c\u2500\u2500\u2500\u2500\u2500\u2510\n                                      \u2502start\u2502\n                                      \u2514\u2500\u252c\u2500\u252c\u2500\u2518\n                                        \u2502 \u2502\n                                        \u2502 \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n                                        \u2502                      \u2502\n                                        v                      v\n \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510 \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n \u2502download-departures FAILED PT5.183334S\u2502 \u2502download-airports SUCCEEDED PT1.91309S\u2502\n \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518 \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n [main]\n Exception in thread \"main\" io.smartdatalake.util.dag.TaskFailedException: Task download-departures failed. Root cause is 'WebserviceException: Read timed out'\n")),(0,r.kt)("p",null,"Since both web servers are freely available on the internet, they might be overloaded by traffic. If the download fails because of a timeout, either increase readTimeoutMs or wait a couple of minutes and try again. If the download still won't work (or if you just get empty files), you can copy the contents of the folder data-fallback-download into your data folder."),(0,r.kt)("h2",{id:"how-to-kill-sdlb-if-it-hangs"},"How to kill SDLB if it hangs"),(0,r.kt)("p",null,"In case you run into issues when executing your pipeline and you want to terminate the process\nyou can use this docker command to list the running containers:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"docker ps\n")),(0,r.kt)("p",null,"While your feed-execution is running, the output of this command will contain\nan execution with the image name ",(0,r.kt)("em",{parentName:"p"},"sdl-spark:latest"),".\nUse the container id to stop the container by typing:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"docker containter stop <container id>\n")))}u.isMDXComponent=!0}}]);