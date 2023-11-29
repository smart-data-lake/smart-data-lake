"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2194],{2701:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>d,contentTitle:()=>a,default:()=>h,frontMatter:()=>s,metadata:()=>r,toc:()=>l});var o=n(5893),i=n(1151);const s={id:"common-problems",title:"Common Problems"},a=void 0,r={id:"getting-started/troubleshooting/common-problems",title:"Common Problems",description:"This page lists a couple of common pitfalls that you may encounter in this guide as well as their solutions.",source:"@site/docs/getting-started/troubleshooting/common-problems.md",sourceDirName:"getting-started/troubleshooting",slug:"/getting-started/troubleshooting/common-problems",permalink:"/docs/getting-started/troubleshooting/common-problems",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/troubleshooting/common-problems.md",tags:[],version:"current",frontMatter:{id:"common-problems",title:"Common Problems"},sidebar:"tutorialSidebar",previous:{title:"Incremental Mode",permalink:"/docs/getting-started/part-3/incremental-mode"},next:{title:"Notes for Windows Users",permalink:"/docs/getting-started/troubleshooting/docker-on-windows"}},d={},l=[{value:"Missing files / DataObject schema is undefined",id:"missing-files--dataobject-schema-is-undefined",level:2},{value:"download-departures fails because of a Timeout",id:"download-departures-fails-because-of-a-timeout",level:2},{value:"How to kill SDLB if it hangs",id:"how-to-kill-sdlb-if-it-hangs",level:2},{value:"ERROR 08001: java.net.ConnectException : Error connecting to server localhost on port 1527",id:"error-08001-javanetconnectexception--error-connecting-to-server-localhost-on-port-1527",level:2}];function c(e){const t={code:"code",em:"em",h2:"h2",p:"p",pre:"pre",...(0,i.a)(),...e.components};return(0,o.jsxs)(o.Fragment,{children:[(0,o.jsx)(t.p,{children:"This page lists a couple of common pitfalls that you may encounter in this guide as well as their solutions."}),"\n",(0,o.jsx)(t.h2,{id:"missing-files--dataobject-schema-is-undefined",children:"Missing files / DataObject schema is undefined"}),"\n",(0,o.jsx)(t.p,{children:"If you encounter an error that looks like this:"}),"\n",(0,o.jsx)(t.pre,{children:(0,o.jsx)(t.code,{children:"Exception in thread \"main\" io.smartdatalake.util.dag.TaskFailedException: Task select-airport-cols failed. Root cause is 'IllegalArgumentException: requirement failed: (DataObject~stg-airports) DataObject schema is undefined. A sche\nma must be defined if there are no existing files.'\nCaused by: java.lang.IllegalArgumentException: requirement failed: (DataObject~stg-airports) DataObject schema is undefined. A schema must be defined if there are no existing files.\n"})}),"\n",(0,o.jsxs)(t.p,{children:["The init-phase will require to know the schema for all Data Objects to check for inconsistencies. If downloaded files already exist, the schema can be inferred.\nIn SDL version ",(0,o.jsx)(t.em,{children:"< 2.3.0"})," the ",(0,o.jsx)(t.em,{children:"download"})," action was typically defined in a separate feed, separated from further transformation actions. This download feeds needed to be run upfront, before running further feeds like ",(0,o.jsx)(t.em,{children:"compute"}),". Once the downloaded files are present, the schema can be inferred."]}),"\n",(0,o.jsxs)(t.p,{children:["To work around this issue, execute the feed ",(0,o.jsx)(t.code,{children:"download"})," again. After that feed was successfully executed, the execution of\nthe feed ",(0,o.jsx)(t.code,{children:".*"})," or ",(0,o.jsx)(t.code,{children:"compute"})," will work.\nOne way to prevent this problem is to explicitly provide the schema for the JSON and for the CSV-File."]}),"\n",(0,o.jsxs)(t.p,{children:["This issue should not occur in SDL versions ",(0,o.jsx)(t.em,{children:"> 2.3.0"}),"."]}),"\n",(0,o.jsx)(t.h2,{id:"download-departures-fails-because-of-a-timeout",children:"download-departures fails because of a Timeout"}),"\n",(0,o.jsx)(t.p,{children:"If you encounter an error that looks like this:"}),"\n",(0,o.jsx)(t.pre,{children:(0,o.jsx)(t.code,{children:"                                        \u250c\u2500\u2500\u2500\u2500\u2500\u2510\n                                        \u2502start\u2502\n                                        \u2514\u2500\u252c\u2500\u252c\u2500\u2518\n                                        \u2502 \u2502\n                                        \u2502 \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n                                        \u2502                      \u2502\n                                        v                      v\n    \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510 \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n    \u2502download-departures FAILED PT5.183334S\u2502 \u2502download-airports SUCCEEDED PT1.91309S\u2502\n    \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518 \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n    [main]\n    Exception in thread \"main\" io.smartdatalake.util.dag.TaskFailedException: Task download-departures failed. Root cause is 'WebserviceException: Read timed out'\n"})}),"\n",(0,o.jsx)(t.p,{children:"Since both web servers are freely available on the internet, they might be overloaded by traffic. If the download fails because of a timeout, either increase readTimeoutMs or wait a couple of minutes and try again. If the download still won't work (or if you just get empty files), you can copy the contents of the folder data-fallback-download into your data folder."}),"\n",(0,o.jsx)(t.h2,{id:"how-to-kill-sdlb-if-it-hangs",children:"How to kill SDLB if it hangs"}),"\n",(0,o.jsx)(t.p,{children:"In case you run into issues when executing your pipeline and you want to terminate the process\nyou can use this docker command to list the running containers:"}),"\n",(0,o.jsx)(t.pre,{children:(0,o.jsx)(t.code,{children:"docker ps\n"})}),"\n",(0,o.jsxs)(t.p,{children:["While your feed-execution is running, the output of this command will contain\nan execution with the image name ",(0,o.jsxs)(t.em,{children:["sdl-spark",":latest"]}),".\nUse the container id to stop the container by typing:"]}),"\n",(0,o.jsx)(t.pre,{children:(0,o.jsx)(t.code,{children:"docker containter stop <container id>\n"})}),"\n",(0,o.jsx)(t.h2,{id:"error-08001-javanetconnectexception--error-connecting-to-server-localhost-on-port-1527",children:"ERROR 08001: java.net.ConnectException : Error connecting to server localhost on port 1527"}),"\n",(0,o.jsxs)(t.p,{children:["This likely happens during or after part 2 if you forget to add ",(0,o.jsx)(t.code,{children:"--pod getting-started"})," to your podman command.\nRemember that SDL now needs to communicate with the Metastore that we are starting.\nTo allow this communication, you need to make sure that SDL and the metastore are running in the same pod."]})]})}function h(e={}){const{wrapper:t}={...(0,i.a)(),...e.components};return t?(0,o.jsx)(t,{...e,children:(0,o.jsx)(c,{...e})}):c(e)}},1151:(e,t,n)=>{n.d(t,{Z:()=>r,a:()=>a});var o=n(7294);const i={},s=o.createContext(i);function a(e){const t=o.useContext(s);return o.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function r(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(i):e.components||i:a(e.components),o.createElement(s.Provider,{value:t},e.children)}}}]);