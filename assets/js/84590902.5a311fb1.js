"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[8142],{4152:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>c,contentTitle:()=>i,default:()=>h,frontMatter:()=>s,metadata:()=>a,toc:()=>d});var r=t(4848),o=t(8453);const s={id:"streaming",title:"Streaming"},i=void 0,a={id:"reference/streaming",title:"Streaming",description:"This page is under review.",source:"@site/docs/reference/streaming.md",sourceDirName:"reference",slug:"/reference/streaming",permalink:"/docs/reference/streaming",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/streaming.md",tags:[],version:"current",frontMatter:{id:"streaming",title:"Streaming"},sidebar:"tutorialSidebar",previous:{title:"Agents (Experimental)",permalink:"/docs/reference/agents"},next:{title:"Deployment options",permalink:"/docs/reference/deploymentOptions"}},c={},d=[];function u(e){const n={admonition:"admonition",code:"code",li:"li",p:"p",ul:"ul",...(0,o.R)(),...e.components};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(n.admonition,{type:"warning",children:(0,r.jsx)(n.p,{children:"This page is under review."})}),"\n",(0,r.jsxs)(n.p,{children:["You can execute any DAG in streaming mode by using commandline option ",(0,r.jsx)(n.code,{children:"--streaming"}),".\nIn streaming mode SDL executes the Exec-phase of the same DAG continuously, processing your data incrementally.\nSDL discerns between synchronous and asynchronous actions:"]}),"\n",(0,r.jsxs)(n.ul,{children:["\n",(0,r.jsx)(n.li,{children:"Synchronous actions are executed one after another in the DAG, they are synchronized with their predecessors and successors."}),"\n",(0,r.jsx)(n.li,{children:"Asynchronous actions have their own rhythm. They are executed not synchronized from the other actions, except for the first increment. In the first increment their start is synchronized with their predecessors and the first execution is waited for before starting their successors. This allows to maintain execution order for initial loads, where tables and directories might need to be created one after another."}),"\n"]}),"\n",(0,r.jsx)(n.p,{children:"You can mix synchronous and asynchronous actions in the same DAG. Asynchronous actions are started in the first increment. Synchronous actions are executed in each execution of the DAG."}),"\n",(0,r.jsx)(n.p,{children:'Whether an action is synchronous or asynchronous depends on the execution engine used. For now only "Spark Structured Streaming" is an asynchronous execution engine. It is configured by setting execution mode SparkStreamingMode to an action.'}),"\n",(0,r.jsxs)(n.p,{children:["You can control the minimum delay between synchronous streaming runs by setting configuration ",(0,r.jsx)(n.code,{children:"global.synchronousStreamingTriggerIntervalSec"})," to a certain amount of seconds.\nFor asynchronous streaming actions this is controlled by the corresponding streaming mode, e.g. SparkStreamingMode."]})]})}function h(e={}){const{wrapper:n}={...(0,o.R)(),...e.components};return n?(0,r.jsx)(n,{...e,children:(0,r.jsx)(u,{...e})}):u(e)}},8453:(e,n,t)=>{t.d(n,{R:()=>i,x:()=>a});var r=t(6540);const o={},s=r.createContext(o);function i(e){const n=r.useContext(s);return r.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function a(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(o):e.components||o:i(e.components),r.createElement(s.Provider,{value:n},e.children)}}}]);