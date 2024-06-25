"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[7957],{6350:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>c,contentTitle:()=>a,default:()=>h,frontMatter:()=>s,metadata:()=>r,toc:()=>l});var i=n(4848),o=n(8453);const s={title:"Get Airports"},a=void 0,r={id:"getting-started/part-1/get-airports",title:"Get Airports",description:"Goal",source:"@site/docs/getting-started/part-1/get-airports.md",sourceDirName:"getting-started/part-1",slug:"/getting-started/part-1/get-airports",permalink:"/docs/getting-started/part-1/get-airports",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-1/get-airports.md",tags:[],version:"current",frontMatter:{title:"Get Airports"},sidebar:"tutorialSidebar",previous:{title:"Get Departures",permalink:"/docs/getting-started/part-1/get-departures"},next:{title:"Select Columns",permalink:"/docs/getting-started/part-1/select-columns"}},c={},l=[{value:"Goal",id:"goal",level:2},{value:"Solution",id:"solution",level:2},{value:"Mess Up the Solution",id:"mess-up-the-solution",level:2},{value:"Try fixing it",id:"try-fixing-it",level:2}];function d(e){const t={a:"a",admonition:"admonition",br:"br",code:"code",em:"em",h2:"h2",li:"li",ol:"ol",p:"p",pre:"pre",strong:"strong",...(0,o.R)(),...e.components};return(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(t.h2,{id:"goal",children:"Goal"}),"\n",(0,i.jsxs)(t.p,{children:["In this step, we will download airports master data from the website described in ",(0,i.jsx)(t.a,{href:"../get-input-data",children:"Inputs"}),' using Smart Data Lake Builder.\nBecause this step is very similar to the previous one, we will make some "mistake" on purpose to demonstrate how to deal with config errors.']}),"\n",(0,i.jsx)(t.p,{children:"Just like in the previous step, we need one action and two DataObjects.\nExcept for the object and action names, the config to add here is almost identical to the previous step."}),"\n",(0,i.jsx)(t.p,{children:'You are welcome to try to implement it yourself before continuing.\nJust as in the previous step, you can use "download" as feed name.'}),"\n",(0,i.jsx)(t.h2,{id:"solution",children:"Solution"}),"\n",(0,i.jsxs)(t.p,{children:["You should now have a file similar to ",(0,i.jsx)(t.a,{target:"_blank",href:n(5109).A+"",children:"this"})," one.\nThe only notable difference is that you had to use the type ",(0,i.jsx)(t.strong,{children:"CsvFileDataObject"})," for the airports.csv file,\nsince this is what the second webservice answers with.\nNote that you would not get an error at this point if you had chosen another file format.\nSince we use ",(0,i.jsx)(t.em,{children:"FileTransferAction"})," in both cases, the files are copied without the content being interpreted yet."]}),"\n",(0,i.jsxs)(t.p,{children:["You can start the same ",(0,i.jsx)(t.code,{children:"docker run"})," command as before and you should see that both directories\n",(0,i.jsx)(t.em,{children:"stg-airports"})," and ",(0,i.jsx)(t.em,{children:"stg-departures"})," have new files now.\nNotice that since both actions have the same feed, the option ",(0,i.jsx)(t.code,{children:"--feed-sel download"})," executes both of them."]}),"\n",(0,i.jsx)(t.h2,{id:"mess-up-the-solution",children:"Mess Up the Solution"}),"\n",(0,i.jsxs)(t.p,{children:["Now let's see what happens when things don't go as planned.\nFor that, replace your config file with the contents of ",(0,i.jsx)(t.a,{target:"_blank",href:n(144).A+"",children:"this"})," file.\nWhen you start the ",(0,i.jsx)(t.code,{children:"docker run"})," command again, you will see two errors:"]}),"\n",(0,i.jsxs)(t.ol,{children:["\n",(0,i.jsx)(t.li,{children:'The name of the DataObject "NOPEext-departures" does not match with the inputId of the action download-departures.\nThis is a very common error and the stacktrace should help you to quickly find and correct it'}),"\n"]}),"\n",(0,i.jsx)(t.pre,{children:(0,i.jsx)(t.code,{children:'    Exception in thread "main" io.smartdatalake.config.ConfigurationException: (Action~download-departures) [] key not found: DataObject~ext-departures\n'})}),"\n",(0,i.jsx)(t.p,{children:"As noted before, SDL will often use Action-IDs and DataObject-IDs to communicate where to look in your configuration files."}),"\n",(0,i.jsxs)(t.ol,{start:"2",children:["\n",(0,i.jsx)(t.li,{children:"An unknown DataObject type was used. In this example, stg-airports was assigned the type UnicornFileDataObject, which does not exist."}),"\n"]}),"\n",(0,i.jsx)(t.pre,{children:(0,i.jsx)(t.code,{children:'    Exception in thread "main" io.smartdatalake.config.ConfigurationException: (DataObject~stg-airports) ClassNotFoundException: io.smartdatalake.workflow.dataobject.UnicornFileDataObject\n'})}),"\n",(0,i.jsxs)(t.p,{children:["Internally, the types you choose are represented by Scala Classes.\nThese classes define all characteristics of a DataObject and all it's parameters, i.e. the url we defined in our WebserviceFileDataObject.\nThis also explains why you get a ",(0,i.jsx)(t.em,{children:"ClassNotFoundException"})," in this case."]}),"\n",(0,i.jsx)(t.h2,{id:"try-fixing-it",children:"Try fixing it"}),"\n",(0,i.jsx)(t.p,{children:"Try to fix one of the errors and keep the other one to see what happens: Nothing.\nWhy is that?"}),"\n",(0,i.jsx)(t.p,{children:"SDL validates your configuration file(s) before executing it's contents.\nIf the configuration does not make sense, it will abort before executing anything to minimize the chance that you'll end up in an inconsistent state."}),"\n",(0,i.jsx)(t.admonition,{type:"tip",children:(0,i.jsx)(t.p,{children:"During validation, the whole configuration is checked, not just the parts you are trying to execute.\nIf you have large configuration files, it can sometimes be confusing to see an error and realize that\nit's not on the part you are currently working on but in a different section."})}),"\n",(0,i.jsx)(t.p,{children:"SDL is built to detect configuration errors as early as possible (early-validation). It does this by going through several phases."}),"\n",(0,i.jsxs)(t.ol,{children:["\n",(0,i.jsxs)(t.li,{children:["Validate configuration",(0,i.jsx)(t.br,{}),"\n","validate superfluous attributes, missing mandatory attributes, attribute content and consistency when referencing other configuration objects."]}),"\n",(0,i.jsxs)(t.li,{children:[(0,i.jsx)(t.em,{children:"Prepare"})," phase",(0,i.jsx)(t.br,{}),"\n","validate preconditions, e.g. connections and existence of tables and directories."]}),"\n",(0,i.jsxs)(t.li,{children:[(0,i.jsx)(t.em,{children:"Init"})," phase",(0,i.jsx)(t.br,{}),"\n","executes the whole feed ",(0,i.jsx)(t.em,{children:"without any data"})," to spot incompatibilities between the Data Objects that cannot be spotted\nby just looking at the config file. For example a column which doesn't exist but is referenced in a later Action will cause the init phase to fail."]}),"\n",(0,i.jsxs)(t.li,{children:[(0,i.jsx)(t.em,{children:"Exec"})," phase",(0,i.jsx)(t.br,{}),"\n","only if all previous phases have been passed successfully, execution is started."]}),"\n"]}),"\n",(0,i.jsx)(t.p,{children:'When running SDL, you can clearly find "prepare", "init" and "exec" steps for every Action in the logs.'}),"\n",(0,i.jsxs)(t.p,{children:["See ",(0,i.jsx)(t.a,{href:"/docs/reference/executionPhases",children:"this page"})," for a detailed description on the execution phases of SDL."]}),"\n",(0,i.jsx)(t.p,{children:"Now is a good time to fix both errors in your configuration file and execute the action again."}),"\n",(0,i.jsx)(t.p,{children:"Early-validation is a core feature of SDL and will become more and more valuable with the increasing complexity of your data pipelines.\nSpeaking of increasing complexity: In the next step, we will begin transforming our data."})]})}function h(e={}){const{wrapper:t}={...(0,o.R)(),...e.components};return t?(0,i.jsx)(t,{...e,children:(0,i.jsx)(d,{...e})}):d(e)}},144:(e,t,n)=>{n.d(t,{A:()=>i});const i=n.p+"assets/files/application-part1-download-errors-a1cdc48499417f00a82e988c3ef2ea10.conf"},5109:(e,t,n)=>{n.d(t,{A:()=>i});const i=n.p+"assets/files/application-part1-download-02efb087b3115d19f48934d6cf0db8dc.conf"},8453:(e,t,n)=>{n.d(t,{R:()=>a,x:()=>r});var i=n(6540);const o={},s=i.createContext(o);function a(e){const t=i.useContext(s);return i.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function r(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(o):e.components||o:a(e.components),i.createElement(s.Provider,{value:t},e.children)}}}]);