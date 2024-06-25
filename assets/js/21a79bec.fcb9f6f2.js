"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[7633],{862:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>c,contentTitle:()=>a,default:()=>h,frontMatter:()=>s,metadata:()=>o,toc:()=>l});var i=t(4848),r=t(8453);const s={id:"deploy-microsoft-azure",title:"Deploy on Microsoft Azure Databricks"},a=void 0,o={id:"reference/deploy-microsoft-azure",title:"Deploy on Microsoft Azure Databricks",description:"Smart Data Lake Builder can be executed in multiple ways on Microsoft Azure:",source:"@site/docs/reference/deploy-microsoft-azure.md",sourceDirName:"reference",slug:"/reference/deploy-microsoft-azure",permalink:"/docs/reference/deploy-microsoft-azure",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/deploy-microsoft-azure.md",tags:[],version:"current",frontMatter:{id:"deploy-microsoft-azure",title:"Deploy on Microsoft Azure Databricks"},sidebar:"tutorialSidebar",previous:{title:"Deployment options",permalink:"/docs/reference/deploymentOptions"},next:{title:"Testing",permalink:"/docs/reference/testing"}},c={},l=[{value:"SDLB on Databricks",id:"sdlb-on-databricks",level:2}];function d(e){const n={a:"a",code:"code",em:"em",h2:"h2",li:"li",ol:"ol",p:"p",pre:"pre",ul:"ul",...(0,r.R)(),...e.components};return(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(n.p,{children:"Smart Data Lake Builder can be executed in multiple ways on Microsoft Azure:"}),"\n",(0,i.jsxs)(n.ul,{children:["\n",(0,i.jsx)(n.li,{children:"on Databricks"}),"\n",(0,i.jsx)(n.li,{children:"as containers orchestrated with Kubernetes"}),"\n",(0,i.jsx)(n.li,{children:"as virtual machine"}),"\n"]}),"\n",(0,i.jsx)(n.h2,{id:"sdlb-on-databricks",children:"SDLB on Databricks"}),"\n",(0,i.jsx)(n.p,{children:"Databricks has the advantage of pre-configurated features like ready-to-use Spark clusters, metastore, notebook support and integrated SQL endpoints."}),"\n",(0,i.jsxs)(n.p,{children:["At the time of this writing, a few extra steps are needed to overwrite specific libraries.\nWhen running a job in Databricks, a few dependencies are given and can not be simply overwritten with your own as described in the\n",(0,i.jsx)(n.a,{href:"https://docs.microsoft.com/en-us/azure/databricks/jobs#library-dependencies",children:"Azure documentation"}),".\nSince we use a newer version of typesafe config, we need to force the overwrite of this dependency.\nWe will create a cluster init script that downloads the library and saves it on the cluster, then use Sparks ChildFirstURLClassLoader to explicitly load our library first.\nThis can hopefully be simplified in the future."]}),"\n",(0,i.jsxs)(n.ol,{children:["\n",(0,i.jsxs)(n.li,{children:["\n",(0,i.jsx)(n.p,{children:"In your Azure portal, create a Databricks Workspace and launch it"}),"\n"]}),"\n",(0,i.jsxs)(n.li,{children:["\n",(0,i.jsx)(n.p,{children:"Create a cluster that fits your needs. For a first test you can use the miminal configuration of 1 Worker and 1 Driver node.\nThis example was tested on Databricks Runtime Version 6.2."}),"\n"]}),"\n",(0,i.jsxs)(n.li,{children:["\n",(0,i.jsxs)(n.p,{children:["Open the Advanced Options, Init Scripts and configure the path:\n",(0,i.jsx)(n.code,{children:"dbfs:/databricks/scripts/config-install.sh"})]}),"\n"]}),"\n",(0,i.jsxs)(n.li,{children:["\n",(0,i.jsx)(n.p,{children:"On your local machine, create a simple script called config-install.sh with the following content"}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-bash",children:"#!/bin/bash\nwget -O /databricks/jars/-----config-1.3.4.jar https://repo1.maven.org/maven2/com/typesafe/config/1.3.4/config-1.3.4.jar\n"})}),"\n"]}),"\n",(0,i.jsxs)(n.li,{children:["\n",(0,i.jsxs)(n.p,{children:["To copy this local file to your Databricks filesystem, use the ",(0,i.jsx)(n.a,{href:"https://docs.databricks.com/dev-tools/cli/index.html",children:"Databricks CLI"}),":"]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-bash",children:"databricks fs mkdirs dbfs:/databricks/scripts\ndatabricks fs cp \\&ltpath-to/config-install.sh\\&gt dbfs:/databricks/scripts/\n"})}),"\n",(0,i.jsx)(n.p,{children:"Now this script gets executed every time the cluster starts.\nIt will download the config library and put it in a place where the classloader can find it."}),"\n"]}),"\n",(0,i.jsxs)(n.li,{children:["\n",(0,i.jsx)(n.p,{children:"Start your cluster, check the event log to see if it's up.\nIf something is wrong with the init script, the cluster will not start."}),"\n"]}),"\n",(0,i.jsxs)(n.li,{children:["\n",(0,i.jsxs)(n.p,{children:["On your local machine, create a second the SDLB configuration file(s) e.g. called application.conf.\nFor more details of the configuration file(s) see ",(0,i.jsx)(n.a,{href:"/docs/reference/hoconOverview",children:"hocon overview"}),"."]}),"\n"]}),"\n",(0,i.jsxs)(n.li,{children:["\n",(0,i.jsx)(n.p,{children:"Upload the file(s) to a conf folder in dbfs:"}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{className:"language-bash",children:"databricks fs mkdirs dbfs:/conf\ndatabricks fs cp path-to/application.conf dbfs:/conf/\n"})}),"\n"]}),"\n",(0,i.jsxs)(n.li,{children:["\n",(0,i.jsxs)(n.p,{children:["Now create a Job with the following details:\nIf you don't have the JAR file yet, see ",(0,i.jsx)(n.a,{href:"/docs/reference/build#building-jar-with-runtime-dependencies",children:"build fat jar"})," on how to build it (using the Maven profile fat-jar)."]}),"\n",(0,i.jsx)(n.p,{children:"Task: Upload JAR - Choose the smartdatalake-&ltversion&gt-jar-with-dependencies.jar"}),"\n",(0,i.jsxs)(n.p,{children:["Main Class: io.smartdatalake.app.LocalSmartDataLakeBuilder\nArguments: ",(0,i.jsx)(n.code,{children:'["-c", "file:///dbfs/conf/", "--feed-sel", "download"]'})]}),"\n",(0,i.jsxs)(n.p,{children:["The option ",(0,i.jsx)(n.em,{children:"--override-jars"})," is set automatically to the correct value for DatabricksConfigurableApp.\nIf you want to override any additional libraries, you can provide a list with this option."]}),"\n"]}),"\n",(0,i.jsxs)(n.li,{children:["\n",(0,i.jsx)(n.p,{children:"Finally the job can be started and the result checked."}),"\n"]}),"\n"]}),"\n",(0,i.jsxs)(n.p,{children:["For a detailed example see ",(0,i.jsx)(n.a,{href:"../../blog/sdl-databricks",children:"Deployment on Databricks"})," blog post."]})]})}function h(e={}){const{wrapper:n}={...(0,r.R)(),...e.components};return n?(0,i.jsx)(n,{...e,children:(0,i.jsx)(d,{...e})}):d(e)}},8453:(e,n,t)=>{t.d(n,{R:()=>a,x:()=>o});var i=t(6540);const r={},s=i.createContext(r);function a(e){const n=i.useContext(s);return i.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function o(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:a(e.components),i.createElement(s.Provider,{value:n},e.children)}}}]);