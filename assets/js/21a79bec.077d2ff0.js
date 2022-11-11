"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2795],{3905:(e,t,a)=>{a.d(t,{Zo:()=>p,kt:()=>f});var r=a(7294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},i=Object.keys(e);for(r=0;r<i.length;r++)a=i[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)a=i[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var s=r.createContext({}),c=function(e){var t=r.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},p=function(e){var t=c(e.components);return r.createElement(s.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},u=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,i=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),u=c(a),f=n,m=u["".concat(s,".").concat(f)]||u[f]||d[f]||i;return a?r.createElement(m,o(o({ref:t},p),{},{components:a})):r.createElement(m,o({ref:t},p))}));function f(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var i=a.length,o=new Array(i);o[0]=u;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:n,o[1]=l;for(var c=2;c<i;c++)o[c]=a[c];return r.createElement.apply(null,o)}return r.createElement.apply(null,a)}u.displayName="MDXCreateElement"},4039:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>s,contentTitle:()=>o,default:()=>d,frontMatter:()=>i,metadata:()=>l,toc:()=>c});var r=a(7462),n=(a(7294),a(3905));const i={id:"deploy-microsoft-azure",title:"Deploy on Microsoft Azure Databricks"},o=void 0,l={unversionedId:"reference/deploy-microsoft-azure",id:"reference/deploy-microsoft-azure",title:"Deploy on Microsoft Azure Databricks",description:"Smart Data Lake Builder can be executed in multiple ways on Microsoft Azure:",source:"@site/docs/reference/deploy-microsoft-azure.md",sourceDirName:"reference",slug:"/reference/deploy-microsoft-azure",permalink:"/docs/reference/deploy-microsoft-azure",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/deploy-microsoft-azure.md",tags:[],version:"current",frontMatter:{id:"deploy-microsoft-azure",title:"Deploy on Microsoft Azure Databricks"},sidebar:"docs",previous:{title:"Deployment options",permalink:"/docs/reference/deploymentOptions"},next:{title:"Testing",permalink:"/docs/reference/testing"}},s={},c=[{value:"SDLB on Databricks",id:"sdlb-on-databricks",level:2}],p={toc:c};function d(e){let{components:t,...a}=e;return(0,n.kt)("wrapper",(0,r.Z)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("p",null,"Smart Data Lake Builder can be executed in multiple ways on Microsoft Azure:"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"on Databricks"),(0,n.kt)("li",{parentName:"ul"},"as containers orchestrated with Kubernetes"),(0,n.kt)("li",{parentName:"ul"},"as virtual machine")),(0,n.kt)("h2",{id:"sdlb-on-databricks"},"SDLB on Databricks"),(0,n.kt)("p",null,"Databricks has the advantage of pre-configurated features like ready-to-use Spark clusters, metastore, notebook support and integrated SQL endpoints."),(0,n.kt)("p",null,"At the time of this writing, a few extra steps are needed to overwrite specific libraries.\nWhen running a job in Databricks, a few dependencies are given and can not be simply overwritten with your own as described in the\n",(0,n.kt)("a",{parentName:"p",href:"https://docs.microsoft.com/en-us/azure/databricks/jobs#library-dependencies"},"Azure documentation"),".\nSince we use a newer version of typesafe config, we need to force the overwrite of this dependency.\nWe will create a cluster init script that downloads the library and saves it on the cluster, then use Sparks ChildFirstURLClassLoader to explicitly load our library first.\nThis can hopefully be simplified in the future."),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"In your Azure portal, create a Databricks Workspace and launch it")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Create a cluster that fits your needs. For a first test you can use the miminal configuration of 1 Worker and 1 Driver node.\nThis example was tested on Databricks Runtime Version 6.2.")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Open the Advanced Options, Init Scripts and configure the path:\n",(0,n.kt)("inlineCode",{parentName:"p"},"dbfs:/databricks/scripts/config-install.sh"))),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"On your local machine, create a simple script called config-install.sh with the following content"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-bash"},"#!/bin/bash\nwget -O /databricks/jars/-----config-1.3.4.jar https://repo1.maven.org/maven2/com/typesafe/config/1.3.4/config-1.3.4.jar\n"))),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"To copy this local file to your Databricks filesystem, use the ",(0,n.kt)("a",{parentName:"p",href:"https://docs.databricks.com/dev-tools/cli/index.html"},"Databricks CLI"),":"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-bash"},"databricks fs mkdirs dbfs:/databricks/scripts\ndatabricks fs cp \\&ltpath-to/config-install.sh\\&gt dbfs:/databricks/scripts/\n")),(0,n.kt)("p",{parentName:"li"},"Now this script gets executed every time the cluster starts.\nIt will download the config library and put it in a place where the classloader can find it.")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Start your cluster, check the event log to see if it's up.\nIf something is wrong with the init script, the cluster will not start.")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"On your local machine, create a second the SDLB configuration file(s) e.g. called application.conf.\nFor more details of the configuration file(s) see ",(0,n.kt)("a",{parentName:"p",href:"/docs/reference/hoconOverview"},"hocon overview"),". ")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Upload the file(s) to a conf folder in dbfs:"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-bash"},"databricks fs mkdirs dbfs:/conf\ndatabricks fs cp path-to/application.conf dbfs:/conf/\n"))),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Now create a Job with the following details:\nIf you don't have the JAR file yet, see ",(0,n.kt)("a",{parentName:"p",href:"/docs/reference/build#building-jar-with-runtime-dependencies"},"build fat jar")," on how to build it (using the Maven profile fat-jar)."),(0,n.kt)("p",{parentName:"li"}," Task: Upload JAR - Choose the smartdatalake-","<","version",">","-jar-with-dependencies.jar"),(0,n.kt)("p",{parentName:"li"}," Main Class: io.smartdatalake.app.LocalSmartDataLakeBuilder\nArguments: ",(0,n.kt)("inlineCode",{parentName:"p"},'["-c", "file:///dbfs/conf/", "--feed-sel", "download"]')),(0,n.kt)("p",{parentName:"li"}," The option ",(0,n.kt)("em",{parentName:"p"},"--override-jars")," is set automatically to the correct value for DatabricksConfigurableApp.\nIf you want to override any additional libraries, you can provide a list with this option.")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Finally the job can be started and the result checked. "))),(0,n.kt)("p",null,"For a detailed example see ",(0,n.kt)("a",{parentName:"p",href:"../../blog/sdl-databricks"},"Deployment on Databricks")," blog post."))}d.isMDXComponent=!0}}]);