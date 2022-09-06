"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[859],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>u});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var c=a.createContext({}),l=function(e){var t=a.useContext(c),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=l(e.components);return a.createElement(c.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,c=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),m=l(n),u=r,h=m["".concat(c,".").concat(u)]||m[u]||d[u]||i;return n?a.createElement(h,o(o({ref:t},p),{},{components:n})):a.createElement(h,o({ref:t},p))}));function u(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=m;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s.mdxType="string"==typeof e?e:r,o[1]=s;for(var l=2;l<i;l++)o[l]=n[l];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},7702:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>c,contentTitle:()=>o,default:()=>d,frontMatter:()=>i,metadata:()=>s,toc:()=>l});var a=n(7462),r=(n(7294),n(3905));const i={title:"Using Airbyte connector to inspect github data",description:"A short example using Airbyte github connector",slug:"sdl-airbyte",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["Airbyte","Connector"],image:"airbyte.png",hide_table_of_contents:!1},o=void 0,s={permalink:"/blog/sdl-airbyte",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2022-03-18-SDL-airbyte/2022-03-18-SDL-airbyte.md",source:"@site/blog/2022-03-18-SDL-airbyte/2022-03-18-SDL-airbyte.md",title:"Using Airbyte connector to inspect github data",description:"A short example using Airbyte github connector",date:"2022-03-18T00:00:00.000Z",formattedDate:"March 18, 2022",tags:[{label:"Airbyte",permalink:"/blog/tags/airbyte"},{label:"Connector",permalink:"/blog/tags/connector"}],readingTime:4.46,hasTruncateMarker:!0,authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],frontMatter:{title:"Using Airbyte connector to inspect github data",description:"A short example using Airbyte github connector",slug:"sdl-airbyte",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["Airbyte","Connector"],image:"airbyte.png",hide_table_of_contents:!1},prevItem:{title:"Combine Spark and Snowpark to ingest and transform data in one pipeline",permalink:"/blog/sdl-snowpark"}},c={authorsImageUrls:[void 0]},l=[{value:"Prerequisites",id:"prerequisites",level:2},{value:"Optional Inspect the connector specification",id:"optional-inspect-the-connector-specification",level:2},{value:"Configuration",id:"configuration",level:2},{value:"Run and inspect results",id:"run-and-inspect-results",level:2},{value:"Summary",id:"summary",level:2}],p={toc:l};function d(e){let{components:t,...i}=e;return(0,r.kt)("wrapper",(0,a.Z)({},p,i,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("p",null,"This article presents the deployment of an ",(0,r.kt)("a",{parentName:"p",href:"https://airbyte.com"},"Airbyte Connector")," with Smart Data Lake Builder (SDLB).\nIn particular the ",(0,r.kt)("a",{parentName:"p",href:"https://docs.airbyte.com/integrations/sources/github"},"github connector")," is implemented using the python sources."),(0,r.kt)("p",null,"Airbyte is a framework to sync data from a variety of sources (APIs and databases) into data warehouses and data lakes.\nIn this example an Airbyte connector is utilized to stream data into Smart Data Lake (SDL).\nTherefore, the ",(0,r.kt)("a",{parentName:"p",href:"http://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=2-2"},"Airbyte dataObject")," is used and will be configured.\nThe general ",(0,r.kt)("a",{parentName:"p",href:"https://docs.airbyte.com/understanding-airbyte/airbyte-specification#source"},"Airbyte connector handling")," is implemented in SDL, which includes the 4 main steps:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"spec"),": receiving the specification of the connector"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"check"),": validating the specified configuration"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"discover"),": gather a catalog of available streams and its schemas"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"read"),": collect the actual data")),(0,r.kt)("p",null,"The actual connector is not provided in the SDL repository and needs to be obtained from the ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/airbytehq/airbyte"},"Airbyte repository"),". Besides the ",(0,r.kt)("a",{parentName:"p",href:"https://docs.airbyte.com/integrations"},"list of existing connectors"),", custom connectors could be implemented in Python or Javascript. "),(0,r.kt)("p",null,"The following description builds on top of the example setup from the ",(0,r.kt)("a",{parentName:"p",href:"../../docs/getting-started/setup"},"getting-started")," guide, using ",(0,r.kt)("a",{parentName:"p",href:"https://docs.podman.io"},"Podman")," as container engine within a ",(0,r.kt)("a",{parentName:"p",href:"https://docs.microsoft.com/en-us/windows/wsl/install"},"WSL")," Ubuntu image. "),(0,r.kt)("p",null,"The ",(0,r.kt)("a",{parentName:"p",href:"https://docs.airbyte.com/integrations/sources/github"},"github connector")," is utilized to gather data about a specific repository."),(0,r.kt)("h2",{id:"prerequisites"},"Prerequisites"),(0,r.kt)("p",null,"After downloading and installing all necessary packages, the connector is briefly tested:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Python"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"../../docs/getting-started/troubleshooting/docker-on-windows"},"Podman including ",(0,r.kt)("inlineCode",{parentName:"a"},"podman-compose"))," or ",(0,r.kt)("a",{parentName:"li",href:"https://www.docker.com/get-started"},"Docker")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"https://github.com/smart-data-lake/getting-started/archive/refs/heads/master.zip"},"SDL example"),", download and unpack: ",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"git clone https://github.com/smart-data-lake/getting-started.git SDL_airbyte\ncd SDL_airbyte\n"))),(0,r.kt)("li",{parentName:"ul"},"download the ",(0,r.kt)("a",{parentName:"li",href:"https://github.com/airbytehq/airbyte"},"Airbyte repository")," ",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"git clone https://github.com/airbytehq/airbyte.git\n")),"Alternatively, only the target connector can be downloaded:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"svn checkout https://github.com/airbytehq/airbyte/trunk/airbyte-integrations/connectors/source-github\n")),"Here the Airbyte ",(0,r.kt)("inlineCode",{parentName:"li"},"airbyte/airbyte-integrations/connectors/source-github/")," directory is copied into the ",(0,r.kt)("inlineCode",{parentName:"li"},"SDL_airbyte")," directory for handy calling the connector.")),(0,r.kt)("h2",{id:"optional-inspect-the-connector-specification"},"[Optional]"," Inspect the connector specification"),(0,r.kt)("p",null,"The first connector command ",(0,r.kt)("inlineCode",{parentName:"p"},"spec")," provides the connector specification. This is the basis to create a connector configuration. To run the connector as is, the Python ",(0,r.kt)("inlineCode",{parentName:"p"},"airbyte-cdk")," package needs to be installed and the connector can be launched:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Install Python airbyte-cdk: ",(0,r.kt)("inlineCode",{parentName:"li"},"pip install airbyte_cdk")),(0,r.kt)("li",{parentName:"ul"},"try the connector: ",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"cd SDL_airbyte\npython source_github/main.py spec | python -m json.tool\n")),"This provides a ",(0,r.kt)("a",{target:"_blank",href:n(8461).Z},"JSON string")," with the connector specification. The fields listed under ",(0,r.kt)("inlineCode",{parentName:"li"},"properties")," are relevant for the configuration (compare with the configuration  used later). ")),(0,r.kt)("h2",{id:"configuration"},"Configuration"),(0,r.kt)("p",null,"To launch Smart Data Lake Builder (SDLB) with the Airbyte connector the following needs to be modified:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"add the Airbyte ",(0,r.kt)("strong",{parentName:"p"},(0,r.kt)("em",{parentName:"strong"},"dataObject"))," with its configuration to the ",(0,r.kt)("inlineCode",{parentName:"p"},"config/application.conf"),":"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Python"},'dataObjects {\n  ext-commits {\n    type = AirbyteDataObject\n    config = {\n      "credentials": {\n        "personal_access_token": "<yourPersonalAccessToken>" ### enter your personal access token here\n      },\n      "repository": "smart-data-lake/smart-data-lake",\n      "start_date": "2021-02-01T00:00:00Z",\n      "branch": "documentation develop-spark3 develop-spark2",\n      "page_size_for_large_streams": 100\n    },\n    streamName = "commits",\n    cmd = {\n      type = CmdScript\n      name = "airbyte_connector_github"\n      linuxCmd = "python3 /mnt/source-github/main.py"\n    }\n  }\n...\n  stg-commits {\n   type = DeltaLakeTableDataObject\n   path = "~{id}"\n   table {\n    db = "default"\n    name = "stg_commits"\n    primaryKey = [created_at]\n    }\n  }\n')),(0,r.kt)("p",{parentName:"li"},"Note the options set for ",(0,r.kt)("inlineCode",{parentName:"p"},"ext-commits")," which define the Airbyte connector settings.\nWhile the ",(0,r.kt)("inlineCode",{parentName:"p"},"config")," varies from connector to connector, the remaining fields are SDL specific.\nThe ",(0,r.kt)("inlineCode",{parentName:"p"},"streamName")," selects the stream, exactly one.\nIf multiple streams should be collected, multiple dataObjects need to be defined.\nIn ",(0,r.kt)("inlineCode",{parentName:"p"},"linuxCmd")," the actual connector script is called.\nIn our case we will mount the connector directory into the SDL container. ")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"also add the definition of the data stream ",(0,r.kt)("strong",{parentName:"p"},(0,r.kt)("em",{parentName:"strong"},"action"))," to pipe the coming data stream into a ",(0,r.kt)("inlineCode",{parentName:"p"},"DeltaLakeTableDataObject"),":"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"  actions {\n    download-commits {\n      type = CopyAction\n      inputId = ext-commits\n      outputId = stg-commits\n      metadata {\n        feed = download\n      }\n    }\n...\n"))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Since Airbyte will be called as Python script in the sdl container, we need to (re-)build the container with Python support and the Python ",(0,r.kt)("inlineCode",{parentName:"p"},"airbyte-cdk")," package.\nTherefore, in the Dockerfile we add:"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},"RUN \\\napt update && \\\napt --assume-yes install python3 python3-pip && \\\npip3 install airbyte-cdk~=0.1.25\n")),(0,r.kt)("p",{parentName:"li"},"and rebuild "),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman build . -t sdl-spark\n")))),(0,r.kt)("p",null,"Now we are ready to go. My full ",(0,r.kt)("a",{target:"_blank",href:n(3468).Z},"SDLB config file")," additionally includes the pull-request stream."),(0,r.kt)("h2",{id:"run-and-inspect-results"},"Run and inspect results"),(0,r.kt)("p",null,"Since the data will be streamed into a ",(0,r.kt)("inlineCode",{parentName:"p"},"DeltaLakeTableDataObject"),", the metastore container is necessary. Further, we aim to inspect the data using the Polynote notebook. Thus, first these containers are launched using (in the SDL example base directory):"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman-compose up\npodman pod ls\n")),(0,r.kt)("p",null,"With the second command we can verify the pod name and both running containers in it (should be three including the infra container)."),(0,r.kt)("p",null,"Then, the SDLB can be launched using the additional option to mount the Airbyte connector directory:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"podman run --hostname localhost --rm --pod sdl_airbyte -v ${PWD}/source-github/:/mnt/source-github -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\n")),(0,r.kt)("p",null,"The output presents the successful run of the workflow:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-Bash"},"2022-03-16 07:54:03 INFO  ActionDAGRun$ActionEventListener - Action~download-commits[CopyAction]: Exec succeeded [dag-1-80]\n2022-03-16 07:54:03 INFO  ActionDAGRun$ - exec SUCCEEDED for dag 1:\n                 \u250c\u2500\u2500\u2500\u2500\u2500\u2510\n                 \u2502start\u2502\n                 \u2514\u2500\u2500\u2500\u252c\u2500\u2518\n                     \u2502\n                     v\n \u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510\n \u2502download-commits SUCCEEDED PT11.686865S\u2502\n \u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n     [main]\n2022-03-16 07:54:03 INFO  LocalSmartDataLakeBuilder$ - LocalSmartDataLakeBuilder finished successfully: SUCCEEDED=1 [main]\n2022-03-16 07:54:03 INFO  SparkUI - Stopped Spark web UI at http://localhost:4040 [shutdown-hook-0]\n")),(0,r.kt)("p",null,"Launching Polynote ",(0,r.kt)("inlineCode",{parentName:"p"},"localhost:8192")," in the browser, we can inspect data and develop further workflows. Here an example, where the commits are listed, which were committed in the name of someone else, excluding the web-flow. See ",(0,r.kt)("a",{target:"_blank",href:n(680).Z},"Polynote Notebook"),"\n",(0,r.kt)("img",{alt:"polynote example",src:n(904).Z,width:"1018",height:"810"})),(0,r.kt)("h2",{id:"summary"},"Summary"),(0,r.kt)("p",null,"The Airbyte connectors provide easy access to a variety of data sources. The connectors can be utilized in SDLB with just a few settings. This also works great for more complex interfaces."))}d.isMDXComponent=!0},680:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/SelectingData-fc913585bab1ef41a6b1f32ee50d5adb.ipynb"},3468:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/application-b6a5a6494622e5ff40741395d829f558.conf"},8461:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/github_spec_out-e78cbc05ed4f12e8414017c3698a8edb.json"},904:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/images/polynote_commits-0877fa6cab02c46db63471b8220af7ea.png"}}]);