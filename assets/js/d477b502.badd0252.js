"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[311],{3905:function(e,t,a){a.d(t,{Zo:function(){return p},kt:function(){return m}});var n=a(7294);function o(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function r(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?r(Object(a),!0).forEach((function(t){o(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):r(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,n,o=function(e,t){if(null==e)return{};var a,n,o={},r=Object.keys(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||(o[a]=e[a]);return o}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(o[a]=e[a])}return o}var l=n.createContext({}),d=function(e){var t=n.useContext(l),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},p=function(e){var t=d(e.components);return n.createElement(l.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},u=n.forwardRef((function(e,t){var a=e.components,o=e.mdxType,r=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),u=d(a),m=o,h=u["".concat(l,".").concat(m)]||u[m]||c[m]||r;return a?n.createElement(h,i(i({ref:t},p),{},{components:a})):n.createElement(h,i({ref:t},p))}));function m(e,t){var a=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var r=a.length,i=new Array(r);i[0]=u;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:o,i[1]=s;for(var d=2;d<r;d++)i[d]=a[d];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}u.displayName="MDXCreateElement"},8250:function(e,t,a){a.r(t),a.d(t,{frontMatter:function(){return s},contentTitle:function(){return l},metadata:function(){return d},toc:function(){return p},default:function(){return u}});var n=a(7462),o=a(3366),r=(a(7294),a(3905)),i=["components"],s={title:"Get Departures"},l=void 0,d={unversionedId:"getting-started/part-1/get-departures",id:"getting-started/part-1/get-departures",title:"Get Departures",description:"Goal",source:"@site/docs/getting-started/part-1/get-departures.md",sourceDirName:"getting-started/part-1",slug:"/getting-started/part-1/get-departures",permalink:"/docs/getting-started/part-1/get-departures",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-1/get-departures.md",tags:[],version:"current",frontMatter:{title:"Get Departures"},sidebar:"docs",previous:{title:"Inputs",permalink:"/docs/getting-started/get-input-data"},next:{title:"Get Airports",permalink:"/docs/getting-started/part-1/get-airports"}},p=[{value:"Goal",id:"goal",children:[],level:2},{value:"Config File",id:"config-file",children:[],level:2},{value:"Define departures objects",id:"define-departures-objects",children:[{value:"Naming Conventions",id:"naming-conventions",children:[],level:4}],level:2},{value:"Define download-ext-departures",id:"define-download-ext-departures",children:[],level:2},{value:"Try it out",id:"try-it-out",children:[],level:2}],c={toc:p};function u(e){var t=e.components,a=(0,o.Z)(e,i);return(0,r.kt)("wrapper",(0,n.Z)({},c,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h2",{id:"goal"},"Goal"),(0,r.kt)("p",null,"In this step, we will download plane departure data from the REST-Interface described in the previous step using Smart Data Lake Builder."),(0,r.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"Smart Data Lake = SDL")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"Throughout this documentation, we will mostly refer to ",(0,r.kt)("em",{parentName:"p"},"SDL")," which is just short for ",(0,r.kt)("em",{parentName:"p"},"Smart Data Lake")))),(0,r.kt)("h2",{id:"config-file"},"Config File"),(0,r.kt)("p",null,"With Smart Data Lake Builder, you describe your data pipelines in a config file using the ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/lightbend/config/blob/master/HOCON.md"},"HOCON")," format.\nAll HOCON features are supported so you could also split your configuration into several files. But for this first part, let's just use one file."),(0,r.kt)("p",null,"A data pipeline is composed of at least two entities: ",(0,r.kt)("em",{parentName:"p"},"DataObjects")," and ",(0,r.kt)("em",{parentName:"p"},"Actions"),"."),(0,r.kt)("p",null,"An action defines how one (or multiple) DataObject are copied or transformed into another (or multiple) DataObject.\nIn every data pipeline, you will have at least one ",(0,r.kt)("em",{parentName:"p"},"DataObject")," for your input and one for your output.\nIf you have more than one action, you will also have at least one ",(0,r.kt)("em",{parentName:"p"},"DataObject")," for each intermediary step between two actions."),(0,r.kt)("p",null,"In our case, in order to get our departure data, we are going to build one action. Hence, we need one DataObject for our input, and one for our output.\nCreate a directory called config in your current working directory and an empty file called application.conf. This is where we will define our data pipeline."),(0,r.kt)("h2",{id:"define-departures-objects"},"Define departures objects"),(0,r.kt)("p",null,"Add the following lines to your configuration file:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'dataObjects {\n  ext-departures {\n    type = WebserviceFileDataObject\n    url = "https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1630200800&end=1630310979"\n    readTimeoutMs=200000\n  }\n  stg-departures {\n    type = JsonFileDataObject\n    path = "~{id}"\n  }\n}\n')),(0,r.kt)("p",null,"Here, we first created the DataObjects section. This section will contain our DataObjects of our pipeline.\nInside, we defined two DataObjects to start with."),(0,r.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"The names ",(0,r.kt)("em",{parentName:"p"},"ext-departures")," and ",(0,r.kt)("em",{parentName:"p"},"stg-departures")," are called DataObject-ID.\nThey uniquely define the data object and we will frequently refer to these IDs as does SDL, i.e. in error messages.."),(0,r.kt)("p",{parentName:"div"},"You will see further down, that actions also have a name that uniquely identifies them, they are called Action-ID."))),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"ext-departures:",(0,r.kt)("br",{parentName:"p"}),"\n","This data object acts as a source in our action and defines where we get our departure information from.\nWe set its type to WebserviceFileDataObject to tell SDL that this is a webservice call returning a file.\nSDL comes with a broad set of predefined data object types and can easily be extended. More on that later.\nEach type of data object comes with its own set of parameters. For a WebserviceFileDataObject, the only mandatory one is the url, so we set that as well.\nWe also set the option readTimeoutMs to a couple of seconds because the Rest-Service can be slow to respond.")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"stg-departures:",(0,r.kt)("br",{parentName:"p"}),"\n","This data object acts as a target for our first action, so where and how to download the file to.\nWe set type to JsonFileDataObject because we know from before that the webservice will return a json file.\nPath defines where the file will be stored. You could choose any name you want, but most of the time, the name of your DataObject is a good fit.\nInstead of writing ",(0,r.kt)("em",{parentName:"p"},"stg-departures")," again,\nwe used the placeholder ",(0,r.kt)("em",{parentName:"p"},"{~id}")," which gets replaced by the DataObject-ID. Don't forget to surround that placeholder\nwith double quotes so that it is interpreted as a string.\nWe defined a relative path - it is relative to the working directory SDL is started in.\nThe working directory has been set to the ",(0,r.kt)("em",{parentName:"p"},"data")," directory in the Dockerfile by setting the JVM Property "),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},"-Duser.dir=/mnt/data\n")),(0,r.kt)("p",{parentName:"li"},"so that's why all your relative paths will start in the ",(0,r.kt)("em",{parentName:"p"},"data")," directory."))),(0,r.kt)("h4",{id:"naming-conventions"},"Naming Conventions"),(0,r.kt)("p",null,"A quick note on our naming conventions: We typically follow some conventions when naming our data objects and actions.\nThey follow the layering conventions of our structured Smart Data Lake:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},'External data objects are prefixed with "ext"'),(0,r.kt)("li",{parentName:"ul"},"Your first action typically copies the data into the Data Lake, without making any changes.\nThis layer is called the ",(0,r.kt)("em",{parentName:"li"},"Staging Layer"),'.\nDataObjects of the staging layer are prefixed with "stg".'),(0,r.kt)("li",{parentName:"ul"},"When applying some basic transformation to your data that does not require any specific business logic, you store the result in the ",(0,r.kt)("em",{parentName:"li"},"Integration Layer"),'.\nSome of these transformations are data deduplication, historization and format standardization.\nDataObjects of the Integration Layer are prefixed with "int".'),(0,r.kt)("li",{parentName:"ul"},"When applying business logic to your data, you store the result in the ",(0,r.kt)("em",{parentName:"li"},"Business Tranformation Layer")," or ",(0,r.kt)("em",{parentName:"li"},"BTL"),' for short.\nDataObjects of the Business Transformation Layer are prefixed with "btl".')),(0,r.kt)("p",null,"You are of course free to use any other naming conventions, but it's worth to think about one at the beginning of your project."),(0,r.kt)("p",null,"In our case, we simply copy data exactly as is from an external source. Hence, our output DataObject belongs to the Staging Layer."),(0,r.kt)("h2",{id:"define-download-ext-departures"},"Define download-ext-departures"),(0,r.kt)("p",null,"After the dataObjects section, add the following lines to your configuration file:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"actions {\n    download-departures {\n      type = FileTransferAction\n      inputId = ext-departures\n      outputId = stg-departures\n      metadata {\n        feed = download\n      }\n    }\n}\n")),(0,r.kt)("p",null,"We added another section called actions, in which, you guessed it, all actions reside.\nWe defined our action and called it ",(0,r.kt)("em",{parentName:"p"},"download-departures"),"."),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"The type ",(0,r.kt)("em",{parentName:"li"},"FileTransferAction")," tells SDL that it should transfer a file from one place to another without any transformation.\nIn our case, from a location on the web to a place on your machine."),(0,r.kt)("li",{parentName:"ul"},"With inputId and outputId, we wire this action and the two data objects together."),(0,r.kt)("li",{parentName:"ul"},'Finally, we added some metadata to our action. Metadata is used to select the right actions to run.\nIn our case, we defined a feed called "download". When starting SDL, we can tell it to execute only actions corresponding to certain feeds.\nMultiple actions can be associated with the same feed.\nYou can think of feeds as group of actions in your data pipeline, typically processing a data type through multiple layers.\nYou can group actions together into the same feed if you want to execute them together.\nWe will come back to the concept of feeds as our pipeline gets more complex.')),(0,r.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"Metadata is not just used to select the right feeds.\nMetadata can also help a lot in documenting your data pipelines and making its data lineage understandable and discoverable. "))),(0,r.kt)("h2",{id:"try-it-out"},"Try it out"),(0,r.kt)("p",null,"Let's execute our action. We now come back to a similar ",(0,r.kt)("em",{parentName:"p"},"docker run")," command as in the ",(0,r.kt)("a",{parentName:"p",href:"/docs/getting-started/setup"},"setup step")," of our guide.\nThe only difference is that we mount 2 volumes instead of one and specify the path to your config file.\nBefore, we only mounted the data folder so that you could see the results of the execution on your machine.\nThe config file that was being used was located inside the docker image.\nThis time, we add another volume with your config-file and tell SDL to use it with the ",(0,r.kt)("em",{parentName:"p"},"--config")," option."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/config:/mnt/config smart-data-lake/gs1:latest --config /mnt/config --feed-sel download\n")),(0,r.kt)("p",null,"After executing it, you will see the file ",(0,r.kt)("em",{parentName:"p"},"data/stg_departures/result.json")," has been replaced with the output of your pipeline."),(0,r.kt)("div",{className:"admonition admonition-caution alert alert--warning"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"16",height:"16",viewBox:"0 0 16 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M8.893 1.5c-.183-.31-.52-.5-.887-.5s-.703.19-.886.5L.138 13.499a.98.98 0 0 0 0 1.001c.193.31.53.501.886.501h13.964c.367 0 .704-.19.877-.5a1.03 1.03 0 0 0 .01-1.002L8.893 1.5zm.133 11.497H6.987v-2.003h2.039v2.003zm0-3.004H6.987V5.987h2.039v4.006z"}))),"caution")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"Since both web servers are freely available on the internet, they might be overloaded by traffic.\nIf the download fails because of a timeout, either increase ",(0,r.kt)("em",{parentName:"p"},"readTimeoutMs")," or wait a couple of minutes and try again.\nIf the download still won't work (or if you just get empty files), you can copy the contents of the folder ",(0,r.kt)("em",{parentName:"p"},"data-fallback-download"),"\ninto your data folder. This will allow you to execute all steps starting from ",(0,r.kt)("a",{parentName:"p",href:"/docs/getting-started/part-1/select-columns"},"Select Columns")))),(0,r.kt)("p",null,"In case you run into issues when executing your pipeline and you want to terminate the process\nyou can use this docker command to list the running containers:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"docker ps\n")),(0,r.kt)("p",null,"While your feed-execution is running, the output of this command will contain\nan execution with the image name ",(0,r.kt)("em",{parentName:"p"},"smart-data-lake/gs1:latest"),".\nUse the container id to stop the container by typing:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"docker containter stop <container id>\n")),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Congratulations!")," You just wrote your first configuration and executed your feed! Now let's get our second input data source..."))}u.isMDXComponent=!0}}]);