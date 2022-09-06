"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[3311],{3905:(e,t,a)=>{a.d(t,{Zo:()=>d,kt:()=>m});var n=a(7294);function o(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function r(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?r(Object(a),!0).forEach((function(t){o(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):r(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,o=function(e,t){if(null==e)return{};var a,n,o={},r=Object.keys(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||(o[a]=e[a]);return o}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(o[a]=e[a])}return o}var s=n.createContext({}),u=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},d=function(e){var t=u(e.components);return n.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},p=n.forwardRef((function(e,t){var a=e.components,o=e.mdxType,r=e.originalType,s=e.parentName,d=l(e,["components","mdxType","originalType","parentName"]),p=u(a),m=o,f=p["".concat(s,".").concat(m)]||p[m]||c[m]||r;return a?n.createElement(f,i(i({ref:t},d),{},{components:a})):n.createElement(f,i({ref:t},d))}));function m(e,t){var a=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var r=a.length,i=new Array(r);i[0]=p;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:o,i[1]=l;for(var u=2;u<r;u++)i[u]=a[u];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}p.displayName="MDXCreateElement"},5162:(e,t,a)=>{a.d(t,{Z:()=>i});var n=a(7294),o=a(6010);const r="tabItem_Ymn6";function i(e){let{children:t,hidden:a,className:i}=e;return n.createElement("div",{role:"tabpanel",className:(0,o.Z)(r,i),hidden:a},t)}},5488:(e,t,a)=>{a.d(t,{Z:()=>m});var n=a(7462),o=a(7294),r=a(6010),i=a(2389),l=a(7392),s=a(7094),u=a(2466);const d="tabList__CuJ",c="tabItem_LNqP";function p(e){var t;const{lazy:a,block:i,defaultValue:p,values:m,groupId:f,className:h}=e,g=o.Children.map(e.children,(e=>{if((0,o.isValidElement)(e)&&"value"in e.props)return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)})),y=m??g.map((e=>{let{props:{value:t,label:a,attributes:n}}=e;return{value:t,label:a,attributes:n}})),b=(0,l.l)(y,((e,t)=>e.value===t.value));if(b.length>0)throw new Error(`Docusaurus error: Duplicate values "${b.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`);const k=null===p?p:p??(null==(t=g.find((e=>e.props.default)))?void 0:t.props.value)??g[0].props.value;if(null!==k&&!y.some((e=>e.value===k)))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${k}" but none of its children has the corresponding value. Available values are: ${y.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);const{tabGroupChoices:w,setTabGroupChoices:v}=(0,s.U)(),[N,D]=(0,o.useState)(k),j=[],{blockElementScrollPositionUntilNextRender:O}=(0,u.o5)();if(null!=f){const e=w[f];null!=e&&e!==N&&y.some((t=>t.value===e))&&D(e)}const T=e=>{const t=e.currentTarget,a=j.indexOf(t),n=y[a].value;n!==N&&(O(t),D(n),null!=f&&v(f,String(n)))},x=e=>{var t;let a=null;switch(e.key){case"ArrowRight":{const t=j.indexOf(e.currentTarget)+1;a=j[t]??j[0];break}case"ArrowLeft":{const t=j.indexOf(e.currentTarget)-1;a=j[t]??j[j.length-1];break}}null==(t=a)||t.focus()};return o.createElement("div",{className:(0,r.Z)("tabs-container",d)},o.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,r.Z)("tabs",{"tabs--block":i},h)},y.map((e=>{let{value:t,label:a,attributes:i}=e;return o.createElement("li",(0,n.Z)({role:"tab",tabIndex:N===t?0:-1,"aria-selected":N===t,key:t,ref:e=>j.push(e),onKeyDown:x,onFocus:T,onClick:T},i,{className:(0,r.Z)("tabs__item",c,null==i?void 0:i.className,{"tabs__item--active":N===t})}),a??t)}))),a?(0,o.cloneElement)(g.filter((e=>e.props.value===N))[0],{className:"margin-top--md"}):o.createElement("div",{className:"margin-top--md"},g.map(((e,t)=>(0,o.cloneElement)(e,{key:t,hidden:e.props.value!==N})))))}function m(e){const t=(0,i.Z)();return o.createElement(p,(0,n.Z)({key:String(t)},e))}},8250:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>d,contentTitle:()=>s,default:()=>m,frontMatter:()=>l,metadata:()=>u,toc:()=>c});var n=a(7462),o=(a(7294),a(3905)),r=a(5488),i=a(5162);const l={title:"Get Departures"},s=void 0,u={unversionedId:"getting-started/part-1/get-departures",id:"getting-started/part-1/get-departures",title:"Get Departures",description:"Goal",source:"@site/docs/getting-started/part-1/get-departures.md",sourceDirName:"getting-started/part-1",slug:"/getting-started/part-1/get-departures",permalink:"/docs/getting-started/part-1/get-departures",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-1/get-departures.md",tags:[],version:"current",frontMatter:{title:"Get Departures"},sidebar:"docs",previous:{title:"Inputs",permalink:"/docs/getting-started/get-input-data"},next:{title:"Get Airports",permalink:"/docs/getting-started/part-1/get-airports"}},d={},c=[{value:"Goal",id:"goal",level:2},{value:"Config File",id:"config-file",level:2},{value:"Define departures objects",id:"define-departures-objects",level:2},{value:"Naming Conventions",id:"naming-conventions",level:4},{value:"Define download-ext-departures",id:"define-download-ext-departures",level:2},{value:"Try it out",id:"try-it-out",level:2}],p={toc:c};function m(e){let{components:t,...l}=e;return(0,o.kt)("wrapper",(0,n.Z)({},p,l,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"goal"},"Goal"),(0,o.kt)("p",null,"In this step, we will download plane departure data from the REST-Interface described in the previous step using Smart Data Lake Builder."),(0,o.kt)("admonition",{title:"Smart Data Lake = SDL",type:"info"},(0,o.kt)("p",{parentName:"admonition"},"Throughout this documentation, we will mostly refer to ",(0,o.kt)("em",{parentName:"p"},"SDL")," which is just short for ",(0,o.kt)("em",{parentName:"p"},"Smart Data Lake"),". Further, we use ",(0,o.kt)("strong",{parentName:"p"},"SDLB")," as abbreviation for Smart Data Lake Builder, the automation tool.")),(0,o.kt)("h2",{id:"config-file"},"Config File"),(0,o.kt)("p",null,"With Smart Data Lake Builder, you describe your data pipelines in a config file using the ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/lightbend/config/blob/master/HOCON.md"},"HOCON")," format.\nAll HOCON features are supported so you could also split your configuration into several files. But for this first part, let's just use one file."),(0,o.kt)("p",null,"The configuration is located in the downloaded code under config/application.conf.\nTo walk through part-1 of this tutorial, please reset the existing application.conf to the config file available ",(0,o.kt)("a",{target:"_blank",href:a(7846).Z},"here"),"."),(0,o.kt)("p",null,"A data pipeline is composed of at least two entities: ",(0,o.kt)("em",{parentName:"p"},"DataObjects")," and ",(0,o.kt)("em",{parentName:"p"},"Actions"),"."),(0,o.kt)("p",null,"An action defines how one (or multiple) DataObject are copied or transformed into another (or multiple) DataObject.\nIn every data pipeline, you will have at least one ",(0,o.kt)("em",{parentName:"p"},"DataObject")," for your input and one for your output.\nIf you have more than one action, you will also have at least one ",(0,o.kt)("em",{parentName:"p"},"DataObject")," for each intermediary step between two actions."),(0,o.kt)("p",null,"In our case, in order to get our departure data, we are going to build one action. Hence, we need one DataObject for our input, and one for our output.\nCreate a directory called config in your current working directory and an empty file called application.conf. This is where we will define our data pipeline."),(0,o.kt)("h2",{id:"define-departures-objects"},"Define departures objects"),(0,o.kt)("p",null,"Add the following lines to your configuration file:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'dataObjects {\n  ext-departures {\n    type = WebserviceFileDataObject\n    url = "https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1630200800&end=1630310979"\n    readTimeoutMs=200000\n  }\n  stg-departures {\n    type = JsonFileDataObject\n    path = "~{id}"\n  }\n}\n')),(0,o.kt)("p",null,"Here, we first created the DataObjects section. This section will contain our DataObjects of our pipeline.\nInside, we defined two DataObjects to start with."),(0,o.kt)("admonition",{type:"info"},(0,o.kt)("p",{parentName:"admonition"},"The names ",(0,o.kt)("em",{parentName:"p"},"ext-departures")," and ",(0,o.kt)("em",{parentName:"p"},"stg-departures")," are called DataObject-ID.\nThey uniquely define the data object and we will frequently refer to these IDs as does SDL, i.e. in error messages.."),(0,o.kt)("p",{parentName:"admonition"},"You will see further down, that actions also have a name that uniquely identifies them, they are called Action-ID.")),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"ext-departures:",(0,o.kt)("br",{parentName:"p"}),"\n","This data object acts as a source in our action and defines where we get our departure information from.\nWe set its type to WebserviceFileDataObject to tell SDL that this is a webservice call returning a file.\nSDL comes with a broad set of predefined data object types and can easily be extended. More on that later.\nEach type of data object comes with its own set of parameters. For a WebserviceFileDataObject, the only mandatory one is the url, so we set that as well.\nWe also set the option readTimeoutMs to a couple of seconds because the Rest-Service can be slow to respond.")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("p",{parentName:"li"},"stg-departures:",(0,o.kt)("br",{parentName:"p"}),"\n","This data object acts as a target for our first action, so where and how to download the file to.\nWe set type to JsonFileDataObject because we know from before that the webservice will return a json file.\nPath defines where the file will be stored. You could choose any name you want, but most of the time, the name of your DataObject is a good fit.\nInstead of writing ",(0,o.kt)("em",{parentName:"p"},"stg-departures")," again,\nwe used the placeholder ",(0,o.kt)("em",{parentName:"p"},"{~id}")," which gets replaced by the DataObject-ID. Don't forget to surround that placeholder\nwith double quotes so that it is interpreted as a string.\nWe defined a relative path - it is relative to the working directory SDL is started in.\nThe working directory has been set to the ",(0,o.kt)("em",{parentName:"p"},"data")," directory in the Dockerfile by setting the JVM Property "),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre"},"-Duser.dir=/mnt/data\n")),(0,o.kt)("p",{parentName:"li"},"so that's why all your relative paths will start in the ",(0,o.kt)("em",{parentName:"p"},"data")," directory."))),(0,o.kt)("h4",{id:"naming-conventions"},"Naming Conventions"),(0,o.kt)("p",null,"A quick note on our naming conventions: We typically follow some conventions when naming our data objects and actions.\nThey follow the layering conventions of our structured Smart Data Lake:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},'External data objects are prefixed with "ext"'),(0,o.kt)("li",{parentName:"ul"},"Your first action typically copies the data into the Data Lake, without making any changes.\nThis layer is called the ",(0,o.kt)("em",{parentName:"li"},"Staging Layer"),'.\nDataObjects of the staging layer are prefixed with "stg".'),(0,o.kt)("li",{parentName:"ul"},"When applying some basic transformation to your data that does not require any specific business logic, you store the result in the ",(0,o.kt)("em",{parentName:"li"},"Integration Layer"),'.\nSome of these transformations are data deduplication, historization and format standardization.\nDataObjects of the Integration Layer are prefixed with "int".'),(0,o.kt)("li",{parentName:"ul"},"When applying business logic to your data, you store the result in the ",(0,o.kt)("em",{parentName:"li"},"Business Tranformation Layer")," or ",(0,o.kt)("em",{parentName:"li"},"BTL"),' for short.\nDataObjects of the Business Transformation Layer are prefixed with "btl".')),(0,o.kt)("p",null,"You are of course free to use any other naming conventions, but it's worth to think about one at the beginning of your project."),(0,o.kt)("p",null,"In our case, we simply copy data exactly as is from an external source. Hence, our output DataObject belongs to the Staging Layer."),(0,o.kt)("h2",{id:"define-download-ext-departures"},"Define download-ext-departures"),(0,o.kt)("p",null,"After the dataObjects section, add the following lines to your configuration file:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"actions {\n    download-departures {\n      type = FileTransferAction\n      inputId = ext-departures\n      outputId = stg-departures\n      metadata {\n        feed = download\n      }\n    }\n}\n")),(0,o.kt)("p",null,"We added another section called actions, in which, you guessed it, all actions reside.\nWe defined our action and called it ",(0,o.kt)("em",{parentName:"p"},"download-departures"),"."),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"The type ",(0,o.kt)("em",{parentName:"li"},"FileTransferAction")," tells SDL that it should transfer a file from one place to another without any transformation.\nIn our case, from a location on the web to a place on your machine."),(0,o.kt)("li",{parentName:"ul"},"With inputId and outputId, we wire this action and the two data objects together."),(0,o.kt)("li",{parentName:"ul"},'Finally, we added some metadata to our action. Metadata is used to select the right actions to run.\nIn our case, we defined a feed called "download". When starting SDL, we can tell it to execute only actions corresponding to certain feeds.\nMultiple actions can be associated with the same feed.\nYou can think of feeds as group of actions in your data pipeline, typically processing a data type through multiple layers.\nYou can group actions together into the same feed if you want to execute them together.\nWe will come back to the concept of feeds as our pipeline gets more complex.')),(0,o.kt)("admonition",{type:"info"},(0,o.kt)("p",{parentName:"admonition"},"Metadata is not just used to select the right feeds.\nMetadata can also help a lot in documenting your data pipelines and making its data lineage understandable and discoverable. ")),(0,o.kt)("h2",{id:"try-it-out"},"Try it out"),(0,o.kt)("p",null,"Let's execute our action. We now come back to a similar ",(0,o.kt)("em",{parentName:"p"},"docker run")," command as in the ",(0,o.kt)("a",{parentName:"p",href:"/docs/getting-started/setup"},"setup step")," of our guide.\nThe only difference is that we mount 2 volumes instead of one and specify the path to your config file.\nBefore, we only mounted the data folder so that you could see the results of the execution on your machine.\nThe config file that was being used was located inside the docker image.\nThis time, we add another volume with your config-file and tell SDL to use it with the ",(0,o.kt)("em",{parentName:"p"},"--config")," option."),(0,o.kt)(r.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,o.kt)(i.Z,{value:"docker",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\n"))),(0,o.kt)(i.Z,{value:"podman",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\n")))),(0,o.kt)("p",null,"After executing it, you will see the file ",(0,o.kt)("em",{parentName:"p"},"data/stg_departures/result.json")," has been replaced with the output of your pipeline."),(0,o.kt)("admonition",{type:"caution"},(0,o.kt)("p",{parentName:"admonition"},"Since both web servers are freely available on the internet, they might be overloaded by traffic.\nIf the download fails because of a timeout, wait a couple of minutes and try again.\nIf the download still won't work (or if you just get empty files), you can copy the contents of the folder ",(0,o.kt)("em",{parentName:"p"},"data-fallback-download"),"\ninto your data folder. This will allow you to execute all steps starting from ",(0,o.kt)("a",{parentName:"p",href:"/docs/getting-started/part-1/select-columns"},"Select Columns"))),(0,o.kt)("p",null,(0,o.kt)("strong",{parentName:"p"},"Congratulations!")," You just wrote your first configuration and executed your feed! Now let's get our second input data source..."))}m.isMDXComponent=!0},7846:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/application-part1-start-d214cf63cf314cb13ab8faba445ef1d8.conf"}}]);