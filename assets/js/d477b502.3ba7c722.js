"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2852],{9171:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>i,default:()=>u,frontMatter:()=>o,metadata:()=>s,toc:()=>d});var a=n(4848),r=n(8453);n(1470),n(9365);const o={title:"Get Departures"},i=void 0,s={id:"getting-started/part-1/get-departures",title:"Get Departures",description:"Goal",source:"@site/docs/getting-started/part-1/get-departures.md",sourceDirName:"getting-started/part-1",slug:"/getting-started/part-1/get-departures",permalink:"/docs/getting-started/part-1/get-departures",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-1/get-departures.md",tags:[],version:"current",frontMatter:{title:"Get Departures"},sidebar:"tutorialSidebar",previous:{title:"Inputs",permalink:"/docs/getting-started/get-input-data"},next:{title:"Get Airports",permalink:"/docs/getting-started/part-1/get-airports"}},l={},d=[{value:"Goal",id:"goal",level:2},{value:"Config File",id:"config-file",level:2},{value:"Define departures objects",id:"define-departures-objects",level:2},{value:"Naming Conventions",id:"naming-conventions",level:4},{value:"Define download-ext-departures",id:"define-download-ext-departures",level:2},{value:"Try it out",id:"try-it-out",level:2}];function c(e){const t={a:"a",admonition:"admonition",br:"br",code:"code",em:"em",h2:"h2",h4:"h4",li:"li",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,r.R)(),...e.components};return(0,a.jsxs)(a.Fragment,{children:[(0,a.jsx)(t.h2,{id:"goal",children:"Goal"}),"\n",(0,a.jsx)(t.p,{children:"In this step, we will download plane departure data from the REST-Interface described in the previous step using Smart Data Lake Builder."}),"\n",(0,a.jsx)(t.admonition,{title:"Smart Data Lake Builder = SDLB",type:"info",children:(0,a.jsxs)(t.p,{children:["Throughout this documentation, we will use ",(0,a.jsx)(t.strong,{children:"SDLB"})," as abbreviation for Smart Data Lake Builder, an automation tool for building smarter Data Lakes, Lakehouses, Data Migrations and more..."]})}),"\n",(0,a.jsx)(t.h2,{id:"config-file",children:"Config File"}),"\n",(0,a.jsxs)(t.p,{children:["With Smart Data Lake Builder, you describe your data pipelines in configuration files using the ",(0,a.jsx)(t.a,{href:"https://github.com/lightbend/config/blob/master/HOCON.md",children:"HOCON"})," format.\nAll HOCON features are supported, as SDLB uses the original HOCON parser from lightbend."]}),"\n",(0,a.jsx)(t.p,{children:"The configuration is located in the downloaded code under the config folder. Configuration can be split across configuration files as you like.\nFor this tutorial we will use departures.conf, airports.conf, btl.conf and a global.conf file.\nTo walk through part-1 of this tutorial, please reset the existing departures/airports/btl.conf with the following cmd:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"pushd config && cp config.template departures.conf && cp config.template airports.conf && cp config.template btl.conf && popd\n"})}),"\n",(0,a.jsxs)(t.p,{children:["A data pipeline is composed of at least two entities: ",(0,a.jsx)(t.em,{children:"DataObjects"})," and ",(0,a.jsx)(t.em,{children:"Actions"}),"."]}),"\n",(0,a.jsxs)(t.p,{children:["An action defines how one (or multiple) DataObject are copied or transformed into another (or multiple) DataObject.\nIn every data pipeline, you will have at least one ",(0,a.jsx)(t.em,{children:"DataObject"})," for your input and one for your output.\nIf you have more than one action, you will also have at least one ",(0,a.jsx)(t.em,{children:"DataObject"})," for each intermediary step between two actions."]}),"\n",(0,a.jsx)(t.p,{children:"In our case, in order to get our departure data, we are going to build one action. Hence, we need one DataObject for our input, and one for our output.\nCreate a directory called config in your current working directory and an empty file called application.conf. This is where we will define our data pipeline."}),"\n",(0,a.jsx)(t.h2,{id:"define-departures-objects",children:"Define departures objects"}),"\n",(0,a.jsx)(t.p,{children:"Add the following lines to your departures.conf file:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:'    dataObjects {\n    \n      ext-departures {\n        type = WebserviceFileDataObject\n        url = "https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1696854853&end=1697027653"\n        timeouts {\n          connectionTimeoutMs = 3000\n          readTimeoutMs = 200000\n        }\n      }\n      \n      stg-departures {\n        type = JsonFileDataObject\n        path = "~{id}"\n      }\n      \n    }\n'})}),"\n",(0,a.jsx)(t.admonition,{type:"caution",children:(0,a.jsxs)(t.p,{children:["Note that the API Call ",(0,a.jsx)(t.strong,{children:"may freeze"})," as the timestamps provided under ",(0,a.jsx)(t.strong,{children:"begin=1696854853&end=1697027653"})," get older. When that's the case, simply replace them with more recent timestamps.\nYou can go on ",(0,a.jsx)(t.a,{href:"https://www.epochconverter.com/",children:"https://www.epochconverter.com/"}),' and set "end" to the current time in seconds and "begin" to the current time in seconds minus 2 days.\nAt this stage in the guide, the capabilities of our dataObject  ext-departures are somewhat limited as you need to provide to it the exact url of the data you want to download.\nIn part 3 of this guide we will make our DataObject much smarter and these steps won\'t be needed anymore.']})}),"\n",(0,a.jsx)(t.p,{children:"Here, we first created the DataObjects section. This section will contain our DataObjects of our pipeline.\nInside, we defined two DataObjects to start with."}),"\n",(0,a.jsxs)(t.admonition,{type:"info",children:[(0,a.jsxs)(t.p,{children:["The names ",(0,a.jsx)(t.em,{children:"ext-departures"})," and ",(0,a.jsx)(t.em,{children:"stg-departures"})," are called DataObject-ID.\nThey uniquely define the data object, and we will frequently refer to these IDs as does SDLB, i.e. in error messages."]}),(0,a.jsx)(t.p,{children:"You will see further down, that actions also have a name that uniquely identifies them, they are called Action-ID."})]}),"\n",(0,a.jsxs)(t.ul,{children:["\n",(0,a.jsxs)(t.li,{children:["\n",(0,a.jsxs)(t.p,{children:["ext-departures:",(0,a.jsx)(t.br,{}),"\n","This data object acts as a source in our action and defines where we get our departure information from.\nWe set its type to WebserviceFileDataObject to tell SDLB that this is a webservice call returning a file.\nSDLB comes with a broad set of predefined data object types and can easily be extended. More on that later.\nEach type of data object comes with its own set of parameters. For a WebserviceFileDataObject, the only mandatory one is the url, so we set that as well.\nWe also set the option readTimeoutMs (and connectionTimeoutMs) to a couple of seconds because the Rest-Service can be slow to respond."]}),"\n"]}),"\n",(0,a.jsxs)(t.li,{children:["\n",(0,a.jsxs)(t.p,{children:["stg-departures:",(0,a.jsx)(t.br,{}),"\n","This data object acts as a target for our first action, so where and how to download the file to.\nWe set type to JsonFileDataObject because we know from before that the webservice will return a json file.\nPath defines where the file will be stored. You could choose any name you want, but most of the time, the name of your DataObject is a good fit.\nInstead of writing ",(0,a.jsx)(t.em,{children:"stg-departures"})," again,\nwe used the placeholder ",(0,a.jsx)(t.em,{children:"~{id}"})," which gets replaced by the DataObject-ID. Don't forget to surround that placeholder\nwith double quotes so that it is interpreted as a string.\nWe defined a relative path - it is relative to the working directory SDLB is started in.\nThe working directory has been set to the ",(0,a.jsx)(t.em,{children:"data"})," directory in the Dockerfile by setting the JVM Property"]}),"\n"]}),"\n"]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"-Duser.dir=/mnt/data\n"})}),"\n",(0,a.jsxs)(t.p,{children:["so that's why all your relative paths will start in the ",(0,a.jsx)(t.em,{children:"data"})," directory."]}),"\n",(0,a.jsx)(t.h4,{id:"naming-conventions",children:"Naming Conventions"}),"\n",(0,a.jsx)(t.p,{children:"A quick note on our naming conventions: We typically follow some conventions when naming our data objects and actions.\nThey follow the layering conventions of our structured Smart Data Lake:"}),"\n",(0,a.jsxs)(t.ul,{children:["\n",(0,a.jsx)(t.li,{children:'External data objects are prefixed with "ext"'}),"\n",(0,a.jsxs)(t.li,{children:["Your first action typically copies the data into the Data Lake, without making any changes.\nThis layer is called the ",(0,a.jsx)(t.em,{children:"Staging Layer"}),'.\nDataObjects of the staging layer are prefixed with "stg".']}),"\n",(0,a.jsxs)(t.li,{children:["When applying some basic transformation to your data that does not require any specific business logic, you store the result in the ",(0,a.jsx)(t.em,{children:"Integration Layer"}),'.\nSome of these transformations are data deduplication, historization and format standardization.\nDataObjects of the Integration Layer are prefixed with "int".']}),"\n",(0,a.jsxs)(t.li,{children:["When applying business logic to your data, you store the result in the ",(0,a.jsx)(t.em,{children:"Business Transformation Layer"})," or ",(0,a.jsx)(t.em,{children:"BTL"}),' for short.\nDataObjects of the Business Transformation Layer are prefixed with "btl".']}),"\n"]}),"\n",(0,a.jsx)(t.p,{children:"You are of course free to use any other naming conventions, but it's worth to think about one at the beginning of your project."}),"\n",(0,a.jsx)(t.p,{children:"In our case, we simply copy data exactly as is from an external source. Hence, our output DataObject belongs to the Staging Layer."}),"\n",(0,a.jsx)(t.h2,{id:"define-download-ext-departures",children:"Define download-ext-departures"}),"\n",(0,a.jsxs)(t.p,{children:["After the ",(0,a.jsx)(t.code,{children:"dataObjects"})," section, add the following lines to your configuration file:"]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"    actions {\n    \n        download-departures {\n          type = FileTransferAction\n          inputId = ext-departures\n          outputId = stg-departures\n          metadata {\n            feed = download\n          }\n        }\n        \n    }\n"})}),"\n",(0,a.jsxs)(t.p,{children:["We added another section called actions, in which, you guessed it, all actions reside.\nWe defined our action and called it ",(0,a.jsx)(t.em,{children:"download-departures"}),"."]}),"\n",(0,a.jsxs)(t.ul,{children:["\n",(0,a.jsxs)(t.li,{children:["The type ",(0,a.jsx)(t.em,{children:"FileTransferAction"})," tells SDLB that it should transfer a file from one place to another without any transformation.\nIn our case, from a location on the web to a place on your machine."]}),"\n",(0,a.jsx)(t.li,{children:"With inputId and outputId, we wire this action and the two data objects together."}),"\n",(0,a.jsx)(t.li,{children:'Finally, we added some metadata to our action. Metadata is used to select the right actions to run.\nIn our case, we defined a feed called "download". When starting SDLB, we can tell it to execute only actions corresponding to certain feeds.\nMultiple actions can be associated with the same feed.\nYou can think of feeds as group of actions in your data pipeline, typically processing a data type through multiple layers.\nYou can group actions together into the same feed if you want to execute them together.\nWe will come back to the concept of feeds as our pipeline gets more complex.'}),"\n"]}),"\n",(0,a.jsx)(t.admonition,{type:"info",children:(0,a.jsx)(t.p,{children:"Metadata is not just used to select the right feeds.\nMetadata can also help a lot in documenting your data pipelines and making its data lineage understandable and discoverable."})}),"\n",(0,a.jsx)(t.h2,{id:"try-it-out",children:"Try it out"}),"\n",(0,a.jsxs)(t.p,{children:["Let's execute our action. We now come back to a similar ",(0,a.jsx)(t.code,{children:"./startJob.sh"})," command as in the ",(0,a.jsx)(t.a,{href:"/docs/getting-started/setup",children:"setup step"})," of our guide.\nThe only difference is that we mount 2 volumes instead of one and specify the path to your config file.\nBefore, we only mounted the data folder so that you could see the results of the execution on your machine.\nThe config file that was being used was located inside the docker image.\nThis time, we add another volume with your config-file and tell SDLB to use it with the ",(0,a.jsx)(t.em,{children:"--config"})," option."]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"./startJob.sh --config /mnt/config --feed-sel download\n"})}),"\n",(0,a.jsxs)(t.p,{children:["After executing it, you will see the file ",(0,a.jsx)(t.em,{children:"data/stg_departures/result.json"})," has been replaced with the output of your pipeline."]}),"\n",(0,a.jsx)(t.admonition,{type:"caution",children:(0,a.jsxs)(t.p,{children:["Since both web servers are freely available on the internet, ",(0,a.jsx)(t.strong,{children:"a rate limiting applies"}),". ",(0,a.jsx)(t.a,{href:"https://opensky-network.org/",children:"https://opensky-network.org/"})," will stop responding if you make too many calls.\nIf the download fails because of a timeout, wait a couple of minutes and try again. In the worst case, it should work again on the next day.\nIf the download still won't work (or if you just get empty files), you can copy the contents of the folder ",(0,a.jsx)(t.em,{children:"data/stg-departures-fallback"}),"\ninto your data folder. This will allow you to execute all steps starting from ",(0,a.jsx)(t.a,{href:"/docs/getting-started/part-1/select-columns",children:"Select Columns"})]})}),"\n",(0,a.jsxs)(t.p,{children:[(0,a.jsx)(t.strong,{children:"Congratulations!"})," You just wrote your first configuration and executed your feed! Now let's get our second input data source..."]})]})}function u(e={}){const{wrapper:t}={...(0,r.R)(),...e.components};return t?(0,a.jsx)(t,{...e,children:(0,a.jsx)(c,{...e})}):c(e)}},9365:(e,t,n)=>{n.d(t,{A:()=>i});n(6540);var a=n(53);const r={tabItem:"tabItem_Ymn6"};var o=n(4848);function i(e){let{children:t,hidden:n,className:i}=e;return(0,o.jsx)("div",{role:"tabpanel",className:(0,a.A)(r.tabItem,i),hidden:n,children:t})}},1470:(e,t,n)=>{n.d(t,{A:()=>v});var a=n(6540),r=n(53),o=n(3104),i=n(6347),s=n(205),l=n(7485),d=n(1682),c=n(9466);function u(e){return a.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,a.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)}))?.filter(Boolean)??[]}function h(e){const{values:t,children:n}=e;return(0,a.useMemo)((()=>{const e=t??function(e){return u(e).map((e=>{let{props:{value:t,label:n,attributes:a,default:r}}=e;return{value:t,label:n,attributes:a,default:r}}))}(n);return function(e){const t=(0,d.X)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,n])}function p(e){let{value:t,tabValues:n}=e;return n.some((e=>e.value===t))}function f(e){let{queryString:t=!1,groupId:n}=e;const r=(0,i.W6)(),o=function(e){let{queryString:t=!1,groupId:n}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!n)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return n??null}({queryString:t,groupId:n});return[(0,l.aZ)(o),(0,a.useCallback)((e=>{if(!o)return;const t=new URLSearchParams(r.location.search);t.set(o,e),r.replace({...r.location,search:t.toString()})}),[o,r])]}function m(e){const{defaultValue:t,queryString:n=!1,groupId:r}=e,o=h(e),[i,l]=(0,a.useState)((()=>function(e){let{defaultValue:t,tabValues:n}=e;if(0===n.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:n}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${n.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const a=n.find((e=>e.default))??n[0];if(!a)throw new Error("Unexpected error: 0 tabValues");return a.value}({defaultValue:t,tabValues:o}))),[d,u]=f({queryString:n,groupId:r}),[m,g]=function(e){let{groupId:t}=e;const n=function(e){return e?`docusaurus.tab.${e}`:null}(t),[r,o]=(0,c.Dv)(n);return[r,(0,a.useCallback)((e=>{n&&o.set(e)}),[n,o])]}({groupId:r}),b=(()=>{const e=d??m;return p({value:e,tabValues:o})?e:null})();(0,s.A)((()=>{b&&l(b)}),[b]);return{selectedValue:i,selectValue:(0,a.useCallback)((e=>{if(!p({value:e,tabValues:o}))throw new Error(`Can't select invalid tab value=${e}`);l(e),u(e),g(e)}),[u,g,o]),tabValues:o}}var g=n(2303);const b={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"};var w=n(4848);function y(e){let{className:t,block:n,selectedValue:a,selectValue:i,tabValues:s}=e;const l=[],{blockElementScrollPositionUntilNextRender:d}=(0,o.a_)(),c=e=>{const t=e.currentTarget,n=l.indexOf(t),r=s[n].value;r!==a&&(d(t),i(r))},u=e=>{let t=null;switch(e.key){case"Enter":c(e);break;case"ArrowRight":{const n=l.indexOf(e.currentTarget)+1;t=l[n]??l[0];break}case"ArrowLeft":{const n=l.indexOf(e.currentTarget)-1;t=l[n]??l[l.length-1];break}}t?.focus()};return(0,w.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,r.A)("tabs",{"tabs--block":n},t),children:s.map((e=>{let{value:t,label:n,attributes:o}=e;return(0,w.jsx)("li",{role:"tab",tabIndex:a===t?0:-1,"aria-selected":a===t,ref:e=>l.push(e),onKeyDown:u,onClick:c,...o,className:(0,r.A)("tabs__item",b.tabItem,o?.className,{"tabs__item--active":a===t}),children:n??t},t)}))})}function j(e){let{lazy:t,children:n,selectedValue:r}=e;const o=(Array.isArray(n)?n:[n]).filter(Boolean);if(t){const e=o.find((e=>e.props.value===r));return e?(0,a.cloneElement)(e,{className:"margin-top--md"}):null}return(0,w.jsx)("div",{className:"margin-top--md",children:o.map(((e,t)=>(0,a.cloneElement)(e,{key:t,hidden:e.props.value!==r})))})}function x(e){const t=m(e);return(0,w.jsxs)("div",{className:(0,r.A)("tabs-container",b.tabList),children:[(0,w.jsx)(y,{...e,...t}),(0,w.jsx)(j,{...e,...t})]})}function v(e){const t=(0,g.A)();return(0,w.jsx)(x,{...e,children:u(e.children)},String(t))}},8453:(e,t,n)=>{n.d(t,{R:()=>i,x:()=>s});var a=n(6540);const r={},o=a.createContext(r);function i(e){const t=a.useContext(o);return a.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function s(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:i(e.components),a.createElement(o.Provider,{value:t},e.children)}}}]);