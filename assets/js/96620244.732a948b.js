"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[7160],{7223:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>s,default:()=>u,frontMatter:()=>o,metadata:()=>i,toc:()=>c});var a=n(4848),r=n(8453);n(1470),n(9365);const o={title:"Select Columns"},s=void 0,i={id:"getting-started/part-1/select-columns",title:"Select Columns",description:"Goal",source:"@site/docs/getting-started/part-1/select-columns.md",sourceDirName:"getting-started/part-1",slug:"/getting-started/part-1/select-columns",permalink:"/docs/getting-started/part-1/select-columns",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-1/select-columns.md",tags:[],version:"current",frontMatter:{title:"Select Columns"},sidebar:"tutorialSidebar",previous:{title:"Get Airports",permalink:"/docs/getting-started/part-1/get-airports"},next:{title:"Joining It Together",permalink:"/docs/getting-started/part-1/joining-it-together"}},l={},c=[{value:"Goal",id:"goal",level:2},{value:"Define output object",id:"define-output-object",level:2},{value:"Define select-airport-cols action",id:"define-select-airport-cols-action",level:2},{value:"Try it out",id:"try-it-out",level:2},{value:"More on Feeds",id:"more-on-feeds",level:2},{value:"Example of Common Mistake",id:"example-of-common-mistake",level:2}];function d(e){const t={a:"a",admonition:"admonition",code:"code",em:"em",h2:"h2",li:"li",p:"p",pre:"pre",ul:"ul",...(0,r.R)(),...e.components};return(0,a.jsxs)(a.Fragment,{children:[(0,a.jsx)(t.h2,{id:"goal",children:"Goal"}),"\n",(0,a.jsxs)(t.p,{children:["In this step we write our first Action that modifies data.\nWe will continue based upon the airports.conf file available ",(0,a.jsx)(t.a,{href:"https://github.com/smart-data-lake/getting-started/tree/master/config/airports.conf.part-1a-solution",children:"here"}),".\nWhen you look at the data in the folder ",(0,a.jsx)(t.em,{children:"data/stg-airports/result.csv"}),", you will notice that we\ndon't need most of the columns. In this step, we will write a simple ",(0,a.jsx)(t.em,{children:"CopyAction"})," that selects only the columns we\nare interested in."]}),"\n",(0,a.jsxs)(t.p,{children:["As usual, we need to define an output DataObject and an action.\nWe don't need to define a new input DataObject as we will wire our new action to the existing DataObject ",(0,a.jsx)(t.em,{children:"stg-airports"}),"."]}),"\n",(0,a.jsx)(t.h2,{id:"define-output-object",children:"Define output object"}),"\n",(0,a.jsxs)(t.p,{children:["Let's use CsvFileDataObject again because that makes it easy for us to check the result.\nIn more advanced (speak: real-life) scenarios, we would use one of numerous other possibilities,\nsuch as HiveTableDataObject, SplunkDataObject...\nSee ",(0,a.jsx)(t.a,{href:"https://github.com/smart-data-lake/smart-data-lake/blob/develop-spark3/docs/Reference.md#data-objects",children:"this list"})," for an overview.\nYou can also consult the ",(0,a.jsx)(t.a,{href:"https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=2",children:"Configuration Schema Browser"})," to get a list of all Data Objects and related properties."]}),"\n",(0,a.jsxs)(t.p,{children:["In a first step, we want to make the airport data more understandable by removing any columns we don't need.\nSince we don't introduce any business logic into the transformation,\nthe resulting data object will reside in the integration layer and thus will be called ",(0,a.jsx)(t.em,{children:"int-airports"}),".\nPut this in the existing ",(0,a.jsx)(t.code,{children:"dataObjects"})," section of airports.conf:"]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:'  int-airports {\n    type = CsvFileDataObject\n    path = "~{id}"\n  }\n'})}),"\n",(0,a.jsx)(t.h2,{id:"define-select-airport-cols-action",children:"Define select-airport-cols action"}),"\n",(0,a.jsx)(t.p,{children:"Next, add these lines in the actions section of airports.conf:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:'  select-airport-cols {\n    type = CopyAction\n    inputId = stg-airports\n    outputId = int-airports\n    transformers = [{\n        type = SQLDfTransformer\n        code = "select ident, name, latitude_deg, longitude_deg from stg_airports"\n    }]\n    metadata {\n      feed = compute\n    }\n  }\n'})}),"\n",(0,a.jsx)(t.p,{children:"A couple of things to note here:"}),"\n",(0,a.jsxs)(t.ul,{children:["\n",(0,a.jsxs)(t.li,{children:["We just defined a new action called ",(0,a.jsx)(t.em,{children:"select-airport-cols"}),"."]}),"\n",(0,a.jsxs)(t.li,{children:["We wired it together with the two DataObjects ",(0,a.jsx)(t.em,{children:"stg-airports"})," and ",(0,a.jsx)(t.em,{children:"int-airports"}),"."]}),"\n",(0,a.jsx)(t.li,{children:"A new type of Action was used: CopyAction. This action is intended to copy data from one data object to another\nwith some optional transformations of the data along the way."}),"\n",(0,a.jsx)(t.li,{children:"To define the transformations of an action, you define a list of HOCON Objects.\nHOCON-Objects are just like JSON-Objects (with a few added features, but more on that later)."}),"\n",(0,a.jsxs)(t.li,{children:["Instead of allowing for just one transformer, we could potentially have multiple transformers within the same action that\nget executed one after the other. That's why we have the bracket followed by the curly brace ",(0,a.jsx)(t.code,{children:"[{"})," :\nthe CustomDataFrameAction expects its field ",(0,a.jsx)(t.em,{children:"transformers"})," to be a list of HOCON Objects."]}),"\n",(0,a.jsxs)(t.li,{children:["There's different kinds of transformers, in this case we defined a ",(0,a.jsx)(t.em,{children:"SQLDfTransformer"})," and provided it with a custom SQL-Code.\nThere are other transformer types such as ",(0,a.jsx)(t.em,{children:"ScalaCodeSparkDfTransformer"}),", ",(0,a.jsx)(t.em,{children:"PythonCodeDfTransformer"}),"... More on that later."]}),"\n"]}),"\n",(0,a.jsx)(t.admonition,{type:"caution",children:(0,a.jsxs)(t.p,{children:['Notice that we call our input DataObject stg-airports with a hyphen "-", but in the sql, we call it "stg_airports" with an underscore "_".\nThis is due to the SQL standard not allowing "-" in unquoted identifiers (e.g. table names).\nUnder the hood, Apache Spark SQL is used to execute the query, which implements SQL standard.\nSDLB works around this by replacing special chars in DataObject names used in SQL statements for you.\nIn this case, it automatically replaced ',(0,a.jsx)(t.code,{children:"-"})," with ",(0,a.jsx)(t.code,{children:"_"}),".\nAlternatively you can use the token ",(0,a.jsx)(t.code,{children:"%{inputViewName}"})," instead of the table name, and SDLB will replace it with the correct name at runtime."]})}),"\n",(0,a.jsxs)(t.p,{children:["There are numerous other options available for the CopyAction, which you can view in the ",(0,a.jsx)(t.a,{href:"https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=3-0",children:"Configuration Schema Browser"}),"."]}),"\n",(0,a.jsx)(t.h2,{id:"try-it-out",children:"Try it out"}),"\n",(0,a.jsxs)(t.p,{children:["Note that we used a different feed this time, we called it ",(0,a.jsx)(t.em,{children:"compute"}),".\nWe will keep expanding the feed ",(0,a.jsx)(t.em,{children:"compute"})," in the next few steps.\nThis allows us to keep the data we downloaded in the previous steps in our local files and just\ntry out our new actions."]}),"\n",(0,a.jsx)(t.p,{children:"To execute the pipeline, use the same command as before, but change the feed to compute:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"./startJob.sh --config /mnt/config --feed-sel compute\n"})}),"\n",(0,a.jsxs)(t.p,{children:["Now you should see multiple files in the folder ",(0,a.jsx)(t.em,{children:"data/int-airports"}),". Why is it split across multiple files?\nThis is due to the fact that the query runs with Apache Spark under the hood which computes the query in parallel for different portions of the data.\nWe might work on a small data set for now, but keep in mind that this would scale up horizontally for large amounts of data."]}),"\n",(0,a.jsx)(t.h2,{id:"more-on-feeds",children:"More on Feeds"}),"\n",(0,a.jsx)(t.p,{children:"SDLB gives you precise control on which actions you want to execute.\nFor instance if you only want to execute the action that we just wrote, you can type"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"./startJob.sh --config /mnt/config --feed-sel ids:select-airport-cols\n"})}),"\n",(0,a.jsxs)(t.p,{children:["Of course, at this stage, the feed ",(0,a.jsx)(t.em,{children:"compute"})," only contains this one action, so the result will be the same."]}),"\n",(0,a.jsx)(t.p,{children:"SDLB also allows you to use combinations of expressions to select the actions you want to execute. You can run"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"./startJob.sh --help\n"})}),"\n",(0,a.jsx)(t.p,{children:"to see all options that are available. For your convenience, here is the current output of the help command:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"-f, --feed-sel <operation?><prefix:?><regex>[,<operation?><prefix:?><regex>...]\n                         Select actions to execute by one or multiple expressions separated by comma (,). Results from multiple expressions are combined from left to right.\n                         Operations:\n                         - pipe symbol (|): the two sets are combined by union operation (default)\n                         - ampersand symbol (&): the two sets are combined by intersection operation\n                         - minus symbol (-): the second set is subtracted from the first set\n                         Prefixes:\n                         - 'feeds': select actions where metadata.feed is matched by regex pattern (default)\n                         - 'names': select actions where metadata.name is matched by regex pattern\n                         - 'ids': select actions where id is matched by regex pattern\n                         - 'layers': select actions where metadata.layer of all output DataObjects is matched by regex pattern\n                         - 'startFromActionIds': select actions which with id is matched by regex pattern and any dependent action (=successors)\n                         - 'endWithActionIds': select actions which with id is matched by regex pattern and their predecessors\n                         - 'startFromDataObjectIds': select actions which have an input DataObject with id is matched by regex pattern and any dependent action (=successors)\n                         - 'endWithDataObjectIds': select actions which have an output DataObject with id is matched by regex pattern and their predecessors\n                         All matching is done case-insensitive.\n                         Example: to filter action 'A' and its successors but only in layer L1 and L2, use the following pattern: \"startFromActionIds:a,&layers:(l1|l2)\"\n-n, --name <value>       Optional name of the application. If not specified feed-sel is used.\n-c, --config <file1>[,<file2>...]\n                         One or multiple configuration files or directories containing configuration files, separated by comma. Entries must be valid Hadoop URIs or a special URI with scheme \"cp\" which is treated as classpath entry.\n--partition-values <partitionColName>=<partitionValue>[,<partitionValue>,...]\n                         Partition values to process for one single partition column.\n--multi-partition-values <partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>[;(<partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>;...]\n                         Partition values to process for multiple partitoin columns.\n-s, --streaming          Enable streaming mode for continuous processing.\n--parallelism <int>      Parallelism for DAG run.\n--state-path <path>      Path to save run state files. Must be set to enable recovery in case of failures.\n--override-jars <jar1>[,<jar2>...]\n                         Comma separated list of jar filenames for child-first class loader. The jars must be present in classpath.\n--test <config|dry-run>  Run in test mode: config -> validate configuration, dry-run -> execute prepare- and init-phase only to check environment and spark lineage\n--help                   Display the help text.\n--version                Display version information.\n-m, --master <value>     The Spark master URL passed to SparkContext (default=local[*], yarn, spark://HOST:PORT, mesos://HOST:PORT, k8s://HOST:PORT).\n-x, --deploy-mode <value>\n                         The Spark deploy mode passed to SparkContext (default=client, cluster).\n-d, --kerberos-domain <value>\n                         Kerberos-Domain for authentication (USERNAME@KERBEROS-DOMAIN) in local mode.\n-u, --username <value>   Kerberos username for authentication (USERNAME@KERBEROS-DOMAIN) in local mode.\n-k, --keytab-path <value>\n                         Path to the Kerberos keytab file for authentication in local mode.\n"})}),"\n",(0,a.jsx)(t.p,{children:"One popular option is to use regular expressions to execute multiple feeds together.\nIn our case, we can run the entire data pipeline with the following command :"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"./startJob.sh --config /mnt/config --feed-sel '.*'\n"})}),"\n",(0,a.jsx)(t.admonition,{type:"warning",children:(0,a.jsxs)(t.p,{children:["Note the regex feed selection ",(0,a.jsx)(t.code,{children:".*"})," needs to be specified in quotation marks (",(0,a.jsx)(t.code,{children:"'.*'"})," or ",(0,a.jsx)(t.code,{children:'".*"'}),"), otherwise your shell might substitute the asterisk with files..."]})}),"\n",(0,a.jsx)(t.h2,{id:"example-of-common-mistake",children:"Example of Common Mistake"}),"\n",(0,a.jsxs)(t.p,{children:["One common mistake is mixing up the types of Data Objects.\nTo give you some experience on how to debug your config, you can also try out what happens if you change the type of ",(0,a.jsx)(t.em,{children:"stg-airports"})," to JsonFileDataObject.\nYou will get an error message which indicates that there might be some format problem, but it is hard to spot :"]}),"\n",(0,a.jsxs)(t.p,{children:["Error: cannot resolve '",(0,a.jsx)(t.code,{children:"ident"}),"' given input columns: [stg_airports._corrupt_record]; line 1 pos 7;"]}),"\n",(0,a.jsxs)(t.p,{children:["The FileTransferAction will save the result from the Webservice with the JsonFileDataObject as file with filetype *.json.\nThen Spark tries to parse the CSV-records in the *.json file with a JSON-Parser. It is unable to properly read the data.\nHowever, it generates a column named ",(0,a.jsx)(t.em,{children:"_corrupt_record"})," describing what went wrong.\nIf you know Apache Spark, this column will look very familiar to you.\nAfter that, the query fails, because it only finds that column with error messages instead of the actual data."]}),"\n",(0,a.jsxs)(t.p,{children:["One way to get a better error message is to tell Spark that it should promptly fail when reading a corrupt file.\nYou can do that with the option ",(0,a.jsx)(t.a,{href:"https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=2-13-3",children:"jsonOptions"}),",\nwhich allows you to directly pass on settings to Spark."]}),"\n",(0,a.jsx)(t.p,{children:"In our case, we would end up with a faulty DataObject that looks like this:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:'  stg-airports {\n    type = JsonFileDataObject\n    path = "~{id}"\n    jsonOptions {\n      "mode"="failfast"\n    }\n  }\n'})}),"\n",(0,a.jsx)(t.p,{children:"This time, it will fail with this error message:"}),"\n",(0,a.jsxs)(t.p,{children:['Exception in thread "main" io.smartdatalake.workflow.TaskFailedException: Task select-airport-cols failed.\nRoot cause is \'SparkException: Malformed records are detected in schema inference.\nParse Mode: FAILFAST. Reasons: Failed to infer a common schema. Struct types are expected, but ',(0,a.jsx)(t.code,{children:"string"})," was found.'"]})]})}function u(e={}){const{wrapper:t}={...(0,r.R)(),...e.components};return t?(0,a.jsx)(t,{...e,children:(0,a.jsx)(d,{...e})}):d(e)}},9365:(e,t,n)=>{n.d(t,{A:()=>s});n(6540);var a=n(53);const r={tabItem:"tabItem_Ymn6"};var o=n(4848);function s(e){let{children:t,hidden:n,className:s}=e;return(0,o.jsx)("div",{role:"tabpanel",className:(0,a.A)(r.tabItem,s),hidden:n,children:t})}},1470:(e,t,n)=>{n.d(t,{A:()=>v});var a=n(6540),r=n(53),o=n(3104),s=n(6347),i=n(205),l=n(7485),c=n(1682),d=n(9466);function u(e){return a.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,a.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)}))?.filter(Boolean)??[]}function h(e){const{values:t,children:n}=e;return(0,a.useMemo)((()=>{const e=t??function(e){return u(e).map((e=>{let{props:{value:t,label:n,attributes:a,default:r}}=e;return{value:t,label:n,attributes:a,default:r}}))}(n);return function(e){const t=(0,c.X)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,n])}function p(e){let{value:t,tabValues:n}=e;return n.some((e=>e.value===t))}function m(e){let{queryString:t=!1,groupId:n}=e;const r=(0,s.W6)(),o=function(e){let{queryString:t=!1,groupId:n}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!n)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return n??null}({queryString:t,groupId:n});return[(0,l.aZ)(o),(0,a.useCallback)((e=>{if(!o)return;const t=new URLSearchParams(r.location.search);t.set(o,e),r.replace({...r.location,search:t.toString()})}),[o,r])]}function f(e){const{defaultValue:t,queryString:n=!1,groupId:r}=e,o=h(e),[s,l]=(0,a.useState)((()=>function(e){let{defaultValue:t,tabValues:n}=e;if(0===n.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:n}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${n.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const a=n.find((e=>e.default))??n[0];if(!a)throw new Error("Unexpected error: 0 tabValues");return a.value}({defaultValue:t,tabValues:o}))),[c,u]=m({queryString:n,groupId:r}),[f,b]=function(e){let{groupId:t}=e;const n=function(e){return e?`docusaurus.tab.${e}`:null}(t),[r,o]=(0,d.Dv)(n);return[r,(0,a.useCallback)((e=>{n&&o.set(e)}),[n,o])]}({groupId:r}),x=(()=>{const e=c??f;return p({value:e,tabValues:o})?e:null})();(0,i.A)((()=>{x&&l(x)}),[x]);return{selectedValue:s,selectValue:(0,a.useCallback)((e=>{if(!p({value:e,tabValues:o}))throw new Error(`Can't select invalid tab value=${e}`);l(e),u(e),b(e)}),[u,b,o]),tabValues:o}}var b=n(2303);const x={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"};var w=n(4848);function g(e){let{className:t,block:n,selectedValue:a,selectValue:s,tabValues:i}=e;const l=[],{blockElementScrollPositionUntilNextRender:c}=(0,o.a_)(),d=e=>{const t=e.currentTarget,n=l.indexOf(t),r=i[n].value;r!==a&&(c(t),s(r))},u=e=>{let t=null;switch(e.key){case"Enter":d(e);break;case"ArrowRight":{const n=l.indexOf(e.currentTarget)+1;t=l[n]??l[0];break}case"ArrowLeft":{const n=l.indexOf(e.currentTarget)-1;t=l[n]??l[l.length-1];break}}t?.focus()};return(0,w.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,r.A)("tabs",{"tabs--block":n},t),children:i.map((e=>{let{value:t,label:n,attributes:o}=e;return(0,w.jsx)("li",{role:"tab",tabIndex:a===t?0:-1,"aria-selected":a===t,ref:e=>l.push(e),onKeyDown:u,onClick:d,...o,className:(0,r.A)("tabs__item",x.tabItem,o?.className,{"tabs__item--active":a===t}),children:n??t},t)}))})}function j(e){let{lazy:t,children:n,selectedValue:r}=e;const o=(Array.isArray(n)?n:[n]).filter(Boolean);if(t){const e=o.find((e=>e.props.value===r));return e?(0,a.cloneElement)(e,{className:"margin-top--md"}):null}return(0,w.jsx)("div",{className:"margin-top--md",children:o.map(((e,t)=>(0,a.cloneElement)(e,{key:t,hidden:e.props.value!==r})))})}function y(e){const t=f(e);return(0,w.jsxs)("div",{className:(0,r.A)("tabs-container",x.tabList),children:[(0,w.jsx)(g,{...e,...t}),(0,w.jsx)(j,{...e,...t})]})}function v(e){const t=(0,b.A)();return(0,w.jsx)(y,{...e,children:u(e.children)},String(t))}},8453:(e,t,n)=>{n.d(t,{R:()=>s,x:()=>i});var a=n(6540);const r={},o=a.createContext(r);function s(e){const t=a.useContext(o);return a.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function i(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:s(e.components),a.createElement(o.Provider,{value:t},e.children)}}}]);