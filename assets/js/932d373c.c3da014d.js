"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[5833],{9599:(e,n,a)=>{a.r(n),a.d(n,{assets:()=>d,contentTitle:()=>l,default:()=>m,frontMatter:()=>o,metadata:()=>c,toc:()=>u});var t=a(4848),r=a(8453),s=a(1470),i=a(9365);const o={id:"commandLine",title:"Command Line"},l="Launch Java application using Spark-Submit",c={id:"reference/commandLine",title:"Command Line",description:"SmartDataLakeBuilder is a java application.",source:"@site/docs/reference/commandLine.md",sourceDirName:"reference",slug:"/reference/commandLine",permalink:"/docs/reference/commandLine",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/commandLine.md",tags:[],version:"current",frontMatter:{id:"commandLine",title:"Command Line"},sidebar:"tutorialSidebar",previous:{title:"Build SDL",permalink:"/docs/reference/build"},next:{title:"Data Objects",permalink:"/docs/reference/dataObjects"}},d={},u=[{value:"Pods with Podman",id:"pods-with-podman",level:2}];function p(e){const n={a:"a",code:"code",em:"em",h1:"h1",h2:"h2",li:"li",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,r.R)(),...e.components};return(0,t.jsxs)(t.Fragment,{children:[(0,t.jsx)(n.h1,{id:"launch-java-application-using-spark-submit",children:"Launch Java application using Spark-Submit"}),"\n",(0,t.jsxs)(n.p,{children:["SmartDataLakeBuilder is a java application.\nTo run on a cluster with spark-submit, use ",(0,t.jsx)(n.strong,{children:"DefaultSmartDataLakeBuilder"})," application.\nIt can be started with the following command line options (for details, see ",(0,t.jsx)(n.a,{href:"/docs/reference/deployYarn",children:"YARN"}),")."]}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-bash",children:"spark-submit --master yarn --deploy-mode client --class io.smartdatalake.app.DefaultSmartDataLakeBuilder target/smartdatalake_2.12-2.5.1-jar-with-dependencies.jar [arguments]\n"})}),"\n",(0,t.jsx)(n.p,{children:"and takes the following arguments:"}),"\n",(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{children:"Usage: DefaultSmartDataLakeBuilder [options]\n  -f, --feed-sel <operation?><;prefix:?><regex>[,<operation?><prefix:?><regex>...]\n    Select actions to execute by one or multiple expressions separated by comma (,). Results from multiple expressions are combined from left to right.\n    Operations:\n    - pipe symbol (|): the two sets are combined by union operation (default)\n    - ampersand symbol (&): the two sets are combined by intersection operation\n    - minus symbol (-): the second set is subtracted from the first set\n    Prefixes:\n    - 'feeds': select actions where metadata.feed is matched by regex pattern (default)\n    - 'names': select actions where metadata.name is matched by regex pattern\n    - 'ids': select actions where id is matched by regex pattern\n    - 'layers': select actions where metadata.layer of all output DataObjects is matched by regex pattern\n    - 'startFromActionIds': select actions which with id is matched by regex pattern and any dependent action (=successors)\n    - 'endWithActionIds': select actions which with id is matched by regex pattern and their predecessors\n    - 'startFromDataObjectIds': select actions which have an input DataObject with id is matched by regex pattern and any dependent action (=successors)\n    - 'endWithDataObjectIds': select actions which have an output DataObject with id is matched by regex pattern and their predecessors\n    All matching is done case-insensitive.\n    Example: to filter action 'A' and its successors but only in layer L1 and L2, use the following pattern: \"startFromActionIds:a,&layers:(l1|l2)\"\n  -n, --name <value>\n    Optional name of the application. If not specified feed-sel is used.\n  -c, --config <file1>[,<file2>...]\n    One or multiple configuration files or directories containing configuration files, separated by comma.\n    Entries must be valid Hadoop URIs or a special URI with scheme \"cp\" which is treated as classpath entry.\n  --partition-values <partitionColName>=<partitionValue>[,<partitionValue>,...]\n    Partition values to process for one single partition column.\n  --multi-partition-values <partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>[;<partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>;...]\n    Partition values to process for multiple partition columns.\n  -s, --streaming\n    Enable streaming mode for continuous processing.\n  --parallelism <int>\n    Parallelism for DAG run.\n  --state-path <path>\n    Path to save run state files. Must be set to enable recovery in case of failures.\n  --override-jars <jar1>[,<jar2>...]\n    Comma separated list of jar filenames for child-first class loader. The jars must be present in classpath.\n  --test <config|dry-run>\n    Run in test mode: config -> validate configuration, dry-run -> execute prepare- and init-phase only to check environment and spark lineage\n  --help\n    Display the help text.\n  --version\n    Display version information.\n"})}),"\n",(0,t.jsxs)(n.p,{children:["The ",(0,t.jsx)(n.strong,{children:"DefaultSmartDataLakeBuilder"})," class should be fine in most situations.\nThere are two additional, adapted application versions you can use:"]}),"\n",(0,t.jsxs)(n.ul,{children:["\n",(0,t.jsxs)(n.li,{children:[(0,t.jsx)(n.strong,{children:"LocalSmartDataLakeBuilder"}),": default for Spark master is ",(0,t.jsx)(n.code,{children:"local[*]"})," in this case, and it has additional properties to configure Kerberos authentication.\nUse can use this application to run in a local environment (e.g. IntelliJ) without cluster deployment."]}),"\n",(0,t.jsxs)(n.li,{children:[(0,t.jsx)(n.strong,{children:"DatabricksSmartDataLakeBuilder"}),": see ",(0,t.jsx)(n.a,{href:"/docs/reference/deploy-microsoft-azure",children:"Microsoft Azure"}),", special class when running a Databricks Cluster."]}),"\n"]}),"\n",(0,t.jsx)(n.h1,{id:"launching-sdl-container",children:"Launching SDL container"}),"\n",(0,t.jsxs)(n.p,{children:["Depending on the container definition, especially the entrypoint the arguments may vary. Furthermore, we distinguish starting the container using ",(0,t.jsx)(n.em,{children:"docker"})," or ",(0,t.jsx)(n.em,{children:"podman"}),"."]}),"\n",(0,t.jsx)(n.p,{children:"In general a container launch would look like:"}),"\n",(0,t.jsxs)(s.A,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],children:[(0,t.jsx)(i.A,{value:"docker",children:(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-jsx",children:"docker run [docker-args] sdl-spark --config [config-file] --feed-sel [feed] [further-SDL-args]\n"})})}),(0,t.jsx)(i.A,{value:"podman",children:(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-jsx",children:"podman run [docker-args] sdl-spark --config [config-file] --feed-sel [feed] [further-SDL-args]\n"})})})]}),"\n",(0,t.jsx)(n.p,{children:"These could also include mounted directories for configurations, additional Scala Classes, data directories, etc."}),"\n",(0,t.jsxs)(s.A,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],children:[(0,t.jsx)(i.A,{value:"docker",children:(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-jsx",children:"docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\n"})})}),(0,t.jsx)(i.A,{value:"podman",children:(0,t.jsx)(n.pre,{children:(0,t.jsx)(n.code,{className:"language-jsx",children:"podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\n"})})})]}),"\n",(0,t.jsx)(n.h2,{id:"pods-with-podman",children:"Pods with Podman"}),"\n",(0,t.jsx)(n.p,{children:"When interacting between multiple containers, e.g. SDL container and a metastore container, pods are utilized to manage the container and especially the network. A set of containers is launched using podman-compose.sh."}),"\n",(0,t.jsxs)(n.p,{children:["Assuming an existing pod ",(0,t.jsx)(n.code,{children:"mypod"})," is running, another container can be started within this pod using the additional podman arguments ",(0,t.jsx)(n.code,{children:"--pod mypod --hostname myhost --add-host myhost:127.0.0.1"}),".\nThe hostname specification fixes an issue in resolving the own localhost."]})]})}function m(e={}){const{wrapper:n}={...(0,r.R)(),...e.components};return n?(0,t.jsx)(n,{...e,children:(0,t.jsx)(p,{...e})}):p(e)}},9365:(e,n,a)=>{a.d(n,{A:()=>i});a(6540);var t=a(53);const r={tabItem:"tabItem_Ymn6"};var s=a(4848);function i(e){let{children:n,hidden:a,className:i}=e;return(0,s.jsx)("div",{role:"tabpanel",className:(0,t.A)(r.tabItem,i),hidden:a,children:n})}},1470:(e,n,a)=>{a.d(n,{A:()=>k});var t=a(6540),r=a(53),s=a(3104),i=a(6347),o=a(205),l=a(7485),c=a(1682),d=a(9466);function u(e){return t.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,t.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)}))?.filter(Boolean)??[]}function p(e){const{values:n,children:a}=e;return(0,t.useMemo)((()=>{const e=n??function(e){return u(e).map((e=>{let{props:{value:n,label:a,attributes:t,default:r}}=e;return{value:n,label:a,attributes:t,default:r}}))}(a);return function(e){const n=(0,c.X)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error(`Docusaurus error: Duplicate values "${n.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[n,a])}function m(e){let{value:n,tabValues:a}=e;return a.some((e=>e.value===n))}function h(e){let{queryString:n=!1,groupId:a}=e;const r=(0,i.W6)(),s=function(e){let{queryString:n=!1,groupId:a}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!a)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return a??null}({queryString:n,groupId:a});return[(0,l.aZ)(s),(0,t.useCallback)((e=>{if(!s)return;const n=new URLSearchParams(r.location.search);n.set(s,e),r.replace({...r.location,search:n.toString()})}),[s,r])]}function f(e){const{defaultValue:n,queryString:a=!1,groupId:r}=e,s=p(e),[i,l]=(0,t.useState)((()=>function(e){let{defaultValue:n,tabValues:a}=e;if(0===a.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(n){if(!m({value:n,tabValues:a}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${n}" but none of its children has the corresponding value. Available values are: ${a.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return n}const t=a.find((e=>e.default))??a[0];if(!t)throw new Error("Unexpected error: 0 tabValues");return t.value}({defaultValue:n,tabValues:s}))),[c,u]=h({queryString:a,groupId:r}),[f,g]=function(e){let{groupId:n}=e;const a=function(e){return e?`docusaurus.tab.${e}`:null}(n),[r,s]=(0,d.Dv)(a);return[r,(0,t.useCallback)((e=>{a&&s.set(e)}),[a,s])]}({groupId:r}),b=(()=>{const e=c??f;return m({value:e,tabValues:s})?e:null})();(0,o.A)((()=>{b&&l(b)}),[b]);return{selectedValue:i,selectValue:(0,t.useCallback)((e=>{if(!m({value:e,tabValues:s}))throw new Error(`Can't select invalid tab value=${e}`);l(e),u(e),g(e)}),[u,g,s]),tabValues:s}}var g=a(2303);const b={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"};var v=a(4848);function x(e){let{className:n,block:a,selectedValue:t,selectValue:i,tabValues:o}=e;const l=[],{blockElementScrollPositionUntilNextRender:c}=(0,s.a_)(),d=e=>{const n=e.currentTarget,a=l.indexOf(n),r=o[a].value;r!==t&&(c(n),i(r))},u=e=>{let n=null;switch(e.key){case"Enter":d(e);break;case"ArrowRight":{const a=l.indexOf(e.currentTarget)+1;n=l[a]??l[0];break}case"ArrowLeft":{const a=l.indexOf(e.currentTarget)-1;n=l[a]??l[l.length-1];break}}n?.focus()};return(0,v.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,r.A)("tabs",{"tabs--block":a},n),children:o.map((e=>{let{value:n,label:a,attributes:s}=e;return(0,v.jsx)("li",{role:"tab",tabIndex:t===n?0:-1,"aria-selected":t===n,ref:e=>l.push(e),onKeyDown:u,onClick:d,...s,className:(0,r.A)("tabs__item",b.tabItem,s?.className,{"tabs__item--active":t===n}),children:a??n},n)}))})}function j(e){let{lazy:n,children:a,selectedValue:r}=e;const s=(Array.isArray(a)?a:[a]).filter(Boolean);if(n){const e=s.find((e=>e.props.value===r));return e?(0,t.cloneElement)(e,{className:"margin-top--md"}):null}return(0,v.jsx)("div",{className:"margin-top--md",children:s.map(((e,n)=>(0,t.cloneElement)(e,{key:n,hidden:e.props.value!==r})))})}function y(e){const n=f(e);return(0,v.jsxs)("div",{className:(0,r.A)("tabs-container",b.tabList),children:[(0,v.jsx)(x,{...e,...n}),(0,v.jsx)(j,{...e,...n})]})}function k(e){const n=(0,g.A)();return(0,v.jsx)(y,{...e,children:u(e.children)},String(n))}},8453:(e,n,a)=>{a.d(n,{R:()=>i,x:()=>o});var t=a(6540);const r={},s=t.createContext(r);function i(e){const n=t.useContext(s);return t.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function o(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(r):e.components||r:i(e.components),t.createElement(s.Provider,{value:n},e.children)}}}]);