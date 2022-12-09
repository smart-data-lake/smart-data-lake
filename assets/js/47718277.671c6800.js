"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[1760],{3905:(e,t,a)=>{a.d(t,{Zo:()=>u,kt:()=>h});var n=a(7294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},i=Object.keys(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),d=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},u=function(e){var t=d(e.components);return n.createElement(s.Provider,{value:t},e.children)},c="mdxType",p={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,i=e.originalType,s=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),c=d(a),m=r,h=c["".concat(s,".").concat(m)]||c[m]||p[m]||i;return a?n.createElement(h,o(o({ref:t},u),{},{components:a})):n.createElement(h,o({ref:t},u))}));function h(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=a.length,o=new Array(i);o[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[c]="string"==typeof e?e:r,o[1]=l;for(var d=2;d<i;d++)o[d]=a[d];return n.createElement.apply(null,o)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},5162:(e,t,a)=>{a.d(t,{Z:()=>o});var n=a(7294),r=a(6010);const i="tabItem_Ymn6";function o(e){let{children:t,hidden:a,className:o}=e;return n.createElement("div",{role:"tabpanel",className:(0,r.Z)(i,o),hidden:a},t)}},5488:(e,t,a)=>{a.d(t,{Z:()=>m});var n=a(7462),r=a(7294),i=a(6010),o=a(2389),l=a(7392),s=a(7094),d=a(2466);const u="tabList__CuJ",c="tabItem_LNqP";function p(e){const{lazy:t,block:a,defaultValue:o,values:p,groupId:m,className:h}=e,f=r.Children.map(e.children,(e=>{if((0,r.isValidElement)(e)&&"value"in e.props)return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)})),g=p??f.map((e=>{let{props:{value:t,label:a,attributes:n}}=e;return{value:t,label:a,attributes:n}})),b=(0,l.l)(g,((e,t)=>e.value===t.value));if(b.length>0)throw new Error(`Docusaurus error: Duplicate values "${b.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`);const k=null===o?o:o??f.find((e=>e.props.default))?.props.value??f[0].props.value;if(null!==k&&!g.some((e=>e.value===k)))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${k}" but none of its children has the corresponding value. Available values are: ${g.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);const{tabGroupChoices:v,setTabGroupChoices:w}=(0,s.U)(),[y,x]=(0,r.useState)(k),N=[],{blockElementScrollPositionUntilNextRender:D}=(0,d.o5)();if(null!=m){const e=v[m];null!=e&&e!==y&&g.some((t=>t.value===e))&&x(e)}const C=e=>{const t=e.currentTarget,a=N.indexOf(t),n=g[a].value;n!==y&&(D(t),x(n),null!=m&&w(m,String(n)))},S=e=>{let t=null;switch(e.key){case"ArrowRight":{const a=N.indexOf(e.currentTarget)+1;t=N[a]??N[0];break}case"ArrowLeft":{const a=N.indexOf(e.currentTarget)-1;t=N[a]??N[N.length-1];break}}t?.focus()};return r.createElement("div",{className:(0,i.Z)("tabs-container",u)},r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,i.Z)("tabs",{"tabs--block":a},h)},g.map((e=>{let{value:t,label:a,attributes:o}=e;return r.createElement("li",(0,n.Z)({role:"tab",tabIndex:y===t?0:-1,"aria-selected":y===t,key:t,ref:e=>N.push(e),onKeyDown:S,onFocus:C,onClick:C},o,{className:(0,i.Z)("tabs__item",c,o?.className,{"tabs__item--active":y===t})}),a??t)}))),t?(0,r.cloneElement)(f.filter((e=>e.props.value===y))[0],{className:"margin-top--md"}):r.createElement("div",{className:"margin-top--md"},f.map(((e,t)=>(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==y})))))}function m(e){const t=(0,o.Z)();return r.createElement(p,(0,n.Z)({key:String(t)},e))}},3005:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>u,contentTitle:()=>s,default:()=>m,frontMatter:()=>l,metadata:()=>d,toc:()=>c});var n=a(7462),r=(a(7294),a(3905)),i=a(5488),o=a(5162);const l={id:"incremental-mode",title:"Incremental Mode"},s=void 0,d={unversionedId:"getting-started/part-3/incremental-mode",id:"getting-started/part-3/incremental-mode",title:"Incremental Mode",description:"Goal",source:"@site/docs/getting-started/part-3/incremental-mode.md",sourceDirName:"getting-started/part-3",slug:"/getting-started/part-3/incremental-mode",permalink:"/docs/getting-started/part-3/incremental-mode",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-3/incremental-mode.md",tags:[],version:"current",frontMatter:{id:"incremental-mode",title:"Incremental Mode"},sidebar:"docs",previous:{title:"Custom Webservice",permalink:"/docs/getting-started/part-3/custom-webservice"},next:{title:"Common Problems",permalink:"/docs/getting-started/troubleshooting/common-problems"}},u={},c=[{value:"Goal",id:"goal",level:2},{value:"Define Data Objects",id:"define-data-objects",level:2},{value:"Define state variables",id:"define-state-variables",level:2},{value:"Read and write state",id:"read-and-write-state",level:2},{value:"Try it out",id:"try-it-out",level:2},{value:"Define a Query Logic",id:"define-a-query-logic",level:2}],p={toc:c};function m(e){let{components:t,...l}=e;return(0,r.kt)("wrapper",(0,n.Z)({},p,l,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h2",{id:"goal"},"Goal"),(0,r.kt)("p",null,"The goal of this part is to use the DataObject's state, such that it can be used in subsequent requests.\nThis allows for more dynamic querying of the API.\nFor example in the ",(0,r.kt)("inlineCode",{parentName:"p"},"ext-departures")," DataObject we currently query the API with fixed airport, begin and end query parameters.\nConsequently, we will always query the same time period for a given airport.\nIn a more real-world example, we would want a delta load mechanism that loads any new data since the last execution of the action.\nTo demonstrate these incremental queries based on previous state we will start by rewriting our configuration file."),(0,r.kt)("h2",{id:"define-data-objects"},"Define Data Objects"),(0,r.kt)("p",null,"We only make the following minor changes in our config file to Action ",(0,r.kt)("inlineCode",{parentName:"p"},"download-deduplicate-departures"),":"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'download-deduplicate-departures {\n  type = DeduplicateAction\n  inputId = ext-departures\n  outputId = int-departures\n  executionMode = { type = DataObjectStateIncrementalMode }\n  mergeModeEnable = true\n  updateCapturedColumnOnlyWhenChanged = true\n  transformers = [{\n    type = SQLDfTransformer\n    code = "select ext_departures.*, date_format(from_unixtime(firstseen),\'yyyyMMdd\') dt from ext_departures"\n  },{\n    type = ScalaCodeSparkDfTransformer\n    code = """\n      import org.apache.spark.sql.{DataFrame, SparkSession}\n      def transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {\n        import session.implicits._\n        df.dropDuplicates("icao24", "estdepartureairport", "dt")\n      }\n      // return as function\n      transform _\n    """\n  }]\n  metadata {\n    feed = deduplicate-departures\n  }\n}\n')),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Adding the executionMode ",(0,r.kt)("inlineCode",{parentName:"li"},"DataObjectStateIncrementalMode")," will enable DataObject incremental mode. With every run the input DataObject will only return new data in this mode.\nThe DataObject saves its state in the global state file that is written after each run of the Smart Data Lake Builder. You haven't worked with this state file before, more on that later."),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"mergeModeEnable = true")," tells DeduplicateAction to merge changed data into the output DataObject, instead of overwriting the whole DataObject. This is especially useful if incoming data is read incrementally.\nThe output DataObject must implement CanMergeDataFrame interface (also called trait in Scala) for this. DeltaLakeTableDataObject will then create a complex SQL-Upsert statement to merge new and changed data into existing output data."),(0,r.kt)("li",{parentName:"ul"},"By default DeduplicateAction updates column dl_captured in the output for every record it receives. To reduce the number of updated records, ",(0,r.kt)("inlineCode",{parentName:"li"},"updateCapturedColumnOnlyWhenChanged = true")," can be set.\nIn this case column dl_captured is only updated in the output, when some attribute of the record changed.")),(0,r.kt)("admonition",{type:"info"},(0,r.kt)("p",{parentName:"admonition"},"Execution mode is also something that is going to be explained in more detail later.\nFor now, just think of the execution mode as a way to select which data needs to be processed.\nIn this case, we tell Smart Data Lake Builder to load data based on the state stored in the DataObject itself.")),(0,r.kt)("admonition",{type:"caution"},(0,r.kt)("p",{parentName:"admonition"},"Remember that the time interval in ",(0,r.kt)("inlineCode",{parentName:"p"},"ext-departures")," should not be larger than a week. As mentioned, we will implement a simple incremental query logic that always queries from the last execution time until the current execution.\nIf the time difference between the last execution and the current execution time is larger than a week, we will query the next four days since the last execution time. Otherwise we query the data from the last execution until now.")),(0,r.kt)("h2",{id:"define-state-variables"},"Define state variables"),(0,r.kt)("p",null,"To make use of the newly configured execution mode, we need state variables. Add the following two variables to our CustomWebserviceDataObject."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},"  private var previousState : Seq[State] = Seq()\n  private var nextState : Seq[State] = Seq()\n")),(0,r.kt)("p",null,"The corresponding ",(0,r.kt)("inlineCode",{parentName:"p"},"State")," case class is defined as "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},"  case class State(airport: String, nextBegin: Long)\n")),(0,r.kt)("p",null,"and should be added in the same file outside the DataObject. For example, add it just below the already existing case classes.\nThe state always stores the ",(0,r.kt)("inlineCode",{parentName:"p"},"airport")," and a ",(0,r.kt)("inlineCode",{parentName:"p"},"nextBegin")," as unix timestamp to indicate to the next run, what data needs to be loaded. "),(0,r.kt)("p",null,"Concerning the state variables, ",(0,r.kt)("inlineCode",{parentName:"p"},"previousState")," will basically be used for all the logic of the DataObject and ",(0,r.kt)("inlineCode",{parentName:"p"},"nextState")," will be used to store the state for the next run."),(0,r.kt)("h2",{id:"read-and-write-state"},"Read and write state"),(0,r.kt)("p",null,"To actually work with the state, we need to implement the ",(0,r.kt)("inlineCode",{parentName:"p"},"CanCreateIncrementalOutput")," trait.\nThis can be done by adding ",(0,r.kt)("inlineCode",{parentName:"p"},"with CanCreateIncrementalOutput")," to the ",(0,r.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject"),".\nConsequently, we need to implement the functions ",(0,r.kt)("inlineCode",{parentName:"p"},"setState")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"getState")," defined in the trait. "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},"override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {\n  implicit val formats: Formats = DefaultFormats\n  previousState = state.map(s => JsonMethods.parse(s).extract[Seq[State]]).getOrElse(Seq())\n}\n\noverride def getState: Option[String] = {\n  implicit val formats: Formats = DefaultFormats\n  Some(Serialization.write(nextState))\n}\n")),(0,r.kt)("p",null,"We can see that by implementing these two functions, we start using the variables defined in the section above."),(0,r.kt)("h2",{id:"try-it-out"},"Try it out"),(0,r.kt)("p",null,"We only spoke about this state, but it was never explained where it is stored.\nTo work with a state, we need to introduce two new command line parameters: ",(0,r.kt)("inlineCode",{parentName:"p"},"--state-path")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"--name")," or ",(0,r.kt)("inlineCode",{parentName:"p"},"-n")," in short.\nThis allows us to define the folder and name of the state file.\nTo have access to the state file, we specify the path to be in an already mounted folder."),(0,r.kt)(i.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,r.kt)(o.Z,{value:"docker",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-jsx"},'docker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\ndocker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network getting-started_default sdl-spark:latest --config /mnt/config --feed-sel ids:download-deduplicate-departures --state-path /mnt/data/state -n getting-started\n'))),(0,r.kt)(o.Z,{value:"podman",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-jsx"},'podman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\npodman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --hostname=localhost --pod getting-started sdl-spark:latest --config /mnt/config --feed-sel ids:download-deduplicate-departures --state-path /mnt/data/state -n getting-started\n')))),(0,r.kt)("p",null,"Use this slightly modified command to run ",(0,r.kt)("inlineCode",{parentName:"p"},"download-deduplicate-departures")," Action.\nNothing should have changed so far, since we only read and write an empty state.",(0,r.kt)("br",{parentName:"p"}),"\n","You can verify this by opening the file ",(0,r.kt)("inlineCode",{parentName:"p"},"getting-started.<runId>.<attemptId>.json")," and having a look at the field ",(0,r.kt)("inlineCode",{parentName:"p"},"dataObjectsState"),". The stored state is currently empty.\nIn the next section, we will assign a value to ",(0,r.kt)("inlineCode",{parentName:"p"},"nextState"),", such that the ",(0,r.kt)("inlineCode",{parentName:"p"},"DataObject's")," state is written. "),(0,r.kt)("admonition",{type:"info"},(0,r.kt)("p",{parentName:"admonition"},"The same state file is used by Smart Data Lake Builder to enable automatic recoveries of failed jobs.\nThis will also be explained in detail separately, but we're mentioning the fact here, so you can understand the two variables ",(0,r.kt)("inlineCode",{parentName:"p"},"<runId>")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"<attemptId>")," appearing in the file name.\nFor each execution, the ",(0,r.kt)("inlineCode",{parentName:"p"},"<runId>")," is incremented by one.\nThe ",(0,r.kt)("inlineCode",{parentName:"p"},"<attemptId>")," is usually 1, but gets increased by one if Smart Data Lake Builder has to recover a failed execution.")),(0,r.kt)("h2",{id:"define-a-query-logic"},"Define a Query Logic"),(0,r.kt)("p",null,"Now we want to achieve the following query logic:"),(0,r.kt)("p",null,"The starting point are the query parameters provided in the configuration file and no previous state.\nDuring the first execution, we query the departures for the two airports in the given time window.\nAfterwards, the ",(0,r.kt)("inlineCode",{parentName:"p"},"end"),"-parameter of the current query will be stored as ",(0,r.kt)("inlineCode",{parentName:"p"},"begin"),"-parameter for the next query.\nNow the true incremental phase starts as we can now get the state of the last successful run.\nWe query the flight-data API to get data from the last successful run up until now.\nFor this to work, we need to make two changes. First add the variable"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"private val now = Instant.now.getEpochSecond\n")),(0,r.kt)("p",null,"just below the ",(0,r.kt)("inlineCode",{parentName:"p"},"nextState")," variable. Then modify the ",(0,r.kt)("inlineCode",{parentName:"p"},"currentQueryParameters")," variable according to"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},"// if we have query parameters in the state we will use them from now on\nval currentQueryParameters = if (previousState.isEmpty) checkQueryParameters(queryParameters) else checkQueryParameters(previousState.map{\n  x => DepartureQueryParameters(x.airport, x.nextBegin, now)\n})\n")),(0,r.kt)("p",null,"The implemented logic "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},"nextState = currentQueryParameters.map(params => State(params.airport, params.end))\n")),(0,r.kt)("p",null,"for the next state can be placed below the comment ",(0,r.kt)("inlineCode",{parentName:"p"},"// put simple nextState logic below"),". "),(0,r.kt)("p",null,"Compile and execute the code of this project again and execute it multiple times.\nThe scenario will be that the first run fetches the data defined in the configuration file, then the proceeding run retrieves the data from the endpoint of the last run until now.\nIf this time difference is larger than a week, the program only queries the next four days since the last execution.\nIf there is no data available in a time window, because only a few seconds have passed since the last execution, the execution will fail with Error ",(0,r.kt)("strong",{parentName:"p"},"404"),"."),(0,r.kt)("p",null,"At the end your config file should look something like ",(0,r.kt)("a",{target:"_blank",href:a(2196).Z},"this")," and the CustomWebserviceDataObject code like ",(0,r.kt)("a",{target:"_blank",href:a(355).Z},"this"),"."),(0,r.kt)("admonition",{type:"info"},(0,r.kt)("p",{parentName:"admonition"},"Unfortunately, the webservice on opensky-network.org responds with a ",(0,r.kt)("strong",{parentName:"p"},"404")," error code when no data is available, rather than a ",(0,r.kt)("strong",{parentName:"p"},"200")," and an empty response.\nTherefore, SDLB gets a 404 and will fail the execution. The exception could be catched inside CustomWebserviceDataObject, but what if we have a real 404 error?!")),(0,r.kt)("p",null,"Congratulation, you just completed implementing a nice incremental loading mechanism!"),(0,r.kt)("p",null,"That's it from getting-started for now. We hope you enjoyed your first steps with SDLB.\nFor further information, check the rest of the documentation and the blog on this page!"))}m.isMDXComponent=!0},355:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/CustomWebserviceDataObject-2-03fe1158d189cf1bd05ab35b7bcd8768.scala"},2196:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/application-part3-download-incremental-mode-94f57db928615c7216b82f655dd11d5c.conf"}}]);