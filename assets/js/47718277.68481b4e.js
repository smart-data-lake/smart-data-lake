"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[760],{3905:function(e,t,a){a.d(t,{Zo:function(){return u},kt:function(){return m}});var n=a(7294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},i=Object.keys(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),d=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},u=function(e){var t=d(e.components);return n.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},p=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,i=e.originalType,s=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),p=d(a),m=r,h=p["".concat(s,".").concat(m)]||p[m]||c[m]||i;return a?n.createElement(h,o(o({ref:t},u),{},{components:a})):n.createElement(h,o({ref:t},u))}));function m(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=a.length,o=new Array(i);o[0]=p;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:r,o[1]=l;for(var d=2;d<i;d++)o[d]=a[d];return n.createElement.apply(null,o)}return n.createElement.apply(null,a)}p.displayName="MDXCreateElement"},8215:function(e,t,a){var n=a(7294);t.Z=function(e){var t=e.children,a=e.hidden,r=e.className;return n.createElement("div",{role:"tabpanel",hidden:a,className:r},t)}},9877:function(e,t,a){a.d(t,{Z:function(){return u}});var n=a(7462),r=a(7294),i=a(2389),o=a(9548),l=a(6010),s="tabItem_LplD";function d(e){var t,a,i,d=e.lazy,u=e.block,c=e.defaultValue,p=e.values,m=e.groupId,h=e.className,f=r.Children.map(e.children,(function(e){if((0,r.isValidElement)(e)&&void 0!==e.props.value)return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})),v=null!=p?p:f.map((function(e){var t=e.props;return{value:t.value,label:t.label,attributes:t.attributes}})),g=(0,o.lx)(v,(function(e,t){return e.value===t.value}));if(g.length>0)throw new Error('Docusaurus error: Duplicate values "'+g.map((function(e){return e.value})).join(", ")+'" found in <Tabs>. Every value needs to be unique.');var k=null===c?c:null!=(t=null!=c?c:null==(a=f.find((function(e){return e.props.default})))?void 0:a.props.value)?t:null==(i=f[0])?void 0:i.props.value;if(null!==k&&!v.some((function(e){return e.value===k})))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+k+'" but none of its children has the corresponding value. Available values are: '+v.map((function(e){return e.value})).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");var w=(0,o.UB)(),b=w.tabGroupChoices,y=w.setTabGroupChoices,N=(0,r.useState)(k),x=N[0],D=N[1],C=[],S=(0,o.o5)().blockElementScrollPositionUntilNextRender;if(null!=m){var O=b[m];null!=O&&O!==x&&v.some((function(e){return e.value===O}))&&D(O)}var j=function(e){var t=e.currentTarget,a=C.indexOf(t),n=v[a].value;n!==x&&(S(t),D(n),null!=m&&y(m,n))},T=function(e){var t,a=null;switch(e.key){case"ArrowRight":var n=C.indexOf(e.currentTarget)+1;a=C[n]||C[0];break;case"ArrowLeft":var r=C.indexOf(e.currentTarget)-1;a=C[r]||C[C.length-1]}null==(t=a)||t.focus()};return r.createElement("div",{className:"tabs-container"},r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,l.Z)("tabs",{"tabs--block":u},h)},v.map((function(e){var t=e.value,a=e.label,i=e.attributes;return r.createElement("li",(0,n.Z)({role:"tab",tabIndex:x===t?0:-1,"aria-selected":x===t,key:t,ref:function(e){return C.push(e)},onKeyDown:T,onFocus:j,onClick:j},i,{className:(0,l.Z)("tabs__item",s,null==i?void 0:i.className,{"tabs__item--active":x===t})}),null!=a?a:t)}))),d?(0,r.cloneElement)(f.filter((function(e){return e.props.value===x}))[0],{className:"margin-vert--md"}):r.createElement("div",{className:"margin-vert--md"},f.map((function(e,t){return(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==x})}))))}function u(e){var t=(0,i.Z)();return r.createElement(d,(0,n.Z)({key:String(t)},e))}},3005:function(e,t,a){a.r(t),a.d(t,{contentTitle:function(){return u},default:function(){return h},frontMatter:function(){return d},metadata:function(){return c},toc:function(){return p}});var n=a(7462),r=a(3366),i=(a(7294),a(3905)),o=a(9877),l=a(8215),s=["components"],d={id:"incremental-mode",title:"Incremental Mode"},u=void 0,c={unversionedId:"getting-started/part-3/incremental-mode",id:"getting-started/part-3/incremental-mode",title:"Incremental Mode",description:"Goal",source:"@site/docs/getting-started/part-3/incremental-mode.md",sourceDirName:"getting-started/part-3",slug:"/getting-started/part-3/incremental-mode",permalink:"/docs/getting-started/part-3/incremental-mode",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-3/incremental-mode.md",tags:[],version:"current",frontMatter:{id:"incremental-mode",title:"Incremental Mode"},sidebar:"docs",previous:{title:"Custom Webservice",permalink:"/docs/getting-started/part-3/custom-webservice"},next:{title:"Common Problems",permalink:"/docs/getting-started/troubleshooting/common-problems"}},p=[{value:"Goal",id:"goal",children:[],level:2},{value:"Define Data Objects",id:"define-data-objects",children:[],level:2},{value:"Define state variables",id:"define-state-variables",children:[],level:2},{value:"Read and write state",id:"read-and-write-state",children:[],level:2},{value:"Try it out",id:"try-it-out",children:[],level:2},{value:"Define a Query Logic",id:"define-a-query-logic",children:[],level:2}],m={toc:p};function h(e){var t=e.components,d=(0,r.Z)(e,s);return(0,i.kt)("wrapper",(0,n.Z)({},m,d,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h2",{id:"goal"},"Goal"),(0,i.kt)("p",null,"The goal of this part is to use the DataObject's state, such that it can be used in subsequent requests.\nThis allows for more dynamic querying of the API.\nFor example in the ",(0,i.kt)("inlineCode",{parentName:"p"},"ext-departures")," DataObject we currently query the API with fixed airport, begin and end query parameters.\nConsequently, we will always query the same time period for a given airport.\nIn a more real-world example, we would want a delta load mechanism that loads any new data since the last execution of the action.\nTo demonstrate these incremental queries based on previous state we will start by rewriting our configuration file."),(0,i.kt)("h2",{id:"define-data-objects"},"Define Data Objects"),(0,i.kt)("p",null,"We only make the following minor changes in our config file to Action ",(0,i.kt)("inlineCode",{parentName:"p"},"download-deduplicate-departures"),":"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'download-deduplicate-departures {\n  type = DeduplicateAction\n  inputId = ext-departures\n  outputId = int-departures\n  executionMode = { type = DataObjectStateIncrementalMode }\n  mergeModeEnable = true\n  updateCapturedColumnOnlyWhenChanged = true\n  transformers = [{\n    type = SQLDfTransformer\n    code = "select ext_departures.*, date_format(from_unixtime(firstseen),\'yyyyMMdd\') dt from ext_departures"\n  },{\n    type = ScalaCodeDfTransformer\n    code = """\n      import org.apache.spark.sql.{DataFrame, SparkSession}\n      def transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {\n        import session.implicits._\n        df.dropDuplicates("icao24", "estdepartureairport", "dt")\n      }\n      // return as function\n      transform _\n    """\n  }]\n  metadata {\n    feed = deduplicate-departures\n  }\n}\n')),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Adding the executionMode ",(0,i.kt)("inlineCode",{parentName:"li"},"DataObjectStateIncrementalMode")," will enable DataObject incremental mode. With every run the input DataObject will only return new data in this mode.\nThe DataObject saves its state in the global state file that is written after each run of the Smart Data Lake Builder. You haven't worked with this state file before, more on that later."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"mergeModeEnable = true")," tells DeduplicateAction to merge changed data into the output DataObject, instead of overwriting the whole DataObject. This is especially useful if incoming data is read incrementally.\nThe output DataObject must implement CanMergeDataFrame interface (also called trait in Scala) for this. DeltaLakeTableDataObject will then create a complex SQL-Upsert statement to merge new and changed data into existing output data."),(0,i.kt)("li",{parentName:"ul"},"By default DeduplicateAction updates column dl_captured in the output for every record it receives. To reduce the number of updated records, ",(0,i.kt)("inlineCode",{parentName:"li"},"updateCapturedColumnOnlyWhenChanged = true")," can be set.\nIn this case column dl_captured is only updated in the output, when some attribute of the record changed.")),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"Execution mode is also something that is going to be explained in more detail later.\nFor now, just think of the execution mode as a way to select which data needs to be processed.\nIn this case, we tell Smart Data Lake Builder to load data based on the state stored in the DataObject itself."))),(0,i.kt)("div",{className:"admonition admonition-caution alert alert--warning"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"16",height:"16",viewBox:"0 0 16 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M8.893 1.5c-.183-.31-.52-.5-.887-.5s-.703.19-.886.5L.138 13.499a.98.98 0 0 0 0 1.001c.193.31.53.501.886.501h13.964c.367 0 .704-.19.877-.5a1.03 1.03 0 0 0 .01-1.002L8.893 1.5zm.133 11.497H6.987v-2.003h2.039v2.003zm0-3.004H6.987V5.987h2.039v4.006z"}))),"caution")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"Remember that the time interval in ",(0,i.kt)("inlineCode",{parentName:"p"},"ext-departures")," should not be larger than a week. As mentioned, we will implement a simple incremental query logic that always queries from the last execution time until the current execution.\nIf the time difference between the last execution and the current execution time is larger than a week, we will query the next four days since the last execution time. Otherwise we query the data from the last execution until now."))),(0,i.kt)("h2",{id:"define-state-variables"},"Define state variables"),(0,i.kt)("p",null,"To make use of the newly configured execution mode, we need state variables. Add the following two variables to our CustomWebserviceDataObject."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},"  private var previousState : Seq[State] = Seq()\n  private var nextState : Seq[State] = Seq()\n")),(0,i.kt)("p",null,"The corresponding ",(0,i.kt)("inlineCode",{parentName:"p"},"State")," case class is defined as "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},"  case class State(airport: String, nextBegin: Long)\n")),(0,i.kt)("p",null,"and should be added in the same file outside the DataObject. For example, add it just below the already existing case classes.\nThe state always stores the ",(0,i.kt)("inlineCode",{parentName:"p"},"airport")," and a ",(0,i.kt)("inlineCode",{parentName:"p"},"nextBegin")," as unix timestamp to indicate to the next run, what data needs to be loaded. "),(0,i.kt)("p",null,"Concerning the state variables, ",(0,i.kt)("inlineCode",{parentName:"p"},"previousState")," will basically be used for all the logic of the DataObject and ",(0,i.kt)("inlineCode",{parentName:"p"},"nextState")," will be used to store the state for the next run."),(0,i.kt)("h2",{id:"read-and-write-state"},"Read and write state"),(0,i.kt)("p",null,"To actually work with the state, we need to implement the ",(0,i.kt)("inlineCode",{parentName:"p"},"CanCreateIncrementalOutput")," trait.\nThis can be done by adding ",(0,i.kt)("inlineCode",{parentName:"p"},"with CanCreateIncrementalOutput")," to the ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject"),".\nConsequently, we need to implement the functions ",(0,i.kt)("inlineCode",{parentName:"p"},"setState")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"getState")," defined in the trait. "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},"override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {\n  implicit val formats: Formats = DefaultFormats\n  previousState = state.map(s => JsonMethods.parse(s).extract[Seq[State]]).getOrElse(Seq())\n}\n\noverride def getState: Option[String] = {\n  implicit val formats: Formats = DefaultFormats\n  Some(Serialization.write(nextState))\n}\n")),(0,i.kt)("p",null,"We can see that by implementing these two functions, we start using the variables defined in the section above."),(0,i.kt)("h2",{id:"try-it-out"},"Try it out"),(0,i.kt)("p",null,"We only spoke about this state, but it was never explained where it is stored.\nTo work with a state, we need to introduce two new command line parameters: ",(0,i.kt)("inlineCode",{parentName:"p"},"--state-path")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"--name")," or ",(0,i.kt)("inlineCode",{parentName:"p"},"-n")," in short.\nThis allows us to define the folder and name of the state file.\nTo have access to the state file, we specify the path to be in an already mounted folder."),(0,i.kt)(o.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,i.kt)(l.Z,{value:"docker",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-jsx"},'docker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\ndocker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network getting-started_default sdl-spark:latest --config /mnt/config --feed-sel ids:download-deduplicate-departures --state-path /mnt/data/state -n getting-started\n'))),(0,i.kt)(l.Z,{value:"podman",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-jsx"},'podman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\npodman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --pod getting-started sdl-spark:latest --config /mnt/config --feed-sel ids:download-deduplicate-departures --state-path /mnt/data/state -n getting-started\n')))),(0,i.kt)("p",null,"Use this slightly modified command to run ",(0,i.kt)("inlineCode",{parentName:"p"},"download-deduplicate-departures")," Action.\nNothing should have changed so far, since we only read and write an empty state.",(0,i.kt)("br",{parentName:"p"}),"\n","You can verify this by opening the file ",(0,i.kt)("inlineCode",{parentName:"p"},"getting-started.<runId>.<attemptId>.json")," and having a look at the field ",(0,i.kt)("inlineCode",{parentName:"p"},"dataObjectsState"),". The stored state is currently empty.\nIn the next section, we will assign a value to ",(0,i.kt)("inlineCode",{parentName:"p"},"nextState"),", such that the ",(0,i.kt)("inlineCode",{parentName:"p"},"DataObject's")," state is written. "),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"The same state file is used by Smart Data Lake Builder to enable automatic recoveries of failed jobs.\nThis will also be explained in detail separately, but we're mentioning the fact here, so you can understand the two variables ",(0,i.kt)("inlineCode",{parentName:"p"},"<runId>")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"<attemptId>")," appearing in the file name.\nFor each execution, the ",(0,i.kt)("inlineCode",{parentName:"p"},"<runId>")," is incremented by one.\nThe ",(0,i.kt)("inlineCode",{parentName:"p"},"<attemptId>")," is usually 1, but gets increased by one if Smart Data Lake Builder has to recover a failed execution."))),(0,i.kt)("h2",{id:"define-a-query-logic"},"Define a Query Logic"),(0,i.kt)("p",null,"Now we want to achieve the following query logic:"),(0,i.kt)("p",null,"The starting point are the query parameters provided in the configuration file and no previous state.\nDuring the first execution, we query the departures for the two airports in the given time window.\nAfterwards, the ",(0,i.kt)("inlineCode",{parentName:"p"},"end"),"-parameter of the current query will be stored as ",(0,i.kt)("inlineCode",{parentName:"p"},"begin"),"-parameter for the next query.\nNow the true incremental phase starts as we can now get the state of the last successful run.\nWe query the flight-data API to get data from the last successful run up until now.\nFor this to work, we need to make two changes. First add the variable"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"private val now = Instant.now.getEpochSecond\n")),(0,i.kt)("p",null,"just below the ",(0,i.kt)("inlineCode",{parentName:"p"},"nextState")," variable. Then modify the ",(0,i.kt)("inlineCode",{parentName:"p"},"currentQueryParameters")," variable according to"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},"// if we have query parameters in the state we will use them from now on\nval currentQueryParameters = if (previousState.isEmpty) checkQueryParameters(queryParameters) else checkQueryParameters(previousState.map{\n  x => DepartureQueryParameters(x.airport, x.nextBegin, now)\n})\n")),(0,i.kt)("p",null,"The implemented logic "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},"if(previousState.isEmpty){\n  nextState = currentQueryParameters.map(params => State(params.airport, params.end))\n} else {\n  nextState = previousState.map(params => State(params.airport, now))\n}\n")),(0,i.kt)("p",null,"for the next state can be placed below the comment ",(0,i.kt)("inlineCode",{parentName:"p"},"// put simple nextState logic below"),". "),(0,i.kt)("p",null,"Compile and execute the code of this project again and execute it multiple times.\nThe scenario will be that the first run fetches the data defined in the configuration file, then the proceeding run retrieves the data from the endpoint of the last run until now.\nIf this time difference is larger than a week, the program only queries the next four days since the last execution.\nIf there is no data available in a time window, because only a few seconds have passed since the last execution, the execution will fail with Error ",(0,i.kt)("strong",{parentName:"p"},"404"),"."),(0,i.kt)("p",null,"At the end your config file should look something like ",(0,i.kt)("a",{target:"_blank",href:a(6358).Z},"this")," and the CustomWebserviceDataObject code like ",(0,i.kt)("a",{target:"_blank",href:a(6471).Z},"this"),"."),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"Unfortunately, the webservice on opensky-network.org responds with a ",(0,i.kt)("strong",{parentName:"p"},"404")," error code when no data is available, rather than a ",(0,i.kt)("strong",{parentName:"p"},"200")," and an empty response.\nTherefore, SDLB gets a 404 and will fail the execution. The exception could be catched inside CustomWebserviceDataObject, but what if we have a real 404 error?!"))),(0,i.kt)("p",null,"Congratulation, you just completed implementing a nice incremental loading mechanism!"),(0,i.kt)("p",null,"That's it from getting-started for now. We hope you enjoyed your first steps with SDLB.\nFor further information, check the rest of the documentation and the blog on this page!"))}h.isMDXComponent=!0},6471:function(e,t,a){t.Z=a.p+"assets/files/CustomWebserviceDataObject-2-017115eba4df9200a80eb0db0fa96cbc.scala"},6358:function(e,t,a){t.Z=a.p+"assets/files/application-part3-download-incremental-mode-59f22507152e97d51ca1d6993c771a62.conf"}}]);