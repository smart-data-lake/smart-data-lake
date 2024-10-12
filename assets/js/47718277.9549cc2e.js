"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[5644],{1911:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>d,contentTitle:()=>s,default:()=>h,frontMatter:()=>r,metadata:()=>o,toc:()=>l});var a=n(4848),i=n(8453);const r={id:"incremental-mode",title:"Incremental Mode"},s=void 0,o={id:"getting-started/part-3/incremental-mode",title:"Incremental Mode",description:"Goal",source:"@site/docs/getting-started/part-3/incremental-mode.md",sourceDirName:"getting-started/part-3",slug:"/getting-started/part-3/incremental-mode",permalink:"/docs/getting-started/part-3/incremental-mode",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-3/incremental-mode.md",tags:[],version:"current",frontMatter:{id:"incremental-mode",title:"Incremental Mode"},sidebar:"tutorialSidebar",previous:{title:"Custom Webservice",permalink:"/docs/getting-started/part-3/custom-webservice"},next:{title:"Common Problems",permalink:"/docs/getting-started/troubleshooting/common-problems"}},d={},l=[{value:"Goal",id:"goal",level:2},{value:"Adapt Action",id:"adapt-action",level:2},{value:"Define state variables",id:"define-state-variables",level:2},{value:"Read and write state",id:"read-and-write-state",level:2},{value:"Try it out",id:"try-it-out",level:2},{value:"Define a Query Logic",id:"define-a-query-logic",level:2},{value:"Summary",id:"summary",level:2}];function c(e){const t={a:"a",admonition:"admonition",br:"br",code:"code",h2:"h2",p:"p",pre:"pre",strong:"strong",...(0,i.R)(),...e.components};return(0,a.jsxs)(a.Fragment,{children:[(0,a.jsx)(t.h2,{id:"goal",children:"Goal"}),"\n",(0,a.jsxs)(t.p,{children:["The goal of this part is to use a DataObject with state, which can be used in subsequent requests.\nThis allows for more dynamic querying of the API.\nFor example in the ",(0,a.jsx)(t.code,{children:"ext-departures"})," DataObject we currently query the API with fixed airport, begin and end query parameters.\nConsequently, we will always query the same time period for a given airport.\nIn a more real-world example, we would want a delta load mechanism that loads any new data since the last execution of the action.\nTo demonstrate these incremental queries based on previous state we will start by rewriting our configuration file."]}),"\n",(0,a.jsx)(t.h2,{id:"adapt-action",children:"Adapt Action"}),"\n",(0,a.jsxs)(t.p,{children:["We only have to make a minor change in ",(0,a.jsx)(t.code,{children:"departures.conf"})," file to Action ",(0,a.jsx)(t.code,{children:"download-deduplicate-departures"}),".\nAdding the executionMode ",(0,a.jsx)(t.code,{children:"DataObjectStateIncrementalMode"})," will enable DataObject incremental mode. With every run the input DataObject will only return new data in this mode.\nThe DataObject saves its state in the state file that is written after each run of the SDLB. You haven't worked with this state file before, more on that later."]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"  download-deduplicate-departures {\n    type = DeduplicateAction\n    inputId = ext-departures\n    outputId = int-departures\n    executionMode = { type = DataObjectStateIncrementalMode }\n    ...\n  }\n"})}),"\n",(0,a.jsx)(t.admonition,{title:"Execution Modes",type:"info",children:(0,a.jsxs)(t.p,{children:["An Execution Mode is a way to select which data needs to be processed.\nWith DataObjectStateIncrementalMode, we tell SDLB to load data based on the state managed by the DataObject itself.\nSee ",(0,a.jsx)(t.a,{href:"/docs/reference/executionModes",children:"Execution Modes"})," for detailed documentation."]})}),"\n",(0,a.jsx)(t.admonition,{type:"caution",children:(0,a.jsxs)(t.p,{children:["Due to load limits of the departures web service, the time interval in ",(0,a.jsx)(t.code,{children:"ext-departures"})," should not be larger than a week. As mentioned, we will implement a simple incremental query logic that always queries from the last execution time until the current execution.\nIf the time difference between the last execution and the current execution time is larger than a week, we will query the next four days since the last execution time. Otherwise, we query the data from the last execution until now."]})}),"\n",(0,a.jsx)(t.h2,{id:"define-state-variables",children:"Define state variables"}),"\n",(0,a.jsx)(t.p,{children:"To make use of the newly configured execution mode, we need state variables. Add the following two variables to our CustomWebserviceDataObject."}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-scala",children:"  private var previousState : Seq[State] = Seq()\n  private var nextState : Seq[State] = Seq()\n"})}),"\n",(0,a.jsxs)(t.p,{children:["The corresponding ",(0,a.jsx)(t.code,{children:"State"})," case class is already defined as"]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-scala",children:"  case class State(airport: String, nextBegin: Long)\n"})}),"\n",(0,a.jsxs)(t.p,{children:["It always stores the ",(0,a.jsx)(t.code,{children:"airport"})," and a ",(0,a.jsx)(t.code,{children:"nextBegin"})," as unix timestamp to indicate to the next run, what data needs to be loaded."]}),"\n",(0,a.jsxs)(t.p,{children:["Concerning the state variables, ",(0,a.jsx)(t.code,{children:"previousState"})," will basically be used for all the logic of the DataObject and ",(0,a.jsx)(t.code,{children:"nextState"})," will be used to store the state for the next run."]}),"\n",(0,a.jsx)(t.h2,{id:"read-and-write-state",children:"Read and write state"}),"\n",(0,a.jsxs)(t.p,{children:["To actually work with the state, we need to implement the ",(0,a.jsx)(t.code,{children:"CanCreateIncrementalOutput"})," trait.\nThis can be done by adding ",(0,a.jsx)(t.code,{children:"with CanCreateIncrementalOutput"})," to the ",(0,a.jsx)(t.code,{children:"CustomWebserviceDataObject"}),".\nConsequently, we need to implement the functions ",(0,a.jsx)(t.code,{children:"setState"})," and ",(0,a.jsx)(t.code,{children:"getState"})," defined in the trait."]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-scala",children:"  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {\n    implicit val formats: Formats = DefaultFormats\n    previousState = state.map(s => JsonMethods.parse(s).extract[Seq[State]]).getOrElse(Seq())\n  }\n  \n  override def getState: Option[String] = {\n    implicit val formats: Formats = DefaultFormats\n    Some(Serialization.write(nextState))\n  }\n"})}),"\n",(0,a.jsx)(t.p,{children:"We can see that by implementing these two functions, we start using the variables defined in the section above."}),"\n",(0,a.jsx)(t.p,{children:"To compile the following additional import statements are necessary at the top of the file:"}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-scala",children:"import io.smartdatalake.workflow.dataobject.CanCreateIncrementalOutput\nimport org.json4s.jackson.{JsonMethods, Serialization}\n"})}),"\n",(0,a.jsx)(t.h2,{id:"try-it-out",children:"Try it out"}),"\n",(0,a.jsxs)(t.p,{children:["We only spoke about this state, but it was never explained where it is stored.\nTo work with a state, we need to introduce two new command line parameters: ",(0,a.jsx)(t.code,{children:"--state-path"})," and ",(0,a.jsx)(t.code,{children:"--name"})," or ",(0,a.jsx)(t.code,{children:"-n"})," in short.\nThis allows us to define the folder and name of the state file.\nTo have access to the state file, we specify the path to be in an already mounted folder."]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{children:"./buildJob.sh\n./startJob.sh -c /mnt/config,/mnt/envConfig/dev.conf --feed-sel ids:download-deduplicate-departures --state-path /mnt/data/state -n getting-started\n"})}),"\n",(0,a.jsxs)(t.p,{children:["Use this slightly modified command to run ",(0,a.jsx)(t.code,{children:"download-deduplicate-departures"})," Action.\nNothing should have changed so far, since we only read and write an empty state.",(0,a.jsx)(t.br,{}),"\n","You can verify this by opening the file ",(0,a.jsx)(t.code,{children:"getting-started.<runId>.<attemptId>.json"})," and having a look at the field ",(0,a.jsx)(t.code,{children:"dataObjectsState"}),". The stored state is currently empty.\nIn the next section, we will assign a value to ",(0,a.jsx)(t.code,{children:"nextState"}),", such that the ",(0,a.jsx)(t.code,{children:"DataObject's"})," state is written."]}),"\n",(0,a.jsx)(t.admonition,{type:"info",children:(0,a.jsxs)(t.p,{children:["The same state file is used by SDLB to enable automatic recoveries of failed jobs.\nThis will also be explained in detail separately, but we're mentioning the fact here, so you can understand the two variables ",(0,a.jsx)(t.code,{children:"<runId>"})," and ",(0,a.jsx)(t.code,{children:"<attemptId>"})," appearing in the file name.\nFor each execution, the ",(0,a.jsx)(t.code,{children:"<runId>"})," is incremented by one.\nThe ",(0,a.jsx)(t.code,{children:"<attemptId>"})," is usually 1, but gets increased by one if SDLB has to recover a failed execution."]})}),"\n",(0,a.jsx)(t.h2,{id:"define-a-query-logic",children:"Define a Query Logic"}),"\n",(0,a.jsx)(t.p,{children:"Now we want to achieve the following query logic:"}),"\n",(0,a.jsxs)(t.p,{children:["The starting point are the query parameters provided in the configuration file and no previous state.\nDuring the first execution, we query the departures for the two airports in the given time window.\nIf no begin and end time are provided, we take the interval of [2 weeks and 2 days ago] -> [2 weeks ago] as a starting point.\nAfterward, the ",(0,a.jsx)(t.code,{children:"end"}),"-parameter of the current query will be stored as ",(0,a.jsx)(t.code,{children:"begin"}),"-parameter for the next query.\nNow the true incremental phase starts as we can get the state of the last successful run.\nWe query the flight-data API to get data from the last successful run up until now."]}),"\n",(0,a.jsxs)(t.p,{children:["For this to work, we need to make the following change to the ",(0,a.jsx)(t.code,{children:"currentQueryParameters"})," variable:"]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-scala",children:"      // if we have query parameters in the state, we will use them from now on\n      val currentQueryParameters = if (previousState.isEmpty) checkQueryParameters(queryParameters) else checkQueryParameters(previousState.map{\n        x => DepartureQueryParameters(x.airport, x.nextBegin, java.time.Instant.now.getEpochSecond)\n      })\n"})}),"\n",(0,a.jsxs)(t.p,{children:["Then the logic for the next state must be placed below the comment ",(0,a.jsx)(t.code,{children:"// put simple nextState logic below"}),"."]}),"\n",(0,a.jsx)(t.pre,{children:(0,a.jsx)(t.code,{className:"language-scala",children:"      nextState = currentQueryParameters.map(params => State(params.airport, params.end))\n"})}),"\n",(0,a.jsxs)(t.p,{children:["Compile and execute the code of this project again and execute it multiple times.\nThe scenario will be that the first run fetches the data defined in the configuration file, then the proceeding run retrieves the data from the endpoint of the last run until now.\nIf this time difference is larger than a week, the program only queries the next four days since the last execution.\nIf there is no data available in a time window, because only a few seconds have passed since the last execution, the execution will fail with Error ",(0,a.jsx)(t.strong,{children:"404"}),"."]}),"\n",(0,a.jsxs)(t.p,{children:["At the end the departure.conf file should look something like ",(0,a.jsx)(t.a,{href:"https://github.com/smart-data-lake/getting-started/tree/master/config/departures.conf.part-3-solution",children:"this"}),"\nand the CustomWebserviceDataObject code like ",(0,a.jsx)(t.a,{href:"https://github.com/smart-data-lake/getting-started/tree/master/src/main/scala/com/sample/CustomWebserviceDataObject.scala.part-3-solution",children:"this"}),"."]}),"\n",(0,a.jsx)(t.admonition,{type:"info",children:(0,a.jsxs)(t.p,{children:["Unfortunately, the webservice on opensky-network.org responds with a ",(0,a.jsx)(t.strong,{children:"404"})," error code when no data is available, rather than a ",(0,a.jsx)(t.strong,{children:"200"})," and an empty response.\nTherefore, SDLB gets a 404 and will fail the execution. The exception could be caught inside CustomWebserviceDataObject, but what if we have a real 404 error?!"]})}),"\n",(0,a.jsx)(t.h2,{id:"summary",children:"Summary"}),"\n",(0,a.jsx)(t.p,{children:"Congratulation, you just completed implementing a nice incremental loading mechanism!"}),"\n",(0,a.jsx)(t.p,{children:"That's it from getting-started for now. We hope you enjoyed your first steps with SDLB.\nFor further information, check the rest of the documentation and the blog on this page!"})]})}function h(e={}){const{wrapper:t}={...(0,i.R)(),...e.components};return t?(0,a.jsx)(t,{...e,children:(0,a.jsx)(c,{...e})}):c(e)}},8453:(e,t,n)=>{n.d(t,{R:()=>s,x:()=>o});var a=n(6540);const i={},r=a.createContext(i);function s(e){const t=a.useContext(r);return a.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function o(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(i):e.components||i:s(e.components),a.createElement(r.Provider,{value:t},e.children)}}}]);