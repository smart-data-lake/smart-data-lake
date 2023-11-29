"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[7029],{662:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>c,default:()=>p,frontMatter:()=>i,metadata:()=>d,toc:()=>u});var r=n(5893),a=n(1151),s=n(4866),o=n(5162);const i={id:"custom-webservice",title:"Custom Webservice"},c=void 0,d={id:"getting-started/part-3/custom-webservice",title:"Custom Webservice",description:"Goal",source:"@site/docs/getting-started/part-3/custom-webservice.md",sourceDirName:"getting-started/part-3",slug:"/getting-started/part-3/custom-webservice",permalink:"/docs/getting-started/part-3/custom-webservice",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-3/custom-webservice.md",tags:[],version:"current",frontMatter:{id:"custom-webservice",title:"Custom Webservice"},sidebar:"tutorialSidebar",previous:{title:"Keeping historical data",permalink:"/docs/getting-started/part-2/historical-data"},next:{title:"Incremental Mode",permalink:"/docs/getting-started/part-3/incremental-mode"}},l={},u=[{value:"Goal",id:"goal",level:2},{value:"Starting point",id:"starting-point",level:2},{value:"Define Data Objects",id:"define-data-objects",level:2},{value:"Define Action",id:"define-action",level:2},{value:"Try it out",id:"try-it-out",level:2},{value:"Get Data Frame",id:"get-data-frame",level:2},{value:"Preserve schema",id:"preserve-schema",level:2}];function h(e){const t={a:"a",admonition:"admonition",br:"br",code:"code",em:"em",h2:"h2",li:"li",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,a.a)(),...e.components};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(t.h2,{id:"goal",children:"Goal"}),"\n",(0,r.jsxs)(t.p,{children:["In the previous examples we worked mainly with data that was available as a file or could be fetched with the built-in ",(0,r.jsx)(t.code,{children:"WebserviceFileDataObject"}),".\nTo fetch data from a webservice, the ",(0,r.jsx)(t.code,{children:"WebserviceFileDataObject"})," is sometimes not enough and has to be customized.\nThe reasons why the built-in DataObject is not sufficient are manifold, but it's connected to the way Webservices are designed.\nWebservices often include design features like:"]}),"\n",(0,r.jsxs)(t.ul,{children:["\n",(0,r.jsx)(t.li,{children:"data pagination"}),"\n",(0,r.jsx)(t.li,{children:"protect resources using rate limiting"}),"\n",(0,r.jsx)(t.li,{children:"different authentication mechanisms"}),"\n",(0,r.jsx)(t.li,{children:"filters for incremental load"}),"\n",(0,r.jsx)(t.li,{children:"well defined schema"}),"\n",(0,r.jsx)(t.li,{children:"..."}),"\n"]}),"\n",(0,r.jsxs)(t.p,{children:["Smart Data Lake Builder can not cover all these various needs in a generic ",(0,r.jsx)(t.code,{children:"WebserviceDataObject"}),", which is why we have to write our own ",(0,r.jsx)(t.code,{children:"CustomWebserviceDataObject"}),".\nThe goal of this part is to learn how such a CustomWebserviceDataObject can be implemented in Scala."]}),"\n",(0,r.jsxs)(t.admonition,{type:"info",children:[(0,r.jsx)(t.p,{children:"Other than part 1 and 2, we are writing customized Scala classes in part 3 and making use of Apache Spark features.\nAs such, we expect you to have some Scala and Spark know-how to follow along."}),(0,r.jsxs)(t.p,{children:["It is also a good idea to configure a working development environment at this point.\nIn the ",(0,r.jsx)(t.a,{href:"/docs/getting-started/setup",children:"Technical Setup"})," chapter we briefly introduced how to use IntelliJ for development.\nThat should greatly improve your development experience compared to manipulating the file in a simple text editor."]})]}),"\n",(0,r.jsx)(t.h2,{id:"starting-point",children:"Starting point"}),"\n",(0,r.jsxs)(t.p,{children:["Again we start with the ",(0,r.jsx)(t.code,{children:"application.conf"})," that resulted from finishing the last part.\nIf you don't have the application.conf from part 2 anymore, please copy ",(0,r.jsx)(t.a,{target:"_blank",href:n(9140).Z+"",children:"this"})," configuration file to ",(0,r.jsx)(t.strong,{children:"config/application.conf"})," again."]}),"\n",(0,r.jsx)(t.h2,{id:"define-data-objects",children:"Define Data Objects"}),"\n",(0,r.jsxs)(t.p,{children:["We start by rewriting the existing ",(0,r.jsx)(t.code,{children:"ext-departures"})," DataObject.\nIn the configuration file, replace the old configuration with its new definition:"]}),"\n",(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{children:'ext-departures {\n  type = CustomWebserviceDataObject\n  schema = """array< struct< icao24: string, firstSeen: integer, estDepartureAirport: string, lastSeen: integer, estArrivalAirport: string, callsign: string, estDepartureAirportHorizDistance: integer, estDepartureAirportVertDistance: integer, estArrivalAirportHorizDistance: integer, estArrivalAirportVertDistance: integer, departureAirportCandidatesCount: integer, arrivalAirportCandidatesCount: integer >>"""\n  baseUrl = "https://opensky-network.org/api/flights/departure"\n  nRetry = 5\n  queryParameters = [{\n    airport = "LSZB"\n  },{\n    airport = "EDDF"\n  }]\n  timeouts {\n    connectionTimeoutMs = 200000\n    readTimeoutMs = 200000\n  }\n}\n'})}),"\n",(0,r.jsxs)(t.p,{children:["The Configuration for this new ",(0,r.jsx)(t.code,{children:"ext-departures"})," includes the type of the DataObject, the expected schema, the base url from where we can fetch the departures from, the number of retries, a list of query parameters and timeout options.\nTo have more flexibility, we can now configure the query parameters as options instead defining them in the query string.",(0,r.jsx)(t.br,{}),"\n","The connection timeout corresponds to the time we wait until the connection is established and the read timeout equals the time we wait until the webservice responds after the request has been submitted.\nIf the request cannot be answered in the times configured, we try to automatically resend the request.\nHow many times a failed request will be resend, is controlled by the ",(0,r.jsx)(t.code,{children:"nRetry"})," parameter."]}),"\n",(0,r.jsxs)(t.p,{children:["Note that we changed the type to ",(0,r.jsx)(t.code,{children:"CustomWebserviceDataObject"}),".\nThis is a custom DataObject type, not included in standard Smart Data Lake Builder.\nTo make it work, please go to the project's root directory and copy the Scala class with\n",(0,r.jsx)(t.code,{children:"cp part3/src/ ./ -r"}),":"]}),"\n",(0,r.jsx)(t.p,{children:"This copied the following file:"}),"\n",(0,r.jsxs)(t.ul,{children:["\n",(0,r.jsx)(t.li,{children:"./src/scala/io/smartdatalake/workflow/dataobject/CustomWebserviceDataObject.scala"}),"\n"]}),"\n",(0,r.jsxs)(t.admonition,{type:"info",children:[(0,r.jsxs)(t.p,{children:["The ",(0,r.jsx)(t.em,{children:"begin"})," and ",(0,r.jsx)(t.em,{children:"end"})," are now automatically set to two weeks ago minus 2 days and two weeks ago, respectively.\nThey can still be overridden if you want to try out fixed timestamps. For example, you could also write"]}),(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{children:'{\nairport = "LSZB"\n begin = 1696854853   # 29.08.2021\n end = 1697027653     # 30.08.2021\n}\n'})}),(0,r.jsxs)(t.p,{children:["However, as noted previously, when you request older data it may be that the webservice does not respond, so we recommend not to specify begin and end anymore.\nCan you spot which line of code in ",(0,r.jsx)(t.code,{children:"CustomWebserviceDataObject"})," is reponsible for setting the defaults?"]})]}),"\n",(0,r.jsxs)(t.p,{children:["In this part we will work exclusively on the ",(0,r.jsx)(t.code,{children:"CustomWebserviceDataObject.scala"})," file."]}),"\n",(0,r.jsx)(t.h2,{id:"define-action",children:"Define Action"}),"\n",(0,r.jsx)(t.p,{children:"In the configuration we only change one action again:"}),"\n",(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{children:"download-departures {\n  type = CopyAction\n  inputId = ext-departures\n  outputId = stg-departures\n  metadata {\n    feed = download\n  }\n}\n"})}),"\n",(0,r.jsxs)(t.p,{children:["The type is no longer ",(0,r.jsx)(t.code,{children:"FileTransferAction"})," but a ",(0,r.jsx)(t.code,{children:"CopyAction"})," instead, as our new ",(0,r.jsx)(t.code,{children:"CustomWebserviceDataObject"})," converts the Json-Output of the Webservice into a Spark DataFrame."]}),"\n",(0,r.jsx)(t.admonition,{type:"info",children:(0,r.jsxs)(t.p,{children:[(0,r.jsx)(t.code,{children:"FileTransferAction"}),"s are used, when your DataObject reads an InputStream or writes an OutputStream like ",(0,r.jsx)(t.code,{children:"WebserviceFileDataObject"})," or ",(0,r.jsx)(t.code,{children:"SFtpFileRefDataObject"}),".\nThese transfer files one-to-one from input to output.",(0,r.jsx)(t.br,{}),"\n","More often you work with one of the many provided ",(0,r.jsx)(t.code,{children:"SparkAction"}),"s like the ",(0,r.jsx)(t.code,{children:"CopyAction"})," shown here.\nThey work by using Spark Data Frames under the hood."]})}),"\n",(0,r.jsx)(t.h2,{id:"try-it-out",children:"Try it out"}),"\n",(0,r.jsxs)(t.p,{children:["Compile and execute the code of this project with the following commands.\nNote that parameter ",(0,r.jsx)(t.code,{children:"--feed-sel"})," only selects ",(0,r.jsx)(t.code,{children:"download-departures"})," as Action for execution."]}),"\n",(0,r.jsxs)(s.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],children:[(0,r.jsx)(o.Z,{value:"docker",children:(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{className:"language-jsx",children:'mkdir .mvnrepo\ndocker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\ndocker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel ids:download-departures\n'})})}),(0,r.jsx)(o.Z,{value:"podman",children:(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{className:"language-jsx",children:'mkdir .mvnrepo\npodman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\npodman run --rm --hostname=localhost --pod getting-started -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel ids:download-departures\n'})})})]}),"\n",(0,r.jsxs)(t.p,{children:["Nothing should have changed. You should again receive data as json files in the corresponding ",(0,r.jsx)(t.code,{children:"stg-departures"})," folder.\nBut except of receiving the departures for only one airport, the DataObject returns the departures for all configured airports.\nIn this specific case this would be ",(0,r.jsx)(t.em,{children:"LSZB"})," and ",(0,r.jsx)(t.em,{children:"EDDF"})," within the corresponding time window."]}),"\n",(0,r.jsx)(t.p,{children:"Having a look at the log, something similar should appear on your screen."}),"\n",(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{className:"language-Bash",children:"2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Prepare started\n2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:237 - Action~download-departures[CopyAction]: Prepare succeeded\n2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Init started\n2021-11-10 14:00:33 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1696854853&end=1697027653\n2021-11-10 14:00:33 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1696854853&end=1697027653\n2021-11-10 14:00:35 INFO  ActionDAGRun$ActionEventListener:237 - Action~download-departures[CopyAction]: Init succeeded\n2021-11-10 14:00:35 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Exec started\n2021-11-10 14:00:35 INFO  CopyAction:158 - (Action~download-departures) getting DataFrame for DataObject~ext-departures\n2021-11-10 14:00:36 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1696854853&end=1697027653\n2021-11-10 14:00:37 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1696854853&end=1697027653\n"})}),"\n",(0,r.jsxs)(t.p,{children:["It is important to notice that the two requests for each airport to the API were not send only once, but twice.\nThis stems from the fact that the method ",(0,r.jsx)(t.code,{children:"getSparkDataFrame"})," of the Data Object is called twice in the DAG execution of the Smart Data Lake Builder:\nOnce during the Init Phase and once again during the Exec Phase. See ",(0,r.jsx)(t.a,{href:"/docs/reference/executionPhases",children:"this page"})," for more information on that.\nBefore we address and mitigate this behaviour in the next section, let's have a look at the ",(0,r.jsx)(t.code,{children:"getSparkDataFrame"})," method and the currently implemented logic:"]}),"\n",(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{className:"language-Scala",children:'// use the queryParameters from the config\nval currentQueryParameters = checkQueryParameters(queryParameters)\n\n// given the query parameters, generate all requests\nval departureRequests = currentQueryParameters.map(\n  param => s"${baseUrl}?airport=${param.airport}&begin=${param.begin}&end=${param.end}"\n)\n// make requests\nval departuresResponses = departureRequests.map(request(_))\n// create dataframe with the correct schema and add created_at column with the current timestamp\nval departuresDf = departuresResponses.toDF("responseBinary")\n  .withColumn("responseString", byte2String($"responseBinary"))\n  .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))\n  .select(explode($"response").as("record"))\n  .select("record.*")\n  .withColumn("created_at", current_timestamp())\n// return\ndeparturesDf\n'})}),"\n",(0,r.jsxs)(t.p,{children:["Given the configured query parameters, the requests are first prepared using the request method.\nIf you have a look at the implementation of the ",(0,r.jsx)(t.code,{children:"request"})," method, you notice that we provide some ScalaJCustomWebserviceClient that is based on the ",(0,r.jsx)(t.em,{children:"ScalaJ"})," library.\nAlso in the ",(0,r.jsx)(t.code,{children:"request"})," method you can find the configuration for the number of retries.\nAfterwards, we create a data frame out of the response.\nWe implemented some transformations to flatten the result returned by the API.",(0,r.jsx)(t.br,{}),"\n","Spark has lots of ",(0,r.jsx)(t.em,{children:"Functions"})," that can be used out of the box.\nWe used such a column based function ",(0,r.jsx)(t.em,{children:"from_json"})," to parse the response string with the right schema.\nAt the end we return the freshly created data frame ",(0,r.jsx)(t.code,{children:"departuresDf"}),"."]}),"\n",(0,r.jsx)(t.admonition,{type:"tip",children:(0,r.jsxs)(t.p,{children:["The return type of the response is ",(0,r.jsx)(t.code,{children:"Array[Byte]"}),". To convert that to ",(0,r.jsx)(t.code,{children:"Array[String]"})," a ",(0,r.jsx)(t.em,{children:"User Defined Function"})," (also called ",(0,r.jsx)(t.em,{children:"UDF"}),") ",(0,r.jsx)(t.code,{children:"byte2String"})," has been used, which is declared inside the getSparkDataFrame method.\nThis function is a nice example of how to write your own ",(0,r.jsx)(t.em,{children:"UDF"}),"."]})}),"\n",(0,r.jsx)(t.h2,{id:"get-data-frame",children:"Get Data Frame"}),"\n",(0,r.jsxs)(t.p,{children:["In this section we will learn how we can avoid sending the request twice to the API using the execution phase information provided by the Smart Data Lake Builder.\nWe will now implement a simple ",(0,r.jsx)(t.em,{children:"if ... else"})," statement that allows us to return an empty data frame with the correct schema in the ",(0,r.jsx)(t.strong,{children:"Init"})," phase and to only query the data in the ",(0,r.jsx)(t.strong,{children:"Exec"})," phase.\nThis logic is implemented in the next code snipped and should replace the code currently enclosed between the two ",(0,r.jsx)(t.code,{children:"// REPLACE BLOCK"})," comments."]}),"\n",(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{className:"language-Scala",children:'    if(context.phase == ExecutionPhase.Init){\n  // simply return an empty data frame\n  Seq[String]().toDF("responseString")\n          .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))\n          .select(explode($"response").as("record"))\n          .select("record.*")\n          .withColumn("created_at", current_timestamp())\n} else {\n  // use the queryParameters from the config\n  val currentQueryParameters = checkQueryParameters(queryParameters)\n\n  // given the query parameters, generate all requests\n  val departureRequests = currentQueryParameters.map(\n    param => s"${baseUrl}?airport=${param.airport}&begin=${param.begin}&end=${param.end}"\n  )\n  // make requests\n  val departuresResponses = departureRequests.map(request(_))\n  // create dataframe with the correct schema and add created_at column with the current timestamp\n  val departuresDf = departuresResponses.toDF("responseBinary")\n          .withColumn("responseString", byte2String($"responseBinary"))\n          .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))\n          .select(explode($"response").as("record"))\n          .select("record.*")\n          .withColumn("created_at", current_timestamp())\n\n  // put simple nextState logic below\n\n  // return\n  departuresDf\n}\n\n'})}),"\n",(0,r.jsxs)(t.p,{children:["Note, in the ",(0,r.jsx)(t.em,{children:"Init"})," phase, the pre-defined ",(0,r.jsx)(t.strong,{children:"schema"})," (see the ",(0,r.jsx)(t.code,{children:"ext-departures"})," DataObject definition in the config) is used to create an empty/dummy json string, which then gets converted to the DataFrame."]}),"\n",(0,r.jsx)(t.p,{children:"Don't be confused about some comments in the code. They will be used in the next chapter.\nIf you re-compile the code of this project and then restart the program with the previous commands\nyou should see that we do not query the API twice anymore."}),"\n",(0,r.jsx)(t.admonition,{type:"tip",children:(0,r.jsxs)(t.p,{children:["Use the information of the ",(0,r.jsx)(t.code,{children:"ExecutionPhase"})," in your custom implementations whenever you need to have different logic during the different phases."]})}),"\n",(0,r.jsx)(t.h2,{id:"preserve-schema",children:"Preserve schema"}),"\n",(0,r.jsxs)(t.p,{children:["With this implementation, we still write the Spark data frame of our ",(0,r.jsx)(t.code,{children:"CustomWebserviceDataObject"})," in Json format.\nAs a consequence, we lose the schema definition when the data is read again.",(0,r.jsx)(t.br,{}),"\n","To improve this behaviour, let's directly use the ",(0,r.jsx)(t.code,{children:"ext-departures"})," as ",(0,r.jsx)(t.em,{children:"inputId"})," in the ",(0,r.jsx)(t.code,{children:"deduplicate-departures"})," action, and rename the Action as ",(0,r.jsx)(t.code,{children:"download-deduplicate-departures"}),".\nThe deduplicate action expects a DataFrame as input. Since our ",(0,r.jsx)(t.code,{children:"CustomWebserviceDataObject"})," delivers that, there is no need for an intermediate step anymore.",(0,r.jsx)(t.br,{}),"\n","After you've changed that, the first transformer has to be rewritten as well, since the input has changed.\nPlease replace it with the implementation below"]}),"\n",(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{children:"{\n  type = SQLDfTransformer\n  code = \"select ext_departures.*, date_format(from_unixtime(firstseen),'yyyyMMdd') dt from ext_departures\"\n}\n"})}),"\n",(0,r.jsxs)(t.p,{children:["The old Action ",(0,r.jsx)(t.code,{children:"download-departures"})," and the DataObject ",(0,r.jsx)(t.code,{children:"stg-departures"})," can be deleted, as it's not needed anymore."]}),"\n",(0,r.jsxs)(t.p,{children:["At the end, your config file should look something like ",(0,r.jsx)(t.a,{target:"_blank",href:n(9129).Z+"",children:"this"})," and the CustomWebserviceDataObject code like ",(0,r.jsx)(t.a,{target:"_blank",href:n(6933).Z+"",children:"this"}),".\nNote that since we changed the file format to delta lake, your new ",(0,r.jsx)(t.code,{children:"download-deduplicate-departures"})," feed now needs the metastore that you setup in part 2.\nTherefore, you need to make sure that polynote and the metastore are running as shown in ",(0,r.jsx)(t.a,{href:"../part-2/delta-lake-format",children:"the first step of part 2"}),".\nThen, you need to delete the files in the data folder and then run the following command in another terminal:"]}),"\n",(0,r.jsxs)(s.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],children:[(0,r.jsx)(o.Z,{value:"docker",children:(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{className:"language-jsx",children:"mkdir data\ndocker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network getting-started_default sdl-spark:latest --config /mnt/config --feed-sel ids:download-deduplicate-departures\n"})})}),(0,r.jsx)(o.Z,{value:"podman",children:(0,r.jsx)(t.pre,{children:(0,r.jsx)(t.code,{className:"language-jsx",children:"mkdir data\npodman run --rm --hostname=localhost -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --pod getting-started sdl-spark:latest --config /mnt/config --feed-sel ids:download-deduplicate-departures\n"})})})]})]})}function p(e={}){const{wrapper:t}={...(0,a.a)(),...e.components};return t?(0,r.jsx)(t,{...e,children:(0,r.jsx)(h,{...e})}):h(e)}},5162:(e,t,n)=>{n.d(t,{Z:()=>o});n(7294);var r=n(6010);const a={tabItem:"tabItem_Ymn6"};var s=n(5893);function o(e){let{children:t,hidden:n,className:o}=e;return(0,s.jsx)("div",{role:"tabpanel",className:(0,r.Z)(a.tabItem,o),hidden:n,children:t})}},4866:(e,t,n)=>{n.d(t,{Z:()=>y});var r=n(7294),a=n(6010),s=n(2466),o=n(6550),i=n(469),c=n(1980),d=n(7392),l=n(12);function u(e){return r.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,r.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)}))?.filter(Boolean)??[]}function h(e){const{values:t,children:n}=e;return(0,r.useMemo)((()=>{const e=t??function(e){return u(e).map((e=>{let{props:{value:t,label:n,attributes:r,default:a}}=e;return{value:t,label:n,attributes:r,default:a}}))}(n);return function(e){const t=(0,d.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,n])}function p(e){let{value:t,tabValues:n}=e;return n.some((e=>e.value===t))}function m(e){let{queryString:t=!1,groupId:n}=e;const a=(0,o.k6)(),s=function(e){let{queryString:t=!1,groupId:n}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!n)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return n??null}({queryString:t,groupId:n});return[(0,c._X)(s),(0,r.useCallback)((e=>{if(!s)return;const t=new URLSearchParams(a.location.search);t.set(s,e),a.replace({...a.location,search:t.toString()})}),[s,a])]}function f(e){const{defaultValue:t,queryString:n=!1,groupId:a}=e,s=h(e),[o,c]=(0,r.useState)((()=>function(e){let{defaultValue:t,tabValues:n}=e;if(0===n.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!p({value:t,tabValues:n}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${n.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const r=n.find((e=>e.default))??n[0];if(!r)throw new Error("Unexpected error: 0 tabValues");return r.value}({defaultValue:t,tabValues:s}))),[d,u]=m({queryString:n,groupId:a}),[f,g]=function(e){let{groupId:t}=e;const n=function(e){return e?`docusaurus.tab.${e}`:null}(t),[a,s]=(0,l.Nk)(n);return[a,(0,r.useCallback)((e=>{n&&s.set(e)}),[n,s])]}({groupId:a}),b=(()=>{const e=d??f;return p({value:e,tabValues:s})?e:null})();(0,i.Z)((()=>{b&&c(b)}),[b]);return{selectedValue:o,selectValue:(0,r.useCallback)((e=>{if(!p({value:e,tabValues:s}))throw new Error(`Can't select invalid tab value=${e}`);c(e),u(e),g(e)}),[u,g,s]),tabValues:s}}var g=n(2389);const b={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"};var x=n(5893);function j(e){let{className:t,block:n,selectedValue:r,selectValue:o,tabValues:i}=e;const c=[],{blockElementScrollPositionUntilNextRender:d}=(0,s.o5)(),l=e=>{const t=e.currentTarget,n=c.indexOf(t),a=i[n].value;a!==r&&(d(t),o(a))},u=e=>{let t=null;switch(e.key){case"Enter":l(e);break;case"ArrowRight":{const n=c.indexOf(e.currentTarget)+1;t=c[n]??c[0];break}case"ArrowLeft":{const n=c.indexOf(e.currentTarget)-1;t=c[n]??c[c.length-1];break}}t?.focus()};return(0,x.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,a.Z)("tabs",{"tabs--block":n},t),children:i.map((e=>{let{value:t,label:n,attributes:s}=e;return(0,x.jsx)("li",{role:"tab",tabIndex:r===t?0:-1,"aria-selected":r===t,ref:e=>c.push(e),onKeyDown:u,onClick:l,...s,className:(0,a.Z)("tabs__item",b.tabItem,s?.className,{"tabs__item--active":r===t}),children:n??t},t)}))})}function v(e){let{lazy:t,children:n,selectedValue:a}=e;const s=(Array.isArray(n)?n:[n]).filter(Boolean);if(t){const e=s.find((e=>e.props.value===a));return e?(0,r.cloneElement)(e,{className:"margin-top--md"}):null}return(0,x.jsx)("div",{className:"margin-top--md",children:s.map(((e,t)=>(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==a})))})}function w(e){const t=f(e);return(0,x.jsxs)("div",{className:(0,a.Z)("tabs-container",b.tabList),children:[(0,x.jsx)(j,{...e,...t}),(0,x.jsx)(v,{...e,...t})]})}function y(e){const t=(0,g.Z)();return(0,x.jsx)(w,{...e,children:u(e.children)},String(t))}},6933:(e,t,n)=>{n.d(t,{Z:()=>r});const r=n.p+"assets/files/CustomWebserviceDataObject-1-1063c2b23f80b73d53c89f4e84e25164.scala"},9140:(e,t,n)=>{n.d(t,{Z:()=>r});const r=n.p+"assets/files/application-part2-historical-7a2ca88bc6104da84d43497b32a9718a.conf"},9129:(e,t,n)=>{n.d(t,{Z:()=>r});const r=n.p+"assets/files/application-part3-download-custom-webservice-64d86845e182e0b959e4b61395b5f169.conf"},1151:(e,t,n)=>{n.d(t,{Z:()=>i,a:()=>o});var r=n(7294);const a={},s=r.createContext(a);function o(e){const t=r.useContext(s);return r.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function i(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(a):e.components||a:o(e.components),r.createElement(s.Provider,{value:t},e.children)}}}]);