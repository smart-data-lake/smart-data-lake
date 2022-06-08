"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[7029],{3905:function(e,t,n){n.d(t,{Zo:function(){return d},kt:function(){return u}});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),p=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},d=function(e){var t=p(e.components);return a.createElement(l.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,l=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),m=p(n),u=r,h=m["".concat(l,".").concat(u)]||m[u]||c[u]||i;return n?a.createElement(h,o(o({ref:t},d),{},{components:n})):a.createElement(h,o({ref:t},d))}));function u(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:r,o[1]=s;for(var p=2;p<i;p++)o[p]=n[p];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},8215:function(e,t,n){var a=n(7294);t.Z=function(e){var t=e.children,n=e.hidden,r=e.className;return a.createElement("div",{role:"tabpanel",hidden:n,className:r},t)}},9877:function(e,t,n){n.d(t,{Z:function(){return d}});var a=n(7462),r=n(7294),i=n(2389),o=n(9548),s=n(6010),l="tabItem_LplD";function p(e){var t,n,i,p=e.lazy,d=e.block,c=e.defaultValue,m=e.values,u=e.groupId,h=e.className,f=r.Children.map(e.children,(function(e){if((0,r.isValidElement)(e)&&void 0!==e.props.value)return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})),v=null!=m?m:f.map((function(e){var t=e.props;return{value:t.value,label:t.label,attributes:t.attributes}})),g=(0,o.lx)(v,(function(e,t){return e.value===t.value}));if(g.length>0)throw new Error('Docusaurus error: Duplicate values "'+g.map((function(e){return e.value})).join(", ")+'" found in <Tabs>. Every value needs to be unique.');var k=null===c?c:null!=(t=null!=c?c:null==(n=f.find((function(e){return e.props.default})))?void 0:n.props.value)?t:null==(i=f[0])?void 0:i.props.value;if(null!==k&&!v.some((function(e){return e.value===k})))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+k+'" but none of its children has the corresponding value. Available values are: '+v.map((function(e){return e.value})).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");var b=(0,o.UB)(),w=b.tabGroupChoices,y=b.setTabGroupChoices,N=(0,r.useState)(k),D=N[0],C=N[1],x=[],O=(0,o.o5)().blockElementScrollPositionUntilNextRender;if(null!=u){var j=w[u];null!=j&&j!==D&&v.some((function(e){return e.value===j}))&&C(j)}var A=function(e){var t=e.currentTarget,n=x.indexOf(t),a=v[n].value;a!==D&&(O(t),C(a),null!=u&&y(u,a))},T=function(e){var t,n=null;switch(e.key){case"ArrowRight":var a=x.indexOf(e.currentTarget)+1;n=x[a]||x[0];break;case"ArrowLeft":var r=x.indexOf(e.currentTarget)-1;n=x[r]||x[x.length-1]}null==(t=n)||t.focus()};return r.createElement("div",{className:"tabs-container"},r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,s.Z)("tabs",{"tabs--block":d},h)},v.map((function(e){var t=e.value,n=e.label,i=e.attributes;return r.createElement("li",(0,a.Z)({role:"tab",tabIndex:D===t?0:-1,"aria-selected":D===t,key:t,ref:function(e){return x.push(e)},onKeyDown:T,onFocus:A,onClick:A},i,{className:(0,s.Z)("tabs__item",l,null==i?void 0:i.className,{"tabs__item--active":D===t})}),null!=n?n:t)}))),p?(0,r.cloneElement)(f.filter((function(e){return e.props.value===D}))[0],{className:"margin-vert--md"}):r.createElement("div",{className:"margin-vert--md"},f.map((function(e,t){return(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==D})}))))}function d(e){var t=(0,i.Z)();return r.createElement(p,(0,a.Z)({key:String(t)},e))}},1623:function(e,t,n){n.r(t),n.d(t,{contentTitle:function(){return d},default:function(){return h},frontMatter:function(){return p},metadata:function(){return c},toc:function(){return m}});var a=n(7462),r=n(3366),i=(n(7294),n(3905)),o=n(9877),s=n(8215),l=["components"],p={id:"custom-webservice",title:"Custom Webservice"},d=void 0,c={unversionedId:"getting-started/part-3/custom-webservice",id:"getting-started/part-3/custom-webservice",title:"Custom Webservice",description:"Goal",source:"@site/docs/getting-started/part-3/custom-webservice.md",sourceDirName:"getting-started/part-3",slug:"/getting-started/part-3/custom-webservice",permalink:"/docs/getting-started/part-3/custom-webservice",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-3/custom-webservice.md",tags:[],version:"current",frontMatter:{id:"custom-webservice",title:"Custom Webservice"},sidebar:"docs",previous:{title:"Keeping historical data",permalink:"/docs/getting-started/part-2/historical-data"},next:{title:"Incremental Mode",permalink:"/docs/getting-started/part-3/incremental-mode"}},m=[{value:"Goal",id:"goal",children:[],level:2},{value:"Starting point",id:"starting-point",children:[],level:2},{value:"Define Data Objects",id:"define-data-objects",children:[],level:2},{value:"Define Action",id:"define-action",children:[],level:2},{value:"Try it out",id:"try-it-out",children:[],level:2},{value:"Get Data Frame",id:"get-data-frame",children:[],level:2},{value:"Preserve schema",id:"preserve-schema",children:[],level:2}],u={toc:m};function h(e){var t=e.components,p=(0,r.Z)(e,l);return(0,i.kt)("wrapper",(0,a.Z)({},u,p,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h2",{id:"goal"},"Goal"),(0,i.kt)("p",null,"  In the previous examples we worked mainly with data that was available as a file or could be fetched with the built-in ",(0,i.kt)("inlineCode",{parentName:"p"},"WebserviceFileDataObject"),".\nTo fetch data from a webservice, the ",(0,i.kt)("inlineCode",{parentName:"p"},"WebserviceFileDataObject")," is sometimes not enough and has to be customized.\nThe reasons why the built-in DataObject is not sufficient are manifold, but it's connected to the way Webservices are designed.\nWebservices often include design features like: "),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"data pagination"),(0,i.kt)("li",{parentName:"ul"},"protect resources using rate limiting "),(0,i.kt)("li",{parentName:"ul"},"different authentication mechanisms"),(0,i.kt)("li",{parentName:"ul"},"filters for incremental load"),(0,i.kt)("li",{parentName:"ul"},"well defined schema"),(0,i.kt)("li",{parentName:"ul"},"...")),(0,i.kt)("p",null,"Smart Data Lake Builder can not cover all these various needs in a generic ",(0,i.kt)("inlineCode",{parentName:"p"},"WebserviceDataObject"),", which is why we have to write our own ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject"),".\nThe goal of this part is to learn how such a CustomWebserviceDataObject can be implemented in Scala."),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"Other than part 1 and 2, we are writing customized Scala classes in part 3 and making use of Apache Spark features.\nAs such, we expect you to have some Scala and Spark know-how to follow along."),(0,i.kt)("p",{parentName:"div"},"It is also a good idea to configure a working development environment at this point.\nIn the ",(0,i.kt)("a",{parentName:"p",href:"/docs/getting-started/setup"},"Technical Setup")," chapter we briefly introduced how to use IntelliJ for development.\nThat should greatly improve your development experience compared to manipulating the file in a simple text editor."))),(0,i.kt)("h2",{id:"starting-point"},"Starting point"),(0,i.kt)("p",null,"Again we start with the ",(0,i.kt)("inlineCode",{parentName:"p"},"application.conf")," that resulted from finishing the last part.\nIf you don't have the application.conf from part 2 anymore, please copy ",(0,i.kt)("a",{target:"_blank",href:n(4336).Z},"this")," configuration file to ",(0,i.kt)("strong",{parentName:"p"},"config/application.conf")," again."),(0,i.kt)("h2",{id:"define-data-objects"},"Define Data Objects"),(0,i.kt)("p",null,"We start by rewriting the existing ",(0,i.kt)("inlineCode",{parentName:"p"},"ext-departures")," DataObject.\nIn the configuration file, replace the old configuration with its new definition:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'ext-departures {\n  type = CustomWebserviceDataObject\n  schema = """array< struct< icao24: string, firstSeen: integer, estDepartureAirport: string, lastSeen: integer, estArrivalAirport: string, callsign: string, estDepartureAirportHorizDistance: integer, estDepartureAirportVertDistance: integer, estArrivalAirportHorizDistance: integer, estArrivalAirportVertDistance: integer, departureAirportCandidatesCount: integer, arrivalAirportCandidatesCount: integer >>"""\n  baseUrl = "https://opensky-network.org/api/flights/departure"\n  nRetry = 5\n  queryParameters = [{\n    airport = "LSZB"\n    begin = 1641393602\n    end = 1641483739\n  },{\n    airport = "EDDF"\n    begin = 1641393602\n    end = 1641483739\n  }]\n  timeouts {\n    connectionTimeoutMs = 200000\n    readTimeoutMs = 200000\n  }\n}\n')),(0,i.kt)("p",null,"The Configuration for this new ",(0,i.kt)("inlineCode",{parentName:"p"},"ext-departures")," includes the type of the DataObject, the expected schema, the base url from where we can fetch the departures from, the number of retries, a list of query parameters and timeout options.\nTo have more flexibility, we can now configure the query parameters as options instead defining them in the query string.",(0,i.kt)("br",{parentName:"p"}),"\n","The connection timeout corresponds to the time we wait until the connection is established and the read timeout equals the time we wait until the webservice responds after the request has been submitted.\nIf the request cannot be answered in the times configured, we try to automatically resend the request.\nHow many times a failed request will be resend, is controlled by the ",(0,i.kt)("inlineCode",{parentName:"p"},"nRetry")," parameter."),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"The ",(0,i.kt)("em",{parentName:"p"},"begin")," and ",(0,i.kt)("em",{parentName:"p"},"end")," can now be configured for each airport separatly.\nThe configuration expects unix timestamps, if you don't know what that means, have a look at this ",(0,i.kt)("a",{parentName:"p",href:"https://www.unixtimestamp.com/"},"website"),".\nThe webservice from opensky-network.org will not respond if the interval is larger than a week.\nHence, our ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject")," will enforce the rule that if the chosen interval is larger, we query only the next four days given the ",(0,i.kt)("em",{parentName:"p"},"begin")," configuration."))),(0,i.kt)("p",null,"Note that we changed the type to ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject"),".\nThis is a custom DataObject type, not included in standard Smart Data Lake Builder.\nTo make it work please go to the project's root directory and ",(0,i.kt)("inlineCode",{parentName:"p"},"unzip part3.additional-files.zip")," into the project's root folder.\nIt includes the following file for you:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"./src/scala/io/smartdatalake/workflow/dataobject/CustomWebserviceDataObject.scala")),(0,i.kt)("p",null,"In this part we will work exclusively on the ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject.scala")," file."),(0,i.kt)("h2",{id:"define-action"},"Define Action"),(0,i.kt)("p",null,"In the configuration we only change one action again:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"download-departures {\n  type = CopyAction\n  inputId = ext-departures\n  outputId = stg-departures\n  metadata {\n    feed = download\n  }\n}\n")),(0,i.kt)("p",null,"The type is no longer ",(0,i.kt)("inlineCode",{parentName:"p"},"FileTransferAction")," but a ",(0,i.kt)("inlineCode",{parentName:"p"},"CopyAction")," instead, as our new ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject")," converts the Json-Output of the Webservice into a Spark DataFrame."),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},(0,i.kt)("inlineCode",{parentName:"p"},"FileTransferAction"),"s are used, when your DataObjects reads an InputStream or writes an OutputStream like ",(0,i.kt)("inlineCode",{parentName:"p"},"WebserviceFileDataObject")," or ",(0,i.kt)("inlineCode",{parentName:"p"},"SFtpFileRefDataObject"),".\nThese transfer files one-to-one from input to output.",(0,i.kt)("br",{parentName:"p"}),"\n","More often you work with one of the many provided ",(0,i.kt)("inlineCode",{parentName:"p"},"SparkAction"),"s like the ",(0,i.kt)("inlineCode",{parentName:"p"},"CopyAction")," shown here.\nThey work by using Spark Data Frames under the hood. "))),(0,i.kt)("h2",{id:"try-it-out"},"Try it out"),(0,i.kt)("p",null,"Compile and execute the code of this project with the following commands.\nNote that parameter ",(0,i.kt)("inlineCode",{parentName:"p"},"--feed-sel")," only selects ",(0,i.kt)("inlineCode",{parentName:"p"},"download-departures")," as Action for execution. "),(0,i.kt)(o.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,i.kt)(s.Z,{value:"docker",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-jsx"},'mkdir .mvnrepo\ndocker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\ndocker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel ids:download-departures\n'))),(0,i.kt)(s.Z,{value:"podman",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-jsx"},'mkdir .mvnrepo\npodman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\npodman run --rm --hostname=localhost --pod getting-started -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel ids:download-departures\n')))),(0,i.kt)("p",null,"Nothing should have changed. You should again receive data as json files in the corresponding ",(0,i.kt)("inlineCode",{parentName:"p"},"stg-departures")," folder.\nBut except of receiving the departures for only one airport, the DataObject returns the departures for all configured airports.\nIn this specific case this would be ",(0,i.kt)("em",{parentName:"p"},"LSZB")," and ",(0,i.kt)("em",{parentName:"p"},"EDDF")," within the corresponding time window."),(0,i.kt)("p",null,"Having a look at the log, something similar should appear on your screen. "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-Bash"},"2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Prepare started\n2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:237 - Action~download-departures[CopyAction]: Prepare succeeded\n2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Init started\n2021-11-10 14:00:33 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1630200800&end=1630310979\n2021-11-10 14:00:33 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1630200800&end=1630310979\n2021-11-10 14:00:35 INFO  ActionDAGRun$ActionEventListener:237 - Action~download-departures[CopyAction]: Init succeeded\n2021-11-10 14:00:35 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Exec started\n2021-11-10 14:00:35 INFO  CopyAction:158 - (Action~download-departures) getting DataFrame for DataObject~ext-departures\n2021-11-10 14:00:36 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1630200800&end=1630310979\n2021-11-10 14:00:37 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1630200800&end=1630310979\n")),(0,i.kt)("p",null,"It is important to notice that the two requests for each airport to the API were not send only once, but twice.\nThis stems from the fact that the method ",(0,i.kt)("inlineCode",{parentName:"p"},"getDataFrame")," of the Data Object is called twice in the DAG execution of the Smart Data Lake Builder:\nOnce during the Init Phase and once again during the Exec Phase. See ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/executionPhases"},"this page")," for more information on that.\nBefore we address and mitigate this behaviour in the next section, let's have a look at the ",(0,i.kt)("inlineCode",{parentName:"p"},"getDataFrame")," method and the currently implemented logic:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-Scala"},'// use the queryParameters from the config\nval currentQueryParameters = checkQueryParameters(queryParameters)\n\n// given the query parameters, generate all requests\nval departureRequests = currentQueryParameters.map(\n  param => s"${baseUrl}?airport=${param.airport}&begin=${param.begin}&end=${param.end}"\n)\n// make requests\nval departuresResponses = departureRequests.map(request(_))\n// create dataframe with the correct schema and add created_at column with the current timestamp\nval departuresDf = departuresResponses.toDF("responseBinary")\n  .withColumn("responseString", byte2String($"responseBinary"))\n  .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))\n  .select(explode($"response").as("record"))\n  .select("record.*")\n  .withColumn("created_at", current_timestamp())\n// return\ndeparturesDf\n')),(0,i.kt)("p",null,"Given the configured query parameters, the requests are first prepared using the request method.\nIf you have a look at the implementation of the ",(0,i.kt)("inlineCode",{parentName:"p"},"request")," method, you notice that we provide some ScalaJCustomWebserviceClient that is based on the ",(0,i.kt)("em",{parentName:"p"},"ScalaJ")," library.\nAlso in the ",(0,i.kt)("inlineCode",{parentName:"p"},"request")," method you can find the configuration for the number of retries.\nAfterwards, we create a data frame out of the response.\nWe implemented some transformations to flatten the result returned by the API.",(0,i.kt)("br",{parentName:"p"}),"\n","Spark has lots of ",(0,i.kt)("em",{parentName:"p"},"Functions")," that can be used out of the box.\nWe used such a column based function ",(0,i.kt)("em",{parentName:"p"},"from_json")," to parse the response string with the right schema.\nAt the end we return the freshly created data frame ",(0,i.kt)("inlineCode",{parentName:"p"},"departuresDf"),"."),(0,i.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"The return type of the response is ",(0,i.kt)("inlineCode",{parentName:"p"},"Array[Byte]"),". To convert that to ",(0,i.kt)("inlineCode",{parentName:"p"},"Array[String]")," a ",(0,i.kt)("em",{parentName:"p"},"User Defined Function")," (also called ",(0,i.kt)("em",{parentName:"p"},"UDF"),") ",(0,i.kt)("inlineCode",{parentName:"p"},"byte2String")," has been used, which is declared inside the getDataFrame method.\nThis function is a nice example of how to write your own ",(0,i.kt)("em",{parentName:"p"},"UDF"),"."))),(0,i.kt)("h2",{id:"get-data-frame"},"Get Data Frame"),(0,i.kt)("p",null,"In this section we will learn how we can avoid sending the request twice to the API using the execution phase information provided by the Smart Data Lake Builder.\nWe will now implement a simple ",(0,i.kt)("em",{parentName:"p"},"if ... else")," statement that allows us to return an empty data frame with the correct schema in the ",(0,i.kt)("strong",{parentName:"p"},"Init")," phase and to only query the data in the ",(0,i.kt)("strong",{parentName:"p"},"Exec")," phase.\nThis logic is implemented in the next code snipped and should replace the code currently enclosed between the two ",(0,i.kt)("inlineCode",{parentName:"p"},"// REPLACE BLOCK")," comments."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-Scala"},'    if(context.phase == ExecutionPhase.Init){\n  // simply return an empty data frame\n  Seq[String]().toDF("responseString")\n          .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))\n          .select(explode($"response").as("record"))\n          .select("record.*")\n          .withColumn("created_at", current_timestamp())\n} else {\n  // use the queryParameters from the config\n  val currentQueryParameters = checkQueryParameters(queryParameters)\n\n  // given the query parameters, generate all requests\n  val departureRequests = currentQueryParameters.map(\n    param => s"${baseUrl}?airport=${param.airport}&begin=${param.begin}&end=${param.end}"\n  )\n  // make requests\n  val departuresResponses = departureRequests.map(request(_))\n  // create dataframe with the correct schema and add created_at column with the current timestamp\n  val departuresDf = departuresResponses.toDF("responseBinary")\n          .withColumn("responseString", byte2String($"responseBinary"))\n          .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))\n          .select(explode($"response").as("record"))\n          .select("record.*")\n          .withColumn("created_at", current_timestamp())\n\n  // put simple nextState logic below\n\n  // return\n  departuresDf\n}\n\n')),(0,i.kt)("p",null,"Note, in the ",(0,i.kt)("em",{parentName:"p"},"Init")," phase, the pre-defined ",(0,i.kt)("strong",{parentName:"p"},"schema")," (see the ",(0,i.kt)("inlineCode",{parentName:"p"},"ext-departures")," dataObject definition in the config) is used to create an empty/dummy json string, which then gets converted to the DataFrame."),(0,i.kt)("p",null,"Don't be confused about some comments in the code. They will be used in the next chapter.\nIf you re-compile the code of this project and then restart the program with the previous commands\nyou should see that we do not query the API twice anymore."),(0,i.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"  Use the information of the ",(0,i.kt)("inlineCode",{parentName:"p"},"ExecutionPhase")," in your custom implementations whenever you need to have different logic during the different phases."))),(0,i.kt)("h2",{id:"preserve-schema"},"Preserve schema"),(0,i.kt)("p",null,"With this implementation, we still write the Spark data frame of our ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject")," in Json format.\nAs a consequence, we lose the schema definition when the data is read again.",(0,i.kt)("br",{parentName:"p"}),"\n","To improve this behaviour, let's directly use the ",(0,i.kt)("inlineCode",{parentName:"p"},"ext-departures")," as ",(0,i.kt)("em",{parentName:"p"},"inputId")," in the ",(0,i.kt)("inlineCode",{parentName:"p"},"deduplicate-departures")," action, and rename the Action as ",(0,i.kt)("inlineCode",{parentName:"p"},"download-deduplicate-departures"),".\nThe deduplicate action expects a DataFrame as input. Since our ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject")," delivers that, there is no need for an intermediate step anymore.",(0,i.kt)("br",{parentName:"p"}),"\n","After you've changed that, the first transformer has to be rewritten as well, since the input has changed.\nPlease replace it with the implementation below"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"{\n  type = SQLDfTransformer\n  code = \"select ext_departures.*, date_format(from_unixtime(firstseen),'yyyyMMdd') dt from ext_departures\"\n}\n")),(0,i.kt)("p",null,"The old Action ",(0,i.kt)("inlineCode",{parentName:"p"},"download-departures")," and the DataObject ",(0,i.kt)("inlineCode",{parentName:"p"},"stg-departures")," can be deleted, as it's not needed anymore."),(0,i.kt)("p",null,"At the end, your config file should look something like ",(0,i.kt)("a",{target:"_blank",href:n(8528).Z},"this")," and the CustomWebserviceDataObject code like ",(0,i.kt)("a",{target:"_blank",href:n(1262).Z},"this"),".\nNote that since we changed the file format to delta lake, your new ",(0,i.kt)("inlineCode",{parentName:"p"},"download-deduplicate-departures")," feed now needs the metastore that you setup in part 2.\nTherefore, you need to make sure that polynote and the metastore are running as shown in ",(0,i.kt)("a",{parentName:"p",href:"../part-2/delta-lake-format"},"the first step of part 2"),".\nThen, you need to delete the files in the data folder and then run the following command in another terminal: "),(0,i.kt)(o.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,i.kt)(s.Z,{value:"docker",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-jsx"},"mkdir -f data\ndocker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network getting-started_default sdl-spark:latest --config /mnt/config --feed-sel ids:download-deduplicate-departures\n"))),(0,i.kt)(s.Z,{value:"podman",mdxType:"TabItem"},(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-jsx"},"mkdir -f data\npodman run --rm --hostname=localhost -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --pod getting-started sdl-spark:latest --config /mnt/config --feed-sel ids:download-deduplicate-departures\n")))))}h.isMDXComponent=!0},1262:function(e,t,n){t.Z=n.p+"assets/files/CustomWebserviceDataObject-1-b9264f59c390d53941ed1f8d9e0cf2fc.scala"},4336:function(e,t,n){t.Z=n.p+"assets/files/application-part2-historical-62b47cb172772c79ba8f08c7082ca360.conf"},8528:function(e,t,n){t.Z=n.p+"assets/files/application-part3-download-custom-webservice-8128471803017a347edd923edbae995f.conf"}}]);