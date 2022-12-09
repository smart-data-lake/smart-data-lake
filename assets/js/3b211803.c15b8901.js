"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[6687],{3905:(e,t,a)=>{a.d(t,{Zo:()=>p,kt:()=>h});var n=a(7294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function i(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?i(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):i(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},i=Object.keys(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)a=i[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),d=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},p=function(e){var t=d(e.components);return n.createElement(s.Provider,{value:t},e.children)},u="mdxType",c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,i=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),u=d(a),m=r,h=u["".concat(s,".").concat(m)]||u[m]||c[m]||i;return a?n.createElement(h,o(o({ref:t},p),{},{components:a})):n.createElement(h,o({ref:t},p))}));function h(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=a.length,o=new Array(i);o[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[u]="string"==typeof e?e:r,o[1]=l;for(var d=2;d<i;d++)o[d]=a[d];return n.createElement.apply(null,o)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},5162:(e,t,a)=>{a.d(t,{Z:()=>o});var n=a(7294),r=a(6010);const i="tabItem_Ymn6";function o(e){let{children:t,hidden:a,className:o}=e;return n.createElement("div",{role:"tabpanel",className:(0,r.Z)(i,o),hidden:a},t)}},5488:(e,t,a)=>{a.d(t,{Z:()=>m});var n=a(7462),r=a(7294),i=a(6010),o=a(2389),l=a(7392),s=a(7094),d=a(2466);const p="tabList__CuJ",u="tabItem_LNqP";function c(e){const{lazy:t,block:a,defaultValue:o,values:c,groupId:m,className:h}=e,f=r.Children.map(e.children,(e=>{if((0,r.isValidElement)(e)&&"value"in e.props)return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)})),k=c??f.map((e=>{let{props:{value:t,label:a,attributes:n}}=e;return{value:t,label:a,attributes:n}})),g=(0,l.l)(k,((e,t)=>e.value===t.value));if(g.length>0)throw new Error(`Docusaurus error: Duplicate values "${g.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`);const b=null===o?o:o??f.find((e=>e.props.default))?.props.value??f[0].props.value;if(null!==b&&!k.some((e=>e.value===b)))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${b}" but none of its children has the corresponding value. Available values are: ${k.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);const{tabGroupChoices:y,setTabGroupChoices:v}=(0,s.U)(),[w,N]=(0,r.useState)(b),D=[],{blockElementScrollPositionUntilNextRender:T}=(0,d.o5)();if(null!=m){const e=y[m];null!=e&&e!==w&&k.some((t=>t.value===e))&&N(e)}const _=e=>{const t=e.currentTarget,a=D.indexOf(t),n=k[a].value;n!==w&&(T(t),N(n),null!=m&&v(m,String(n)))},C=e=>{let t=null;switch(e.key){case"ArrowRight":{const a=D.indexOf(e.currentTarget)+1;t=D[a]??D[0];break}case"ArrowLeft":{const a=D.indexOf(e.currentTarget)-1;t=D[a]??D[D.length-1];break}}t?.focus()};return r.createElement("div",{className:(0,i.Z)("tabs-container",p)},r.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,i.Z)("tabs",{"tabs--block":a},h)},k.map((e=>{let{value:t,label:a,attributes:o}=e;return r.createElement("li",(0,n.Z)({role:"tab",tabIndex:w===t?0:-1,"aria-selected":w===t,key:t,ref:e=>D.push(e),onKeyDown:C,onFocus:_,onClick:_},o,{className:(0,i.Z)("tabs__item",u,o?.className,{"tabs__item--active":w===t})}),a??t)}))),t?(0,r.cloneElement)(f.filter((e=>e.props.value===w))[0],{className:"margin-top--md"}):r.createElement("div",{className:"margin-top--md"},f.map(((e,t)=>(0,r.cloneElement)(e,{key:t,hidden:e.props.value!==w})))))}function m(e){const t=(0,o.Z)();return r.createElement(c,(0,n.Z)({key:String(t)},e))}},2549:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>p,contentTitle:()=>s,default:()=>m,frontMatter:()=>l,metadata:()=>d,toc:()=>u});var n=a(7462),r=(a(7294),a(3905)),i=a(5488),o=a(5162);const l={title:"Keeping historical data"},s=void 0,d={unversionedId:"getting-started/part-2/historical-data",id:"getting-started/part-2/historical-data",title:"Keeping historical data",description:"Goal",source:"@site/docs/getting-started/part-2/historical-data.md",sourceDirName:"getting-started/part-2",slug:"/getting-started/part-2/historical-data",permalink:"/docs/getting-started/part-2/historical-data",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-2/historical-data.md",tags:[],version:"current",frontMatter:{title:"Keeping historical data"},sidebar:"docs",previous:{title:"Delta Lake - a better data format",permalink:"/docs/getting-started/part-2/delta-lake-format"},next:{title:"Custom Webservice",permalink:"/docs/getting-started/part-3/custom-webservice"}},p={},u=[{value:"Goal",id:"goal",level:2},{value:"Requirements",id:"requirements",level:2},{value:"Historization of airport data",id:"historization-of-airport-data",level:2},{value:"Deduplication of flight data",id:"deduplication-of-flight-data",level:2},{value:"Summary",id:"summary",level:2}],c={toc:u};function m(e){let{components:t,...l}=e;return(0,r.kt)("wrapper",(0,n.Z)({},c,l,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h2",{id:"goal"},"Goal"),(0,r.kt)("p",null,"Data generally can be split into two groups:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Master data:",(0,r.kt)("br",{parentName:"li"}),"data about objects that evolve over time, e.g. an airport, a person, a product... "),(0,r.kt)("li",{parentName:"ul"},"Transactional data:",(0,r.kt)("br",{parentName:"li"}),"data about events that took place at a certain point in time, e.g. a flight, a payment... ")),(0,r.kt)("p",null,"To keep historical data for both these categories, different strategies are applied:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"Master data")," is most often ",(0,r.kt)("strong",{parentName:"li"},"historized"),' - this means tracking the evolution of objects over time by introducing a time dimension.\nUsually this is modelled with two additional attributes "valid_from" and "valid_to", where "valid_from" is an additional primary key column.'),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"Transactional data")," is usually ",(0,r.kt)("strong",{parentName:"li"},"deduplicated"),", as only the latest state of a specific event is of interest. If an update for an event occurs, the previous information is discarded (or consolidated in special cases).\nAdditional care must be taken to keep all historical events, even if they are no longer present in the source system. Often specific housekeeping rules are applied (e.g. retention period), either for legal or cost saving reasons.")),(0,r.kt)("h2",{id:"requirements"},"Requirements"),(0,r.kt)("p",null,"For Historization and Deduplication a data pipeline needs to read the state of the output DataObject, merge it with the new state of the input DataObject and write the result to the output DataObject.\nTo read and write the same DataObject in the same SDL Action, this must be a transactional DataObject.\nIt means the DataObject must implement the interface TransactionalSparkTableDataObject of SDL.\nLuckily in the previous chapter we already upgraded our data pipeline to use DeltaLakeTableDataObject, which is a TransactionalSparkTableDataObject."),(0,r.kt)("p",null,"Further, we need a key to identify records for a specific object in our data, so we can build the time dimension or deduplicate records of the same object:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"For airport masterdata (",(0,r.kt)("inlineCode",{parentName:"li"},"int_airports"),') the attribute "ident" clearly serves this purpose.'),(0,r.kt)("li",{parentName:"ul"},"For departure data (",(0,r.kt)("inlineCode",{parentName:"li"},"int_departures"),") it gets more complicated to identify a flight. To simplify, let's assume we're only interested in one flight per aircraft, departure airport and day.\nThe key would then be the attributes ",(0,r.kt)("inlineCode",{parentName:"li"},"icao24"),", ",(0,r.kt)("inlineCode",{parentName:"li"},"estdepartureairport")," and ",(0,r.kt)("inlineCode",{parentName:"li"},"trunc_date"),".")),(0,r.kt)("h2",{id:"historization-of-airport-data"},"Historization of airport data"),(0,r.kt)("p",null,"To historize airport master data, we have to adapt our configuration as follows:"),(0,r.kt)("p",null,"Add a primary key to the table definition of ",(0,r.kt)("inlineCode",{parentName:"p"},"int-airports"),":"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'table {\n  db = "default"\n  name = "int_airports"\n  primaryKey = [ident]\n}\n')),(0,r.kt)("p",null,"Note, that a primary key can be a composite primary key, therefore you need to define an array of columns ",(0,r.kt)("inlineCode",{parentName:"p"},"[ident]"),"."),(0,r.kt)("p",null,"For the action ",(0,r.kt)("inlineCode",{parentName:"p"},"select-airport-cols"),", change it's type from ",(0,r.kt)("inlineCode",{parentName:"p"},"CopyAction")," to ",(0,r.kt)("inlineCode",{parentName:"p"},"HistorizeAction"),".",(0,r.kt)("br",{parentName:"p"}),"\n","While you're at it, rename it to ",(0,r.kt)("inlineCode",{parentName:"p"},"historize-airports")," to reflect it's new function."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"historize-airports {\n  type = HistorizeAction\n  ...\n}\n")),(0,r.kt)("p",null,"With historization, this table will now get two additional columns called ",(0,r.kt)("inlineCode",{parentName:"p"},"dl_ts_captured")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"dl_ts_delimited"),".\nSchema evolution of existing tables will be explained later, so for now, just delete the table and it's data for the DataObject ",(0,r.kt)("inlineCode",{parentName:"p"},"int-airports")," through Polynote.\nTo access DataObjects from Polynote you need to first read SDL configuration into a registry, see Notebook ",(0,r.kt)("em",{parentName:"p"},"SelectingData")," chapter ",(0,r.kt)("em",{parentName:"p"},"Select data by using DataObjects configured in SmartDataLake"),":"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'val dataIntAirports = registry.get[DeltaLakeTableDataObject]("int-airports")\ndataIntAirports.dropTable\n')),(0,r.kt)("admonition",{type:"caution"},(0,r.kt)("p",{parentName:"admonition"},"Depending on your system setup, it's possible that Polynote is not allowed to drop the data of your table.\nIf you receive strange errors about dl_ts_captured and dl_ts_delimited not being found, please delete the folder data/int-airports/ manually.")),(0,r.kt)("p",null,"Then start Action ",(0,r.kt)("inlineCode",{parentName:"p"},"historize-airports"),".\nYou may have seen that the ",(0,r.kt)("inlineCode",{parentName:"p"},"--feed-sel")," parameter of SDL command line supports more options to select actions to execute (see command line help).\nWe will now only execute this single action by changing this parameter to ",(0,r.kt)("inlineCode",{parentName:"p"},"--feed-sel ids:historize-airports"),":"),(0,r.kt)(i.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,r.kt)(o.Z,{value:"docker",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-jsx"},"docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network getting-started_default sdl-spark:latest -c /mnt/config --feed-sel ids:historize-airports\n"))),(0,r.kt)(o.Z,{value:"podman",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-jsx"},"podman run --hostname=localhost --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --pod getting-started sdl-spark:latest -c /mnt/config --feed-sel ids:historize-airports\n")))),(0,r.kt)("p",null,"After successful execution you can check the schema and data of our table in Polynote.\nIt now has a time dimension through the two new columns ",(0,r.kt)("inlineCode",{parentName:"p"},"dl_ts_captured")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"dl_ts_delimited"),".\nThey form a closed interval, meaning start and end time are inclusive.\nIt has millisecond precision, but the timestamp value is set to the current time of our data pipeline run.\nThe two attributes show the time period in which an object with this combination of attribute values has existed in our data source.\nThe sampling rate is given by the frequency that our data pipeline is scheduled."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"dataIntAirports.getDataFrame().printSchema\n\nroot\n|-- ident: string (nullable = true)\n|-- name: string (nullable = true)\n|-- latitude_deg: string (nullable = true)\n|-- longitude_deg: string (nullable = true)\n|-- dl_ts_captured: timestamp (nullable = true)\n|-- dl_ts_delimited: timestamp (nullable = true)\n")),(0,r.kt)("p",null,"If you look at the data, there should be only one record per object for now, as we didn't run our data pipeline with historical data yet."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'dataIntAirports.getDataFrame().orderBy($"ident",$"dl_ts_captured").show\n\n+-----+--------------------+------------------+-------------------+--------------------+-------------------+\n|ident|                name|      latitude_deg|      longitude_deg|      dl_ts_captured|    dl_ts_delimited|\n+-----+--------------------+------------------+-------------------+--------------------+-------------------+\n|  00A|   Total Rf Heliport|    40.07080078125| -74.93360137939453|2021-12-05 13:23:...|9999-12-31 00:00:00|\n| 00AA|Aero B Ranch Airport|         38.704022|        -101.473911|2021-12-05 13:23:...|9999-12-31 00:00:00|\n| 00AK|        Lowell Field|         59.947733|        -151.692524|2021-12-05 13:23:...|9999-12-31 00:00:00|\n| 00AL|        Epps Airpark| 34.86479949951172| -86.77030181884766|2021-12-05 13:23:...|9999-12-31 00:00:00|\n| 00AR|Newport Hospital ...|           35.6087|         -91.254898|2021-12-05 13:23:...|9999-12-31 00:00:00|\n...\n')),(0,r.kt)("p",null,"Let's try to simulate the historization process by loading a historical state of the data and see if any of the airports have changed since then.\nFor this, drop table ",(0,r.kt)("inlineCode",{parentName:"p"},"int-airports")," again.\nThen, delete all files in ",(0,r.kt)("inlineCode",{parentName:"p"},"data/stg-airport")," and copy the historical ",(0,r.kt)("inlineCode",{parentName:"p"},"result.csv")," from the folder ",(0,r.kt)("inlineCode",{parentName:"p"},"data-fallback-download/stg-airport")," into the folder ",(0,r.kt)("inlineCode",{parentName:"p"},"data/stg-aiport"),"."),(0,r.kt)("p",null,"Now start the action ",(0,r.kt)("inlineCode",{parentName:"p"},"historize-airports"),' (and only historize-airports) again to do an "initial load".\nRemember how you do that? That\'s right, you can define a single action with ',(0,r.kt)("inlineCode",{parentName:"p"},"--feed-sel ids:historize-airports"),".",(0,r.kt)("br",{parentName:"p"}),"\n","Afterwards, start actions ",(0,r.kt)("inlineCode",{parentName:"p"},"download-airports")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"historize-airports")," by using the parameter ",(0,r.kt)("inlineCode",{parentName:"p"},"--feed-sel 'ids:(download|historize)-airports'")," to download fresh data and build up the airport history."),(0,r.kt)("p",null,"Now check in Polynote again and you'll find several airports that have changed between the intitial and the current state:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'dataIntAirports.getDataFrame()\n.groupBy($"ident").count\n.orderBy($"count".desc)\n.show\n\n+-------+-----+\n|  ident|count|\n+-------+-----+\n|RU-4111|    2|\n|   LL33|    2|\n|   73CA|    2|\n|CA-0120|    2|\n|   CDV3|    2|\n...\n')),(0,r.kt)("p",null,"When checking the details it seems that for many airports the number of significant digits was reduced for the position:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'dataIntAirports.getDataFrame()\n.where($"ident"==="CDV3")\n.show(false)\n\n+-----+-------------------------------------------------+-------------+--------------+--------------------------+--------------------------+\n|ident|name                                             |latitude_deg |longitude_deg |dl_ts_captured            |dl_ts_delimited           |\n+-----+-------------------------------------------------+-------------+--------------+--------------------------+--------------------------+\n|CDV3 |Charlottetown (Queen Elizabeth Hospital) Heliport|46.255493    |-63.098887    |2021-12-05 20:52:58.800645|9999-12-31 00:00:00       |\n|CDV3 |Charlottetown (Queen Elizabeth Hospital) Heliport|46.2554925916|-63.0988866091|2021-12-05 20:40:31.629764|2021-12-05 20:52:58.799645|\n+-----+-------------------------------------------------+-------------+--------------+--------------------------+--------------------------+\n')),(0,r.kt)("p",null,"Values for ",(0,r.kt)("inlineCode",{parentName:"p"},"dl_ts_capture")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"dl_ts_delimited")," respectively were set to the current time of our data pipeline run.\nFor an initial load, this should be set to the time of the historical data set.\nCurrently, this is not possible in SDL, but there are plans to implement this, see issue ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/smart-data-lake/smart-data-lake/issues/427"},"#427"),"."),(0,r.kt)("p",null,"Now let's continue with flight data."),(0,r.kt)("admonition",{title:"Spark performance",type:"tip"},(0,r.kt)("p",{parentName:"admonition"},"Maybe you're under the impression that HistorizeAction runs quite long for a small amount of data.\nAnd you're right about that:",(0,r.kt)("br",{parentName:"p"}),"\n","On one side this is because in the background, it joins all existing data with the new input data and checks for changes.",(0,r.kt)("br",{parentName:"p"}),"\n","On the other side there is a Spark property we should tune for small datasets.\nIf Spark joins data, it needs two processing stages and a shuffle in between to do so (you can read more about this in various Spark tutorials).\nThe default value is to create 200 tasks in each shuffle. With our dataset, 2 tasks would be enough already.\nYou can tune this by setting the following property in global.spark-options of your application.conf:"),(0,r.kt)("pre",{parentName:"admonition"},(0,r.kt)("code",{parentName:"pre"},'"spark.sql.shuffle.partitions" = 2\n')),(0,r.kt)("p",{parentName:"admonition"},"Also, the algorithm to detect and merge changes can be optimized by using Delta formats merge capabilities. This will be covered in part three of this tutorial. ")),(0,r.kt)("h2",{id:"deduplication-of-flight-data"},"Deduplication of flight data"),(0,r.kt)("p",null,"To deduplicate departure flight data, we have to adapt our configuration as follows:"),(0,r.kt)("p",null,"Add a primary key to the table definition of ",(0,r.kt)("inlineCode",{parentName:"p"},"int-departures"),":"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'table {\n  db = "default"\n  name = "int_departures"\n  primaryKey = [icao24, estdepartureairport, dt]\n}\n')),(0,r.kt)("p",null,"Change the type of action ",(0,r.kt)("inlineCode",{parentName:"p"},"prepare-departures")," from ",(0,r.kt)("inlineCode",{parentName:"p"},"CopyAction"),", this time to ",(0,r.kt)("inlineCode",{parentName:"p"},"DeduplicateAction")," and rename it to ",(0,r.kt)("inlineCode",{parentName:"p"},"deduplicate-departures"),", again to reflect its new type.\nIt also needs an additional transformer to calculate the new primary key column ",(0,r.kt)("inlineCode",{parentName:"p"},"dt")," derived from the column ",(0,r.kt)("inlineCode",{parentName:"p"},"firstseen"),".\nSo make sure to add these lines too: "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"deduplicate-departures {\n  type = DeduplicateAction\n  ...\n  transformers = [{\n    type = SQLDfTransformer\n    code = \"select stg_departures.*, date_format(from_unixtime(firstseen),'yyyyMMdd') dt from stg_departures\"\n  }]\n  ...\n}\n")),(0,r.kt)("p",null,"Now, delete the table and data of the DataObject ",(0,r.kt)("inlineCode",{parentName:"p"},"int-departures")," in Polynote, to prepare it for the new columns ",(0,r.kt)("inlineCode",{parentName:"p"},"dt")," and ",(0,r.kt)("inlineCode",{parentName:"p"},"dl_ts_captured"),"."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'val dataIntDepartures = registry.get[DeltaLakeTableDataObject]("int-departures")\ndataIntDepartures.dropTable\n')),(0,r.kt)("p",null,"Then start Action deduplicate-departures:"),(0,r.kt)(i.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,r.kt)(o.Z,{value:"docker",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-jsx"},"docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network getting-started_default sdl-spark:latest -c /mnt/config --feed-sel ids:deduplicate-departures\n"))),(0,r.kt)(o.Z,{value:"podman",mdxType:"TabItem"},(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-jsx"},"podman run --rm --hostname=localhost -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --pod getting-started sdl-spark:latest -c /mnt/config --feed-sel ids:deduplicate-departures\n")))),(0,r.kt)("p",null,"After successful execution you can check the schema and data of our table in Polynote.\nThe new column ",(0,r.kt)("inlineCode",{parentName:"p"},"dl_ts_captured")," shows the current time of the data pipeline run when this object first occurred in the input data. "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"dataIntDepartures.getDataFrame().printSchema\n\nroot\n|-- arrivalairportcandidatescount: long (nullable = true)\n|-- callsign: string (nullable = true)\n|-- departureairportcandidatescount: long (nullable = true)\n|-- estarrivalairport: string (nullable = true)\n|-- estarrivalairporthorizdistance: long (nullable = true)\n|-- estarrivalairportvertdistance: long (nullable = true)\n|-- estdepartureairport: string (nullable = true)\n|-- estdepartureairporthorizdistance: long (nullable = true)\n|-- estdepartureairportvertdistance: long (nullable = true)\n|-- firstseen: long (nullable = true)\n|-- icao24: string (nullable = true)\n|-- lastseen: long (nullable = true)\n|-- dt: string (nullable = true)\n|-- dl_ts_captured: timestamp (nullable = true)\n")),(0,r.kt)("p",null,"We can check the work of DeduplicateAction by the following query in Polynote: "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'dataIntDepartures.getDataFrame()\n.groupBy($"icao24", $"estdepartureairport", $"dt")\n.count\n.orderBy($"count".desc)\n.show\n\n+------+-------------------+--------+-----+\n|icao24|estdepartureairport|      dt|count|\n+------+-------------------+--------+-----+\n|4b43ab|               LSZB|20210829|    3|\n|4b4b8d|               LSZB|20210829|    3|\n|4b1b13|               LSZB|20210829|    2|\n|4b4445|               LSZB|20210829|    2|\n|4b0f70|               LSZB|20210830|    1|\n|4b1a01|               LSZB|20210829|    1|\n|346603|               LSZB|20210829|    1|\n|4b4442|               LSZB|20210829|    1|\n|4d02d7|               LSZB|20210829|    1|\n|4b43ab|               LSZB|20210830|    1|\n...\n')),(0,r.kt)("p",null,"... and it seems that it did not work properly! There are 2 or even 3 records for the same primary key!\nEven worse, we just deleted this table before, so DeduplicateAction shouldn't have any work to do at all."),(0,r.kt)("p",null,"In fact DeduplicateAction assumes that input data is already unique for the given primary key.\nThis would be the case for example, in a messaging context, if you were to receive the same message twice.\nDeduplicateAction doesn't deduplicate your input data again, because deduplication is costly and data often is already unique.\nBut in our example we have duplicates in the input data set, and we need to add some deduplication logic to our input data (this will probably become a configuration flag in future SDL version, see issue ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/smart-data-lake/smart-data-lake/issues/428"},"#428"),")."),(0,r.kt)("p",null,"As the easiest way to do this is by using the Scala Spark API, we will add a second ScalaCodeSparkDfTransformer as follows (make sure you get the brackets right): "),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'deduplicate-departures {\n  type = DeduplicateAction\n  ...\n  transformers = [{\n    type = SQLDfTransformer\n    code = "select stg_departures.*, date_format(from_unixtime(firstseen),\'yyyyMMdd\') dt from stg_departures"\n  },{\n    type = ScalaCodeSparkDfTransformer\n    code = """\n      import org.apache.spark.sql.{DataFrame, SparkSession}\n      def transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {\n        import session.implicits._\n        df.dropDuplicates("icao24", "estdepartureairport", "dt")\n      }\n      // return as function\n      transform _\n    """\n  }]\n  ...\n}\n')),(0,r.kt)("p",null,"If you run Action ",(0,r.kt)("inlineCode",{parentName:"p"},"deduplicate-departures")," again and check the result in Polynote, everything is fine now."),(0,r.kt)("admonition",{type:"info"},(0,r.kt)("p",{parentName:"admonition"},"Note how we have used a third way of defining transformation logic now:",(0,r.kt)("br",{parentName:"p"}),"\n","In part 1 we first used a SQLDfsTransformer writing SQL code.",(0,r.kt)("br",{parentName:"p"}),"\n","Then for the more complex example of computing distances, we used a  ScalaClassSparkDfTransformer pointing to a Scala class.",(0,r.kt)("br",{parentName:"p"}),"\n","Here, we simply include Scala code in our configuration file directly.")),(0,r.kt)("p",null,"For sure DeduplicateAction did not have much work to do, as this was the first data load.\nIn order to get different data you would need to adjust the unix timestamp parameters in the URL of DataObject ",(0,r.kt)("inlineCode",{parentName:"p"},"ext-departures"),".\nFeel free to play around."),(0,r.kt)("admonition",{title:"Scala Code",type:"info"},(0,r.kt)("p",{parentName:"admonition"},"Scala is a compiled language. The compiler creates bytecode which can be run on a JVM.\nNormally compilation takes place before execution. So how does it work with scala code in the configuration as in our deduplication logic above?"),(0,r.kt)("p",{parentName:"admonition"},"With Scala, you can compile code on the fly. This is actually what the Scala Shell/REPL is doing as well.\nThe Scala code in the configuration above gets compiled when ScalaCodeSparkDfTransformer is instantiated during startup of SDL.")),(0,r.kt)("h2",{id:"summary"},"Summary"),(0,r.kt)("p",null,"You have now seen different parts of industrializing a data pipeline like robust data formats and caring about historical data.\nFurther, you have explored data interactively with a notebook. "),(0,r.kt)("p",null,"The final configuration file of Part 2 should look like ",(0,r.kt)("a",{target:"_blank",href:a(9140).Z},"this")),(0,r.kt)("p",null,"In part 3 we will see how to incrementally load fresh flight data and optimize deduplication and historization.\nSee you!"))}m.isMDXComponent=!0},9140:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/application-part2-historical-62b47cb172772c79ba8f08c7082ca360.conf"}}]);