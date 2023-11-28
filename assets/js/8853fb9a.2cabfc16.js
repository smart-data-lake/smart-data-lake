"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2816],{3905:(e,t,n)=>{n.d(t,{Zo:()=>d,kt:()=>h});var a=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function r(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,i=function(e,t){if(null==e)return{};var n,a,i={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var l=a.createContext({}),p=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):r(r({},t),e)),n},d=function(e){var t=p(e.components);return a.createElement(l.Provider,{value:t},e.children)},u="mdxType",c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,i=e.mdxType,o=e.originalType,l=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),u=p(n),m=i,h=u["".concat(l,".").concat(m)]||u[m]||c[m]||o;return n?a.createElement(h,r(r({ref:t},d),{},{components:n})):a.createElement(h,r({ref:t},d))}));function h(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var o=n.length,r=new Array(o);r[0]=m;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[u]="string"==typeof e?e:i,r[1]=s;for(var p=2;p<o;p++)r[p]=n[p];return a.createElement.apply(null,r)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},6679:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>r,default:()=>c,frontMatter:()=>o,metadata:()=>s,toc:()=>p});var a=n(7462),i=(n(7294),n(3905));const o={id:"executionModes",title:"Execution Modes"},r=void 0,s={unversionedId:"reference/executionModes",id:"reference/executionModes",title:"Execution Modes",description:"Execution modes",source:"@site/docs/reference/executionModes.md",sourceDirName:"reference",slug:"/reference/executionModes",permalink:"/docs/reference/executionModes",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/executionModes.md",tags:[],version:"current",frontMatter:{id:"executionModes",title:"Execution Modes"},sidebar:"docs",previous:{title:"Execution Engines",permalink:"/docs/reference/executionEngines"},next:{title:"Transformations",permalink:"/docs/reference/transformations"}},l={},p=[{value:"Execution modes",id:"execution-modes",level:2},{value:"Partitions",id:"partitions",level:2},{value:"Default Behavior",id:"default-behavior",level:3},{value:"FailIfNoPartitionValuesMode",id:"failifnopartitionvaluesmode",level:3},{value:"PartitionDiffMode: Dynamic partition values filter",id:"partitiondiffmode-dynamic-partition-values-filter",level:3},{value:"applyCondition",id:"applycondition",level:4},{value:"failCondition",id:"failcondition",level:4},{value:"selectExpression",id:"selectexpression",level:4},{value:"alternativeOutputId",id:"alternativeoutputid",level:4},{value:"partitionColNb",id:"partitioncolnb",level:4},{value:"nbOfPartitionValuesPerRun",id:"nbofpartitionvaluesperrun",level:4},{value:"CustomPartitionMode",id:"custompartitionmode",level:3},{value:"Incremental load",id:"incremental-load",level:2},{value:"DataFrameIncrementalMode",id:"dataframeincrementalmode",level:3},{value:"DataObjectStateIncrementalMode",id:"dataobjectstateincrementalmode",level:3},{value:"SparkStreamingMode",id:"sparkstreamingmode",level:3},{value:"KafkaStateIncrementalMode",id:"kafkastateincrementalmode",level:3},{value:"FileIncrementalMoveMode",id:"fileincrementalmovemode",level:3},{value:"Others",id:"others",level:2},{value:"ProcessAllMode",id:"processallmode",level:3},{value:"CustomPartitionMode",id:"custompartitionmode-1",level:3},{value:"CustomMode",id:"custommode",level:3}],d={toc:p},u="wrapper";function c(e){let{components:t,...n}=e;return(0,i.kt)(u,(0,a.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h2",{id:"execution-modes"},"Execution modes"),(0,i.kt)("p",null,"Execution modes select the data to be processed by a SmartDataLakeBuilder Action.\nBy default, there is no filter applied meaning that every Action reads all data passed on by the predecessor Action.\nIf an input has no predecessor Action in the DAG, all data is read from the corresponding DataObject."),(0,i.kt)("p",null,"This default behavior is applied if you don't set an explicit execution mode.\nIf you want one of the execution modes described below, you have to explicitly set it on the Action:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"executionMode {\n  type = PartitionDiffMode\n  attribute1 = ...\n}\n")),(0,i.kt)("p",null,"There are 2 major types of execution modes selecting the subset of data based on:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"partition-wise"),": Either SDLB will automatically select and process missing partitions from the input, or the partitions are defined manually by a command line parameter."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"incremental"),": SDLB will automatically select new and updated data from the input and process data incrementally.")),(0,i.kt)("h2",{id:"partitions"},"Partitions"),(0,i.kt)("p",null,(0,i.kt)("em",{parentName:"p"},"Partition")," is an ambiguous term.\nIt is used in distributed data processing (e.g. Spark tasks and repartitioning), but also for splitting data in storage for performance reasons,\ne.g. a partitioned database table, kafka topic or hadoop directory structure."),(0,i.kt)("p",null,"The use of partition in SDLB is about the latter case.\nSDLB supports intelligent processing of partitioned data (data split in storage) for optimal performance."),(0,i.kt)("admonition",{type:"info"},(0,i.kt)("p",{parentName:"admonition"},"Partition-wise data processing is one of the most performant data pipeline paradigms.\nBut it also limits data processing to always write or replace whole partitions.\nKeep that in mind when designing your Actions.")),(0,i.kt)("p",null,"To use partitioned data and partition-wise data processing in SDLB, you need to configure partition columns on a DataObject that can handle partitions.\ni.e. on a ",(0,i.kt)("inlineCode",{parentName:"p"},"DeltaLakeTableDataObject"),":"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'partitionedDataObject {\n    type = DeltaLakeTableDataObject\n    table = {\n        name = "partitionedTable"\n    }\n    partitions = ["day"]\n}\n')),(0,i.kt)("p",null,"Note that not all DataObjects support partitioning."),(0,i.kt)("h3",{id:"default-behavior"},"Default Behavior"),(0,i.kt)("p",null,"In its simplest form, you manually define the partition values to be processed by specifying the command line parameter ",(0,i.kt)("inlineCode",{parentName:"p"},"--partition-values")," or ",(0,i.kt)("inlineCode",{parentName:"p"},"--multi-partition-values"),", see ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/commandLine"},"Command Line"),".\nThis is in the form of columnName=columnValue, e.g. ",(0,i.kt)("inlineCode",{parentName:"p"},"--partition-values day=2020-01-01"),".\nThe partition values specified are passed to ",(0,i.kt)("strong",{parentName:"p"},"all")," start inputs (inputs with no predecessor in the DAG).\nIn other words, each input DataObject is filtered by the given partition values on the specified column(s)."),(0,i.kt)("p",null,"If the parameters ",(0,i.kt)("inlineCode",{parentName:"p"},"--partition-values")," or ",(0,i.kt)("inlineCode",{parentName:"p"},"--multi-partition-values")," are not specified, SDLB will simply process all available data."),(0,i.kt)("p",null,"Subsequent Actions will automatically use the same partition values as their predecessor Action."),(0,i.kt)("admonition",{type:"info"},(0,i.kt)("p",{parentName:"admonition"},"Actions can also have multiple input DataObjects (e.g. CustomDataFrameAction).\nIn this case, a ",(0,i.kt)("em",{parentName:"p"},"main input")," needs to be defined to take the partition values from.\nYou can either set the main input yourself by specifying the ",(0,i.kt)("inlineCode",{parentName:"p"},"mainInputId")," property on the Action, or SDLB will automatically choose the input.",(0,i.kt)("br",{parentName:"p"}),"\n","Automatic selection uses a heuristic:",(0,i.kt)("br",{parentName:"p"}),"\n","The first input which is not skipped, is not in the list of ",(0,i.kt)("inlineCode",{parentName:"p"},"inputIdsToIgnoreFilter"),", and has the most partition columns defined is chosen.")),(0,i.kt)("p",null,"Another special case you might encounter is if an output DataObject has different partition columns than the input DataObject.\nIn these cases, the partition value columns of the input DataObject will be filtered and passed on to the next action.",(0,i.kt)("br",{parentName:"p"}),"\n","So if your input DataObject has ","[runId, division]"," as partition values and your output DataObject ","[runId]",",\nthen ",(0,i.kt)("inlineCode",{parentName:"p"},"division")," will be removed from the partition value columns before they are passed on."),(0,i.kt)("p",null,"This default behaviour is active without providing any explicit ",(0,i.kt)("inlineCode",{parentName:"p"},"executionMode")," in the config of your Actions.\nThis means that processing partitioned data is available out-of-the-box in SDLB."),(0,i.kt)("admonition",{type:"info"},(0,i.kt)("p",{parentName:"admonition"},"The partition values act as filter.\nSDLB also validates that partition values actually exist in input DataObjects.\nIf they don't, execution will stop with an error.\nThis behavior can be changed by setting ",(0,i.kt)("inlineCode",{parentName:"p"},"DataObject.expectedPartitionsCondition"),"."),(0,i.kt)("p",{parentName:"admonition"},"This is especially interesting with CustomDataFrameAction having multiple partitioned inputs,\nso you don't just join them if some of the input data is missing.")),(0,i.kt)("admonition",{type:"info"},(0,i.kt)("p",{parentName:"admonition"},"Another important caveat:",(0,i.kt)("br",{parentName:"p"}),"\n","Partition values filter all input DataObject ",(0,i.kt)("em",{parentName:"p"},"that contain the given partition columns"),".\nThis also means, that they don't have any effect on input DataObjects with different partition columns\nor no partitions at all."),(0,i.kt)("p",{parentName:"admonition"},"If you need to read everything from one DataObject, even though it does have the same partition columns,\nyou can again use ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomDataFrameAction.inputIdsToIgnoreFilter")," to override the default behavior.")),(0,i.kt)("h3",{id:"failifnopartitionvaluesmode"},"FailIfNoPartitionValuesMode"),(0,i.kt)("p",null,"If you use the method described above, you might want to set the executionMode to ",(0,i.kt)("inlineCode",{parentName:"p"},"FailIfNoPartitionValuesMode"),".\nThis mode enforces having partition values specified.\nIt simply checks if partition values are present and fails otherwise.\nThis is useful to prevent potential reprocessing of whole tables due to wrong usage."),(0,i.kt)("h3",{id:"partitiondiffmode-dynamic-partition-values-filter"},"PartitionDiffMode: Dynamic partition values filter"),(0,i.kt)("p",null,"Instead of specifying the partition values manually, you can let SDLB find missing partitions and set partition values automatically by specifying execution mode ",(0,i.kt)("inlineCode",{parentName:"p"},"PartitionDiffMode"),".\nIn its basic form, it compares input and output DataObjects and decides which partitions are missing in the output.\nIt then automatically uses these missing partitions as partition values."),(0,i.kt)("admonition",{type:"info"},(0,i.kt)("p",{parentName:"admonition"},"Remember that a partition can exist in the output DataObject with no data or partial data only.\nFor SDLB, the partition exists and will not be processed again.\nKeep that in mind when recovering from errors.")),(0,i.kt)("p",null,"This mode is quite powerful and has a couple of options to fine tune its behaviour."),(0,i.kt)("h4",{id:"applycondition"},"applyCondition"),(0,i.kt)("p",null,"By default, the PartitionDiffMode is applied if the given partition values are empty (partition values from command line or passed from previous action).\nYou can override this behavior by defining an ",(0,i.kt)("inlineCode",{parentName:"p"},"applyCondition"),"."),(0,i.kt)("p",null,"ApplyCondition is a spark sql expression working with attributes of ",(0,i.kt)("a",{parentName:"p",href:"https://github.com/smart-data-lake/smart-data-lake/blob/c435434d0174ca0862bdeef4838c6678aaef05fd/sdl-core/src/main/scala/io/smartdatalake/workflow/action/executionMode/ExecutionMode.scala#L164"},"DefaultExecutionModeExpressionData")," returning a boolean."),(0,i.kt)("p",null,"Simple Example - apply PartitionDiffMode even if partition values are given (always):"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'  applyCondition = "true"\n')),(0,i.kt)("h4",{id:"failcondition"},"failCondition"),(0,i.kt)("p",null,"If you have clear expectations of what your partition values should look like, you can enforce your rules by defining a ",(0,i.kt)("inlineCode",{parentName:"p"},"failCondition"),".\nIn it, you define a spark sql expression that is evaluated after the PartitionDiffMode is applied.\nIf it evaluates to ",(0,i.kt)("inlineCode",{parentName:"p"},"true"),", the Action will fail."),(0,i.kt)("p",null,"In the condition, the following attributes are available amongst others to make the decision: ",(0,i.kt)("inlineCode",{parentName:"p"},"inputPartitionValues"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"outputPartitionValues"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"selectedInputPartitionValues")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"selectedOutputPartitionValues"),".\nUse these to fail the run based on expected partitions or time conditions."),(0,i.kt)("p",null,"Default is ",(0,i.kt)("inlineCode",{parentName:"p"},"false")," meaning that the application of the PartitionDiffMode does not fail the action.\nIf there is no data to process, the following actions are skipped."),(0,i.kt)("p",null,"Example - fail if partitions are not processed in strictly increasing order of partition column ",(0,i.kt)("inlineCode",{parentName:"p"},"dt"),":"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'  failCondition = "(size(selectedInputPartitionValues) > 0 and array_min(transform(selectedInputPartitionValues, x -> x.dt)) < array_max(transform(outputOutputPartitionValues, x -> x.dt)))"\n')),(0,i.kt)("p",null,"Sometimes the ",(0,i.kt)("inlineCode",{parentName:"p"},"failCondition")," can become quite complex with multiple terms concatenated by or-logic.\nTo improve interpretability of error messages, multiple fail conditions can be configured as an array using the attribute ",(0,i.kt)("inlineCode",{parentName:"p"},"failConditions"),".\nFor each condition you can also define a description which will be inserted into the error message."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'  failConditions = [{\n    expression = "(size(selectedInputPartitionValues) > 0 and array_min(transform(selectedInputPartitionValues, x -> x.dt)) < array_max(transform(outputOutputPartitionValues, x -> x.dt)))"\n    description = "fail if partitions are not processed in strictly increasing order of partition column dt"\n  }]\n')),(0,i.kt)("h4",{id:"selectexpression"},"selectExpression"),(0,i.kt)("p",null,"The option ",(0,i.kt)("inlineCode",{parentName:"p"},"selectExpression")," refines or overrides the list of selected output partitions. It can access the partition values selected by the default behaviour and refine the list, or it can override selected partition values by using input & output partition values directly.\nDefine a spark sql expression working with attributes of PartitionDiffModeExpressionData returning a ",(0,i.kt)("inlineCode",{parentName:"p"},"Seq(Map(String,String))"),"."),(0,i.kt)("p",null,"Example - only process the last selected partition:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'  selectExpression = "slice(selectedInputPartitionValues,-1,1)"\n')),(0,i.kt)("h4",{id:"alternativeoutputid"},"alternativeOutputId"),(0,i.kt)("p",null,"Usually, PartitionDiffMode will check for missing partitions against the main output DataObject.\nYou can force PartitionDiffMode to check for missing partitions against another DataObject by defining ",(0,i.kt)("inlineCode",{parentName:"p"},"alternativeOutputId"),"."),(0,i.kt)("p",null,"This can even be a DataObject that comes later in your pipeline so it doesn't have to be one of the Actions output DataObjects."),(0,i.kt)("h4",{id:"partitioncolnb"},"partitionColNb"),(0,i.kt)("p",null,"When comparing which partitions and subpartitions need to be transferred, usually all the parition columns are used.\nWith these setting, you can limit the amount of columns used for the comparison to the first N columns."),(0,i.kt)("h4",{id:"nbofpartitionvaluesperrun"},"nbOfPartitionValuesPerRun"),(0,i.kt)("p",null,"If you have a lot of partitions, you might want to limit the number of partitions processed per run.\nIf you define ",(0,i.kt)("inlineCode",{parentName:"p"},"nbOfPartitionValuesPerRun"),", PartitionDiffMode will only process the first n partitions and ignore the rest."),(0,i.kt)("h3",{id:"custompartitionmode"},"CustomPartitionMode"),(0,i.kt)("p",null,"This execution mode allows for complete customized logic to select partitions to process in Scala.\nImplement trait ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomPartitionModeLogic")," by defining a function which receives main input & output DataObjects and returns partition values to process as ",(0,i.kt)("inlineCode",{parentName:"p"},"Seq[Map[String,String]]")),(0,i.kt)("h2",{id:"incremental-load"},"Incremental load"),(0,i.kt)("p",null,"Some DataObjects are not partitioned, but nevertheless you don't want to read all data from the input on every run.\nYou want to load it incrementally."),(0,i.kt)("p",null,"SDLB implements various methods to incrementally process data from various DataObjects.\nChoosing an incremental mode depends on the DataObjects used.\nDo they support Spark Streaming?\nDoes Spark Streaming support the transformations you're using?\nDo you want a synchronous data pipeline or run all steps asynchronously to improve latency?"),(0,i.kt)("h3",{id:"dataframeincrementalmode"},"DataFrameIncrementalMode"),(0,i.kt)("p",null,"One of the most common forms of incremental processing is ",(0,i.kt)("inlineCode",{parentName:"p"},"DataFrameIncrementalMode"),".\nYou configure it by defining the attribute ",(0,i.kt)("inlineCode",{parentName:"p"},"compareCol")," naming a column that exists in both the input and output DataObject.\n",(0,i.kt)("inlineCode",{parentName:"p"},"DataFrameIncrementalMode")," then compares the maximum values between input and output to decide what needs to be loaded."),(0,i.kt)("p",null,"For this mode to work, ",(0,i.kt)("inlineCode",{parentName:"p"},"compareCol")," needs to be of a sortable datatype like int or timestamp."),(0,i.kt)("p",null,"The attributes ",(0,i.kt)("inlineCode",{parentName:"p"},"applyCondition")," and ",(0,i.kt)("inlineCode",{parentName:"p"},"alternativeOutputId")," work the same as for ",(0,i.kt)("a",{parentName:"p",href:"executionModes#partitiondiffmode-dynamic-partition-values-filter"},"PartitionDiffMode"),"."),(0,i.kt)("admonition",{type:"info"},(0,i.kt)("p",{parentName:"admonition"},"This execution mode has a performance drawback as it has to query the maximum value for ",(0,i.kt)("inlineCode",{parentName:"p"},"compareCol")," on input and output DataObjects each time.")),(0,i.kt)("h3",{id:"dataobjectstateincrementalmode"},"DataObjectStateIncrementalMode"),(0,i.kt)("p",null,"To optimize performance, SDLB can remember the state of the last increment it successfully loaded.\nFor this mode to work, you need to start SDLB with a ",(0,i.kt)("inlineCode",{parentName:"p"},"--state-path"),", see ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/commandLine"},"Command Line"),".\nThe .json file used to store the overall state of your application will be extended with state information for this DataObject."),(0,i.kt)("p",null,(0,i.kt)("inlineCode",{parentName:"p"},"DataObjectStateIncrementalMode")," needs a main input DataObject that can create incremental output, e.g. ",(0,i.kt)("inlineCode",{parentName:"p"},"JdbcTableDataObject"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"SparkFileDataObject")," or ",(0,i.kt)("inlineCode",{parentName:"p"},"AirbyteDataObject"),".\nThe inner meaning of the state is defined by the DataObject and not interpreted outside of it.\nSome DataObjects have a configuration option to define the incremental output, e.g. ",(0,i.kt)("inlineCode",{parentName:"p"},"JdbcTableDataObject.incrementalOutputExpr"),", others just use technical timestamps existing by design,\ne.g. ",(0,i.kt)("inlineCode",{parentName:"p"},"SparkFileDataObject"),"."),(0,i.kt)("admonition",{type:"info"},(0,i.kt)("p",{parentName:"admonition"},"This option also comes in handy if you can't use ",(0,i.kt)("inlineCode",{parentName:"p"},"DataFrameIncrementalMode")," because you can't access the output DataObject during initialization.\nFor example, if you push incremental Parquet files to a remote storage and these files are immediately processed and removed,\nyou will find an empty directory and therefore can't consult already uploaded data.\nIn this case, SDLB needs to remember what data increments were already uploaded.")),(0,i.kt)("h3",{id:"sparkstreamingmode"},"SparkStreamingMode"),(0,i.kt)("p",null,"SDLB also supports streaming with the ",(0,i.kt)("inlineCode",{parentName:"p"},"SparkStreamingMode"),".\nUnder the hood it uses ",(0,i.kt)("a",{parentName:"p",href:"https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html"},"Spark Structured Streaming"),".\nAn Action with SparkStreamingMode in streaming mode is an asynchronous action."),(0,i.kt)("p",null,"Its rhythm can be configured by setting a ",(0,i.kt)("inlineCode",{parentName:"p"},"triggerType")," to either ",(0,i.kt)("inlineCode",{parentName:"p"},"Once"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"ProcessingTime")," or ",(0,i.kt)("inlineCode",{parentName:"p"},"Continuous")," (default is ",(0,i.kt)("inlineCode",{parentName:"p"},"Once"),").\nWhen using ",(0,i.kt)("inlineCode",{parentName:"p"},"ProcessingTime")," or ",(0,i.kt)("inlineCode",{parentName:"p"},"Continuous")," you can configure the interval through the attribute ",(0,i.kt)("inlineCode",{parentName:"p"},"triggerTime"),"."),(0,i.kt)("admonition",{type:"info"},(0,i.kt)("p",{parentName:"admonition"},"You need to start your SDLB run with the ",(0,i.kt)("inlineCode",{parentName:"p"},"--streaming")," flag to enable streaming mode.\nIf you don't, SDLB will always use ",(0,i.kt)("inlineCode",{parentName:"p"},"Once")," as trigger type (single microbatch) and the action is executed synchronously.")),(0,i.kt)("p",null,"Spark Structured Streaming is keeping state information about processed data.\nTo do so, it needs a ",(0,i.kt)("inlineCode",{parentName:"p"},"checkpointLocation")," configured on the SparkStreamingMode."),(0,i.kt)("p",null,"Note that ",(0,i.kt)("em",{parentName:"p"},"Spark Structured Streaming")," needs an input DataObject that supports the creation of streaming DataFrames.\nFor the time being, only the input sources delivered with Spark Streaming are supported.\nThese are ",(0,i.kt)("inlineCode",{parentName:"p"},"KafkaTopicDataObject")," and all ",(0,i.kt)("inlineCode",{parentName:"p"},"SparkFileDataObjects"),", see also ",(0,i.kt)("a",{parentName:"p",href:"https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets"},"Spark StructuredStreaming"),"."),(0,i.kt)("h3",{id:"kafkastateincrementalmode"},"KafkaStateIncrementalMode"),(0,i.kt)("p",null,"A special incremental execution mode for Kafka inputs, remembering the state from the last increment through the Kafka Consumer,\ne.g. committed offsets."),(0,i.kt)("h3",{id:"fileincrementalmovemode"},"FileIncrementalMoveMode"),(0,i.kt)("p",null,"Another paradigm for incremental processing with files is to move or delete input files once they are processed.\nThis can be achieved by using FileIncrementalMoveMode. If option ",(0,i.kt)("inlineCode",{parentName:"p"},"archiveSubdirectory")," is configured, files are moved into that directory after processing, otherwise they are deleted."),(0,i.kt)("p",null,"FileIncrementalMoveMode can be used with the file engine (see also ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/executionEngines"},"Execution engines"),"), but also with SparkFileDataObjects and the data frame engine."),(0,i.kt)("h2",{id:"others"},"Others"),(0,i.kt)("h3",{id:"processallmode"},"ProcessAllMode"),(0,i.kt)("p",null,"An execution mode which forces processing of all data from its inputs.\nAny partitionValues and filter conditions received from previous actions are ignored."),(0,i.kt)("h3",{id:"custompartitionmode-1"},"CustomPartitionMode"),(0,i.kt)("p",null,"This execution mode allows to implement arbitrary custom partition logic using Scala."),(0,i.kt)("p",null,"Implement trait ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomPartitionMode")," by defining a function which receives the main input and output DataObjects\nand returns the partition values that need to be processed."),(0,i.kt)("h3",{id:"custommode"},"CustomMode"),(0,i.kt)("p",null,"This execution mode allows to implement arbitrary processing logic using Scala."),(0,i.kt)("p",null,"Implement trait ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomModeLogic")," by defining a function which receives main input and output DataObjects and returns an ",(0,i.kt)("inlineCode",{parentName:"p"},"ExecutionModeResult"),".\nThe result can contain input and output partition values, but also options which are passed to the transformations defined in the Action."))}c.isMDXComponent=!0}}]);