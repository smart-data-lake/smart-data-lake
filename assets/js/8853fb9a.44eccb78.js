"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2816],{3905:(e,t,a)=>{a.d(t,{Zo:()=>d,kt:()=>m});var n=a(7294);function i(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function r(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){i(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,n,i=function(e,t){if(null==e)return{};var a,n,i={},o=Object.keys(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||(i[a]=e[a]);return i}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(i[a]=e[a])}return i}var l=n.createContext({}),p=function(e){var t=n.useContext(l),a=t;return e&&(a="function"==typeof e?e(t):r(r({},t),e)),a},d=function(e){var t=p(e.components);return n.createElement(l.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},u=n.forwardRef((function(e,t){var a=e.components,i=e.mdxType,o=e.originalType,l=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),u=p(a),m=i,f=u["".concat(l,".").concat(m)]||u[m]||c[m]||o;return a?n.createElement(f,r(r({ref:t},d),{},{components:a})):n.createElement(f,r({ref:t},d))}));function m(e,t){var a=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var o=a.length,r=new Array(o);r[0]=u;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:i,r[1]=s;for(var p=2;p<o;p++)r[p]=a[p];return n.createElement.apply(null,r)}return n.createElement.apply(null,a)}u.displayName="MDXCreateElement"},6679:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>l,contentTitle:()=>r,default:()=>c,frontMatter:()=>o,metadata:()=>s,toc:()=>p});var n=a(7462),i=(a(7294),a(3905));const o={id:"executionModes",title:"Execution Modes"},r=void 0,s={unversionedId:"reference/executionModes",id:"reference/executionModes",title:"Execution Modes",description:"This page is under review",source:"@site/docs/reference/executionModes.md",sourceDirName:"reference",slug:"/reference/executionModes",permalink:"/docs/reference/executionModes",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/executionModes.md",tags:[],version:"current",frontMatter:{id:"executionModes",title:"Execution Modes"},sidebar:"docs",previous:{title:"Execution Engines",permalink:"/docs/reference/executionEngines"},next:{title:"Transformations",permalink:"/docs/reference/transformations"}},l={},p=[{value:"Execution modes",id:"execution-modes",level:2},{value:"Partitions",id:"partitions",level:2},{value:"Default Behavior",id:"default-behavior",level:3},{value:"FailIfNoPartitionValuesMode",id:"failifnopartitionvaluesmode",level:3},{value:"PartitionDiffMode: Dynamic partition values filter",id:"partitiondiffmode-dynamic-partition-values-filter",level:3},{value:"CustomPartitionMode",id:"custompartitionmode",level:3},{value:"Incremental load",id:"incremental-load",level:2},{value:"SparkStreamingMode",id:"sparkstreamingmode",level:3},{value:"DataFrameIncrementalMode",id:"dataframeincrementalmode",level:3},{value:"DataObjectStateIncrementalMode",id:"dataobjectstateincrementalmode",level:3},{value:"FileIncrementalMoveMode",id:"fileincrementalmovemode",level:3},{value:"Others",id:"others",level:2},{value:"ProcessAllMode",id:"processallmode",level:3},{value:"CustomMode",id:"custommode",level:3}],d={toc:p};function c(e){let{components:t,...a}=e;return(0,i.kt)("wrapper",(0,n.Z)({},d,a,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("admonition",{type:"warning"},(0,i.kt)("p",{parentName:"admonition"},"This page is under review ")),(0,i.kt)("h2",{id:"execution-modes"},"Execution modes"),(0,i.kt)("p",null,"Execution modes select the data to be processed by a SmartDataLakeBuilder job. By default, if you start a SmartDataLakeBuilder job, there is no filter applied. This means every Action reads all data passed on by the predecessor Action. If an input has no predecessor Action in the DAG, all data is read from the corresponding DataObject."),(0,i.kt)("p",null,'You can set an execution mode by defining attribute "executionMode" of an Action. Define the chosen ExecutionMode by setting the type as follows:'),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"executionMode {\n  type = PartitionDiffMode\n  attribute1 = ...\n}\n")),(0,i.kt)("p",null,"If no explicit executionMode is specified, the default behavior is used."),(0,i.kt)("p",null,"There are 2 major types of Execution Modes selecting the subset of data based on:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"partitions"),": the user selects specific partition to process, or lets SDLB select missing partitions automatically "),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"incremental"),": SDLB will automatically selected new and updated data from the input and process data incrementally.")),(0,i.kt)("h2",{id:"partitions"},"Partitions"),(0,i.kt)("p",null,"Partition is an ambiuous word. It is used in distributed data processing (e.g. Spark tasks and repartitioning), but also for spliting data in storage for performance reasons, e.g. a partitioned database table, kafka topic or hadoop directory structure. "),(0,i.kt)("p",null,"The use of partition in SDLB is about the later case. SDLB supports-wise processing of partitioned data (data splitted in storage) for optimal performance. Partition-wise data processing is one of the most performant data pipeline paradigms. But it also limits data processing to always write or replace whole partitions."),(0,i.kt)("p",null,"To use partitioned data and partition-wise data processing in SDLB, you need to configure partition columns on a DataObject that can handle partitions. This is for most DataObjects the case, even though some cannot directly access the partitions in the data storage. We implemented a concept called virtual partitions for them, see JdbcTableDataObject and KafkaTopicDataObject."),(0,i.kt)("h3",{id:"default-behavior"},"Default Behavior"),(0,i.kt)("p",null,"A filter based on partitions can be applied manually by specifying the command line parameter ",(0,i.kt)("inlineCode",{parentName:"p"},"--partition-values")," or ",(0,i.kt)("inlineCode",{parentName:"p"},"--multi-partition-values"),", see ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/commandLine"},"Command Line"),". The partition values specified are passed to ",(0,i.kt)("strong",{parentName:"p"},"all")," start inputs (inputs with no predecessor in the DAG), and filtered for every input DataObject by its defined partition columns.\nOn execution every Action takes the partition values of the main input and filters them again for every output DataObject by its defined partition columns, which serve again as partition values for the input of the next Action."),(0,i.kt)("p",null,"Actions with multiple inputs, e.g. CustomDataFrameAction will use heuristics to define a ",(0,i.kt)("em",{parentName:"p"},"main input"),". The first input which is not skipped, is not in the list of ",(0,i.kt)("em",{parentName:"p"},"inputIdsToIgnoreFilter"),", and has the most partition columns defined is choosen. It can be overriden by setting ",(0,i.kt)("em",{parentName:"p"},"mainInputId")," property."),(0,i.kt)("p",null,"The default behaviour is active without providing any explicit Execution Mode in the config of your Actions. This means that processing partitioned data available out-of-the-box in SDLB and done automatically."),(0,i.kt)("p",null,"Note that during execution of the dag, no new partition values are added, they are only filtered. "),(0,i.kt)("p",null,"If the parameters ",(0,i.kt)("inlineCode",{parentName:"p"},"--partition-values")," or ",(0,i.kt)("inlineCode",{parentName:"p"},"--multi-partition-values")," are not specified, SDLB will process all available data."),(0,i.kt)("p",null,"An exception is if you place a ",(0,i.kt)("inlineCode",{parentName:"p"},"PartitionDiffMode")," in the middle of your pipeline, see section ",(0,i.kt)("a",{parentName:"p",href:"#partitiondiffmode-dynamic-partition-values-filter"},"PartitionDiffMode")," below."),(0,i.kt)("h3",{id:"failifnopartitionvaluesmode"},"FailIfNoPartitionValuesMode"),(0,i.kt)("p",null,"The ",(0,i.kt)("em",{parentName:"p"},"FailIfNoPartitionValuesMode")," enforces to have specified partition values. It simply checks if partition values are present and fails otherwise.\nThis is useful to prevent potential reprocessing of whole tables due to wrong usage."),(0,i.kt)("h3",{id:"partitiondiffmode-dynamic-partition-values-filter"},"PartitionDiffMode: Dynamic partition values filter"),(0,i.kt)("p",null,"In contrast to specifying the partitions manually, you can let SmartDataLakeBuilder find missing partitions and set partition values automatically by specifying execution mode ",(0,i.kt)("em",{parentName:"p"},"PartitionDiffMode"),". This mode has a couple of options to fine tune:"),(0,i.kt)("p",null,"By defining the ",(0,i.kt)("strong",{parentName:"p"},"applyCondition")," attribute you can give a condition to decide at runtime if the PartitionDiffMode should be applied or not.\nDefault is to apply the PartitionDiffMode if the given partition values are empty (partition values from command line or passed from previous action).\nDefine an applyCondition by a spark sql expression working with attributes of DefaultExecutionModeExpressionData returning a boolean."),(0,i.kt)("p",null,"Example - apply also if given partition values are not empty, e.g. always:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'  applyCondition = "true"\n')),(0,i.kt)("p",null,"By defining the ",(0,i.kt)("strong",{parentName:"p"},"failCondition")," attribute you can give a condition to fail application of execution mode if ",(0,i.kt)("em",{parentName:"p"},"true"),".\nIt can be used to fail a run based on expected partitions, time and so on.\nThe expression is evaluated after execution of PartitionDiffMode. In the condition the following attributes are available amongst others to make the decision: ",(0,i.kt)("strong",{parentName:"p"},"inputPartitionValues"),", ",(0,i.kt)("strong",{parentName:"p"},"outputPartitionValues")," and ",(0,i.kt)("strong",{parentName:"p"},"selectedPartitionValues"),".\nDefault is that the application of the PartitionDiffMode does not fail the action. If there is no data to process, the following actions are skipped.\nDefine a failCondition by a spark sql expression working with attributes of PartitionDiffModeExpressionData returning a boolean."),(0,i.kt)("p",null,'Example - fail if partitions are not processed in strictly increasing order of partition column "dt":'),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'  failCondition = "(size(selectedPartitionValues) > 0 and array_min(transform(selectedPartitionValues, x -&gt x.dt)) &lt array_max(transform(outputPartitionValues, x > x.dt)))"\n')),(0,i.kt)("p",null,"Sometimes the failCondition can become quite complex with multiple terms concatenated by or-logic.\nTo improve interpretability of error messages, multiple fail conditions can be configured as array with attribute ",(0,i.kt)("strong",{parentName:"p"},"failConditions"),". For every condition you can also define a description which will be inserted into the error message."),(0,i.kt)("p",null,"The option ",(0,i.kt)("strong",{parentName:"p"},"selectExpression")," defines the selection of custom partitions.\nDefine a spark sql expression working with attributes of PartitionDiffModeExpressionData returning a Seq(Map(String,String)).\nExample - only process the last selected partition:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'  selectExpression = "slice(selectedPartitionValues,-1,1)"\n')),(0,i.kt)("p",null,"By defining ",(0,i.kt)("strong",{parentName:"p"},"alternativeOutputId")," attribute you can define another DataObject which will be used to check for already existing data.\nThis can be used to select data to process against a DataObject later in the pipeline."),(0,i.kt)("h3",{id:"custompartitionmode"},"CustomPartitionMode"),(0,i.kt)("p",null,"This execution mode allows for custom logic to select partitions to process in Scala."),(0,i.kt)("p",null,"Implement trait CustomPartitionModeLogic by defining a function which receives main input & output DataObject and returns partition values to process as Seq[Map","[String,String]","]"),(0,i.kt)("h2",{id:"incremental-load"},"Incremental load"),(0,i.kt)("p",null,"Some DataObjects are not partitioned, but nevertheless you don't want to read all data from the input on every run. You want to load it incrementally."),(0,i.kt)("p",null,"SDLB implements various methods to incrementally process data from the different DataObjects. Choosing an incremental mode depends the DataObjects used, e.g. do they support Spark Streaming, the transformations implemented, e.g. does Spark Streaming support these transformations, and also if you want a synchronous data pipeline or run all steps asynchronously to improve latency."),(0,i.kt)("h3",{id:"sparkstreamingmode"},"SparkStreamingMode"),(0,i.kt)("p",null,'This can be accomplished by specifying execution mode SparkStreamingMode. Under the hood it uses "Spark Structured Streaming".\nAn Action with SparkStreamingMode in streaming mode is an asynchronous action. Its rhythm can be configured by setting a ',(0,i.kt)("em",{parentName:"p"},"triggerType")," different from ",(0,i.kt)("em",{parentName:"p"},"Once")," and a triggerTime.\nIf not in streaming mode SparkStreamingMode triggers a single microbatch per SDLB job. Like this the Action is executed synchronous. It is configured by setting triggerType=Once. "),(0,i.kt)("p",null,'"Spark Structured Streaming" is keeping state information about processed data. It needs a ',(0,i.kt)("em",{parentName:"p"},"checkpointLocation")," configured which can be given as parameter to SparkStreamingMode."),(0,i.kt)("p",null,'Note that "Spark Structured Streaming" needs an input DataObject supporting the creation of streaming DataFrames.\nFor the time being, only the input sources delivered with Spark Streaming are supported.\nThis are KafkaTopicDataObject and all SparkFileDataObjects, see also ',(0,i.kt)("a",{parentName:"p",href:"https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets"},"Spark StructuredStreaming"),"."),(0,i.kt)("h3",{id:"dataframeincrementalmode"},"DataFrameIncrementalMode"),(0,i.kt)("p",null,"As not every input DataObject supports the creation of streaming DataFrames, e.g. JdbcTableDataObject, there is an other execution mode called SparkIncrementalMode.\nYou configure it by defining the attribute ",(0,i.kt)("strong",{parentName:"p"},"compareCol")," with a column name present in input and output DataObject.\nSparkIncrementalMode then compares the maximum values between input and output and creates a filter condition.\nOn execution the filter condition is applied to the input DataObject to load the missing increment.\nNote that compareCol needs to have a sortable datatype."),(0,i.kt)("p",null,"The ",(0,i.kt)("strong",{parentName:"p"},"applyCondition")," and ",(0,i.kt)("strong",{parentName:"p"},"alternativeOutputId")," attributes works the same as for ",(0,i.kt)("a",{parentName:"p",href:"executionModes#partitiondiffmode-dynamic-partition-values-filter"},"PartitionDiffMode"),"."),(0,i.kt)("p",null,"This execution mode has a performance drawback as it has to query the maximum value for ",(0,i.kt)("em",{parentName:"p"},"compareCol")," on input and output in each SDLB job."),(0,i.kt)("h3",{id:"dataobjectstateincrementalmode"},"DataObjectStateIncrementalMode"),(0,i.kt)("p",null,"Performance can be optimized by remembering the state of the last increment. The state is saved in a .json file for which the user needs to provide a path using the ",(0,i.kt)("inlineCode",{parentName:"p"},"--state-path")," option, see ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/commandLine"},"Command Line"),"."),(0,i.kt)("p",null,(0,i.kt)("em",{parentName:"p"},"DataObjectStateIncrementalMode")," needs a main input DataObject that can create incremental output, e.g. JdbcTableDataObject, SparkFileDataObject or AirbyteDataObject. The inner meaning of the state is defined by the DataObject and not interpreted outside of it. Some DataObjects have a configuration option to define the incremental output, e.g. JdbcTableDataObject.incrementalOutputExpr, others just use technical timestamps existing by design, e.g. SparkFileDataObject. "),(0,i.kt)("h3",{id:"fileincrementalmovemode"},"FileIncrementalMoveMode"),(0,i.kt)("p",null,"Another paradigm for incremental processing with files is to move or delete input files once they are processed. This can be achieved by using FileIncrementalMoveMode. If option ",(0,i.kt)("em",{parentName:"p"},"archiveSubdirectory")," is configured, files are moved into that directory after processing, otherwise they are deleted."),(0,i.kt)("p",null,"FileIncrementalMoveMode is the only execution mode that can be used with the file engine (see also ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/executionEngines"},"Execution engines"),"), but also with SparkFileDataObjects and the data frame engine."),(0,i.kt)("h2",{id:"others"},"Others"),(0,i.kt)("h3",{id:"processallmode"},"ProcessAllMode"),(0,i.kt)("p",null,"An execution mode which forces processing all data from it's inputs, removing partitionValues and filter conditions received from previous actions."),(0,i.kt)("h3",{id:"custommode"},"CustomMode"),(0,i.kt)("p",null,"This execution mode allows to implement aritrary processing logic using Scala."),(0,i.kt)("p",null,"Implement trait CustomModeLogic by defining a function which receives main input&output DataObject and returns a ExecutionModeResult. The result can contain input and output partition values, but also options which are passed to the transformations defined in the Action."))}c.isMDXComponent=!0}}]);