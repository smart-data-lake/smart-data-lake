"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[3725],{8008:(e,t,i)=>{i.r(t),i.d(t,{assets:()=>d,contentTitle:()=>s,default:()=>h,frontMatter:()=>o,metadata:()=>r,toc:()=>c});var n=i(4848),a=i(8453);const o={id:"executionModes",title:"Execution Modes"},s=void 0,r={id:"reference/executionModes",title:"Execution Modes",description:"Execution modes",source:"@site/docs/reference/executionModes.md",sourceDirName:"reference",slug:"/reference/executionModes",permalink:"/docs/reference/executionModes",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/executionModes.md",tags:[],version:"current",frontMatter:{id:"executionModes",title:"Execution Modes"},sidebar:"tutorialSidebar",previous:{title:"Execution Engines",permalink:"/docs/reference/executionEngines"},next:{title:"Transformations",permalink:"/docs/reference/transformations"}},d={},c=[{value:"Execution modes",id:"execution-modes",level:2},{value:"Partitions",id:"partitions",level:2},{value:"Default Behavior",id:"default-behavior",level:3},{value:"FailIfNoPartitionValuesMode",id:"failifnopartitionvaluesmode",level:3},{value:"PartitionDiffMode: Dynamic partition values filter",id:"partitiondiffmode-dynamic-partition-values-filter",level:3},{value:"applyCondition",id:"applycondition",level:4},{value:"failCondition",id:"failcondition",level:4},{value:"selectExpression",id:"selectexpression",level:4},{value:"alternativeOutputId",id:"alternativeoutputid",level:4},{value:"partitionColNb",id:"partitioncolnb",level:4},{value:"nbOfPartitionValuesPerRun",id:"nbofpartitionvaluesperrun",level:4},{value:"CustomPartitionMode",id:"custompartitionmode",level:3},{value:"ProcessAllMode",id:"processallmode",level:3},{value:"Incremental load",id:"incremental-load",level:2},{value:"DataFrameIncrementalMode",id:"dataframeincrementalmode",level:3},{value:"DataObjectStateIncrementalMode",id:"dataobjectstateincrementalmode",level:3},{value:"SparkStreamingMode",id:"sparkstreamingmode",level:3},{value:"KafkaStateIncrementalMode",id:"kafkastateincrementalmode",level:3},{value:"FileIncrementalMoveMode",id:"fileincrementalmovemode",level:3},{value:"Others",id:"others",level:2},{value:"CustomMode",id:"custommode",level:3}];function l(e){const t={a:"a",admonition:"admonition",br:"br",code:"code",em:"em",h2:"h2",h3:"h3",h4:"h4",li:"li",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,a.R)(),...e.components};return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(t.h2,{id:"execution-modes",children:"Execution modes"}),"\n",(0,n.jsx)(t.p,{children:"Execution modes select the data to be processed by a SmartDataLakeBuilder Action.\nBy default, there is no filter applied meaning that every Action reads all data passed on by the predecessor Action.\nIf an input has no predecessor Action in the DAG, all data is read from the corresponding DataObject."}),"\n",(0,n.jsx)(t.p,{children:"This default behavior is applied if you don't set an explicit execution mode.\nIf you want one of the execution modes described below, you have to explicitly set it on the Action:"}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{children:"executionMode {\n  type = PartitionDiffMode\n  attribute1 = ...\n}\n"})}),"\n",(0,n.jsx)(t.p,{children:"There are 2 major types of execution modes selecting the subset of data based on:"}),"\n",(0,n.jsxs)(t.ul,{children:["\n",(0,n.jsxs)(t.li,{children:[(0,n.jsx)(t.strong,{children:"partition-wise"}),": Either SDLB will automatically select and process missing partitions from the input, or the partitions are defined manually by a command line parameter."]}),"\n",(0,n.jsxs)(t.li,{children:[(0,n.jsx)(t.strong,{children:"incremental"}),": SDLB will automatically select new and updated data from the input and process data incrementally."]}),"\n"]}),"\n",(0,n.jsx)(t.h2,{id:"partitions",children:"Partitions"}),"\n",(0,n.jsxs)(t.p,{children:[(0,n.jsx)(t.em,{children:"Partition"})," is an ambiguous term.\nIt is used in distributed data processing (e.g. Spark tasks and repartitioning), but also for splitting data in storage for performance reasons,\ne.g. a partitioned database table, kafka topic or hadoop directory structure."]}),"\n",(0,n.jsx)(t.p,{children:"The use of partition in SDLB is about the latter case.\nSDLB supports intelligent processing of partitioned data (data split in storage) for optimal performance."}),"\n",(0,n.jsx)(t.admonition,{type:"info",children:(0,n.jsx)(t.p,{children:"Partition-wise data processing is one of the most performant data pipeline paradigms.\nBut it also limits data processing to always write or replace whole partitions.\nKeep that in mind when designing your Actions."})}),"\n",(0,n.jsxs)(t.p,{children:["To use partitioned data and partition-wise data processing in SDLB, you need to configure partition columns on a DataObject that can handle partitions.\ni.e. on a ",(0,n.jsx)(t.code,{children:"DeltaLakeTableDataObject"}),":"]}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{children:'partitionedDataObject {\n    type = DeltaLakeTableDataObject\n    table = {\n        name = "partitionedTable"\n    }\n    partitions = ["day"]\n}\n'})}),"\n",(0,n.jsx)(t.p,{children:"Note that not all DataObjects support partitioning."}),"\n",(0,n.jsx)(t.h3,{id:"default-behavior",children:"Default Behavior"}),"\n",(0,n.jsxs)(t.p,{children:["In its simplest form, you manually define the partition values to be processed by specifying the command line parameter ",(0,n.jsx)(t.code,{children:"--partition-values"})," or ",(0,n.jsx)(t.code,{children:"--multi-partition-values"}),", see ",(0,n.jsx)(t.a,{href:"/docs/reference/commandLine",children:"Command Line"}),".\nThis is in the form of columnName=columnValue, e.g. ",(0,n.jsx)(t.code,{children:"--partition-values day=2020-01-01"}),".\nThe partition values specified are passed to ",(0,n.jsx)(t.strong,{children:"all"})," start inputs (inputs with no predecessor in the DAG).\nIn other words, each input DataObject is filtered by the given partition values on the specified column(s)."]}),"\n",(0,n.jsxs)(t.p,{children:["If the parameters ",(0,n.jsx)(t.code,{children:"--partition-values"})," or ",(0,n.jsx)(t.code,{children:"--multi-partition-values"})," are not specified, SDLB will simply process all available data."]}),"\n",(0,n.jsx)(t.p,{children:"Subsequent Actions will automatically use the same partition values as their predecessor Action."}),"\n",(0,n.jsx)(t.admonition,{type:"info",children:(0,n.jsxs)(t.p,{children:["Actions can also have multiple input DataObjects (e.g. CustomDataFrameAction).\nIn this case, a ",(0,n.jsx)(t.em,{children:"main input"})," needs to be defined to take the partition values from.\nYou can either set the main input yourself by specifying the ",(0,n.jsx)(t.code,{children:"mainInputId"})," property on the Action, or SDLB will automatically choose the input.",(0,n.jsx)(t.br,{}),"\n","Automatic selection uses a heuristic:",(0,n.jsx)(t.br,{}),"\n","The first input which is not skipped, is not in the list of ",(0,n.jsx)(t.code,{children:"inputIdsToIgnoreFilter"}),", and has the most partition columns defined is chosen."]})}),"\n",(0,n.jsxs)(t.p,{children:["Another special case you might encounter is if an output DataObject has different partition columns than the input DataObject.\nIn these cases, the partition value columns of the input DataObject will be filtered and passed on to the next action.",(0,n.jsx)(t.br,{}),"\n","So if your input DataObject has [runId, division] as partition values and your output DataObject [runId],\nthen ",(0,n.jsx)(t.code,{children:"division"})," will be removed from the partition value columns before they are passed on."]}),"\n",(0,n.jsxs)(t.p,{children:["This default behaviour is active without providing any explicit ",(0,n.jsx)(t.code,{children:"executionMode"})," in the config of your Actions.\nThis means that processing partitioned data is available out-of-the-box in SDLB."]}),"\n",(0,n.jsxs)(t.admonition,{type:"info",children:[(0,n.jsxs)(t.p,{children:["The partition values act as filter.\nSDLB also validates that partition values actually exist in input DataObjects.\nIf they don't, execution will stop with an error.\nThis behavior can be changed by setting ",(0,n.jsx)(t.code,{children:"DataObject.expectedPartitionsCondition"}),"."]}),(0,n.jsx)(t.p,{children:"This is especially interesting with CustomDataFrameAction having multiple partitioned inputs,\nso you don't just join them if some of the input data is missing."})]}),"\n",(0,n.jsxs)(t.admonition,{type:"info",children:[(0,n.jsxs)(t.p,{children:["Another important caveat:",(0,n.jsx)(t.br,{}),"\n","Partition values filter all input DataObject ",(0,n.jsx)(t.em,{children:"that contain the given partition columns"}),".\nThis also means, that they don't have any effect on input DataObjects with different partition columns\nor no partitions at all."]}),(0,n.jsxs)(t.p,{children:["If you need to read everything from one DataObject, even though it does have the same partition columns,\nyou can again use ",(0,n.jsx)(t.code,{children:"CustomDataFrameAction.inputIdsToIgnoreFilter"})," to override the default behavior."]})]}),"\n",(0,n.jsx)(t.h3,{id:"failifnopartitionvaluesmode",children:"FailIfNoPartitionValuesMode"}),"\n",(0,n.jsxs)(t.p,{children:["If you use the method described above, you might want to set the executionMode to ",(0,n.jsx)(t.code,{children:"FailIfNoPartitionValuesMode"}),".\nThis mode enforces having partition values specified.\nIt simply checks if partition values are present and fails otherwise.\nThis is useful to prevent potential reprocessing of whole tables due to wrong usage."]}),"\n",(0,n.jsx)(t.h3,{id:"partitiondiffmode-dynamic-partition-values-filter",children:"PartitionDiffMode: Dynamic partition values filter"}),"\n",(0,n.jsxs)(t.p,{children:["Instead of specifying the partition values manually, you can let SDLB find missing partitions and set partition values automatically by specifying execution mode ",(0,n.jsx)(t.code,{children:"PartitionDiffMode"}),".\nIn its basic form, it compares input and output DataObjects and decides which partitions are missing in the output.\nIt then automatically uses these missing partitions as partition values."]}),"\n",(0,n.jsx)(t.admonition,{type:"info",children:(0,n.jsx)(t.p,{children:"Remember that a partition can exist in the output DataObject with no data or partial data only.\nFor SDLB, the partition exists and will not be processed again.\nKeep that in mind when recovering from errors."})}),"\n",(0,n.jsx)(t.p,{children:"This mode is quite powerful and has a couple of options to fine tune its behaviour."}),"\n",(0,n.jsx)(t.h4,{id:"applycondition",children:"applyCondition"}),"\n",(0,n.jsxs)(t.p,{children:["By default, the PartitionDiffMode is applied if the given partition values are empty (partition values from command line or passed from previous action).\nYou can override this behavior by defining an ",(0,n.jsx)(t.code,{children:"applyCondition"}),"."]}),"\n",(0,n.jsxs)(t.p,{children:["ApplyCondition is a spark sql expression working with attributes of ",(0,n.jsx)(t.a,{href:"https://github.com/smart-data-lake/smart-data-lake/blob/c435434d0174ca0862bdeef4838c6678aaef05fd/sdl-core/src/main/scala/io/smartdatalake/workflow/action/executionMode/ExecutionMode.scala#L164",children:"DefaultExecutionModeExpressionData"})," returning a boolean."]}),"\n",(0,n.jsx)(t.p,{children:"Simple Example - apply PartitionDiffMode even if partition values are given (always):"}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{children:'  applyCondition = "true"\n'})}),"\n",(0,n.jsx)(t.h4,{id:"failcondition",children:"failCondition"}),"\n",(0,n.jsxs)(t.p,{children:["If you have clear expectations of what your partition values should look like, you can enforce your rules by defining a ",(0,n.jsx)(t.code,{children:"failCondition"}),".\nIn it, you define a spark sql expression that is evaluated after the PartitionDiffMode is applied.\nIf it evaluates to ",(0,n.jsx)(t.code,{children:"true"}),", the Action will fail."]}),"\n",(0,n.jsxs)(t.p,{children:["In the condition, the following attributes are available amongst others to make the decision: ",(0,n.jsx)(t.code,{children:"inputPartitionValues"}),", ",(0,n.jsx)(t.code,{children:"outputPartitionValues"}),", ",(0,n.jsx)(t.code,{children:"selectedInputPartitionValues"})," and ",(0,n.jsx)(t.code,{children:"selectedOutputPartitionValues"}),".\nUse these to fail the run based on expected partitions or time conditions."]}),"\n",(0,n.jsxs)(t.p,{children:["Default is ",(0,n.jsx)(t.code,{children:"false"})," meaning that the application of the PartitionDiffMode does not fail the action.\nIf there is no data to process, the following actions are skipped."]}),"\n",(0,n.jsxs)(t.p,{children:["Example - fail if partitions are not processed in strictly increasing order of partition column ",(0,n.jsx)(t.code,{children:"dt"}),":"]}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{children:'  failCondition = "(size(selectedInputPartitionValues) > 0 and array_min(transform(selectedInputPartitionValues, x -> x.dt)) < array_max(transform(outputOutputPartitionValues, x -> x.dt)))"\n'})}),"\n",(0,n.jsxs)(t.p,{children:["Sometimes the ",(0,n.jsx)(t.code,{children:"failCondition"})," can become quite complex with multiple terms concatenated by or-logic.\nTo improve interpretability of error messages, multiple fail conditions can be configured as an array using the attribute ",(0,n.jsx)(t.code,{children:"failConditions"}),".\nFor each condition you can also define a description which will be inserted into the error message."]}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{children:'  failConditions = [{\n    expression = "(size(selectedInputPartitionValues) > 0 and array_min(transform(selectedInputPartitionValues, x -> x.dt)) < array_max(transform(outputOutputPartitionValues, x -> x.dt)))"\n    description = "fail if partitions are not processed in strictly increasing order of partition column dt"\n  }]\n'})}),"\n",(0,n.jsx)(t.h4,{id:"selectexpression",children:"selectExpression"}),"\n",(0,n.jsxs)(t.p,{children:["The option ",(0,n.jsx)(t.code,{children:"selectExpression"})," refines or overrides the list of selected output partitions. It can access the partition values selected by the default behaviour and refine the list, or it can override selected partition values by using input & output partition values directly.\nDefine a spark sql expression working with attributes of PartitionDiffModeExpressionData returning a ",(0,n.jsx)(t.code,{children:"Seq(Map(String,String))"}),"."]}),"\n",(0,n.jsx)(t.p,{children:"Example - only process the last selected partition:"}),"\n",(0,n.jsx)(t.pre,{children:(0,n.jsx)(t.code,{children:'  selectExpression = "slice(selectedInputPartitionValues,-1,1)"\n'})}),"\n",(0,n.jsx)(t.h4,{id:"alternativeoutputid",children:"alternativeOutputId"}),"\n",(0,n.jsxs)(t.p,{children:["Usually, PartitionDiffMode will check for missing partitions against the main output DataObject.\nYou can force PartitionDiffMode to check for missing partitions against another DataObject by defining ",(0,n.jsx)(t.code,{children:"alternativeOutputId"}),"."]}),"\n",(0,n.jsx)(t.p,{children:"This can even be a DataObject that comes later in your pipeline so it doesn't have to be one of the Actions output DataObjects."}),"\n",(0,n.jsx)(t.h4,{id:"partitioncolnb",children:"partitionColNb"}),"\n",(0,n.jsx)(t.p,{children:"When comparing which partitions and subpartitions need to be transferred, usually all the parition columns are used.\nWith these setting, you can limit the amount of columns used for the comparison to the first N columns."}),"\n",(0,n.jsx)(t.h4,{id:"nbofpartitionvaluesperrun",children:"nbOfPartitionValuesPerRun"}),"\n",(0,n.jsxs)(t.p,{children:["If you have a lot of partitions, you might want to limit the number of partitions processed per run.\nIf you define ",(0,n.jsx)(t.code,{children:"nbOfPartitionValuesPerRun"}),", PartitionDiffMode will only process the first n partitions and ignore the rest."]}),"\n",(0,n.jsx)(t.h3,{id:"custompartitionmode",children:"CustomPartitionMode"}),"\n",(0,n.jsxs)(t.p,{children:["This execution mode allows for complete customized logic to select partitions to process in Scala.\nImplement trait ",(0,n.jsx)(t.code,{children:"CustomPartitionModeLogic"})," by defining a function which receives main input & output DataObjects and returns partition values to process as ",(0,n.jsx)(t.code,{children:"Seq[Map[String,String]]"}),".\nThe contents of the command-line parameters ",(0,n.jsx)(t.code,{children:"--partition-values"})," and ",(0,n.jsx)(t.code,{children:"--multi-partition-values"})," is provided in the input argument ",(0,n.jsx)(t.code,{children:"givenPartitionValues"}),".\nYou are free to use or ignore this information in your custom execution mode. You can also use a ",(0,n.jsx)(t.code,{children:"CustomPartitionMode"})," together with the Default Behavior in the same DAG run by having some Actions define a ",(0,n.jsx)(t.code,{children:"CustomPartitionMode"})," and others not. For example, you can partition transaction data by day and select individual days using ",(0,n.jsx)(t.code,{children:"--partition-values"}),", but fetch master data based on a different, custom logic."]}),"\n",(0,n.jsx)(t.h3,{id:"processallmode",children:"ProcessAllMode"}),"\n",(0,n.jsx)(t.p,{children:"An execution mode which forces processing of all data from its inputs.\nAny partitionValues and filter conditions received from previous actions are ignored."}),"\n",(0,n.jsx)(t.h2,{id:"incremental-load",children:"Incremental load"}),"\n",(0,n.jsx)(t.p,{children:"Some DataObjects are not partitioned, but nevertheless you don't want to read all data from the input on every run.\nYou want to load it incrementally."}),"\n",(0,n.jsx)(t.p,{children:"SDLB implements various methods to incrementally process data from various DataObjects.\nChoosing an incremental mode depends on the DataObjects used.\nDo they support Spark Streaming?\nDoes Spark Streaming support the transformations you're using?\nDo you want a synchronous data pipeline or run all steps asynchronously to improve latency?"}),"\n",(0,n.jsx)(t.h3,{id:"dataframeincrementalmode",children:"DataFrameIncrementalMode"}),"\n",(0,n.jsxs)(t.p,{children:["One of the most common forms of incremental processing is ",(0,n.jsx)(t.code,{children:"DataFrameIncrementalMode"}),".\nYou configure it by defining the attribute ",(0,n.jsx)(t.code,{children:"compareCol"})," naming a column that exists in both the input and output DataObject.\n",(0,n.jsx)(t.code,{children:"DataFrameIncrementalMode"})," then compares the maximum values between input and output to decide what needs to be loaded."]}),"\n",(0,n.jsxs)(t.p,{children:["For this mode to work, ",(0,n.jsx)(t.code,{children:"compareCol"})," needs to be of a sortable datatype like int or timestamp."]}),"\n",(0,n.jsxs)(t.p,{children:["The attributes ",(0,n.jsx)(t.code,{children:"applyCondition"})," and ",(0,n.jsx)(t.code,{children:"alternativeOutputId"})," work the same as for ",(0,n.jsx)(t.a,{href:"executionModes#partitiondiffmode-dynamic-partition-values-filter",children:"PartitionDiffMode"}),"."]}),"\n",(0,n.jsx)(t.admonition,{type:"info",children:(0,n.jsxs)(t.p,{children:["This execution mode has a performance drawback as it has to query the maximum value for ",(0,n.jsx)(t.code,{children:"compareCol"})," on input and output DataObjects each time."]})}),"\n",(0,n.jsx)(t.h3,{id:"dataobjectstateincrementalmode",children:"DataObjectStateIncrementalMode"}),"\n",(0,n.jsxs)(t.p,{children:["To optimize performance, SDLB can remember the state of the last increment it successfully loaded.\nFor this mode to work, you need to start SDLB with a ",(0,n.jsx)(t.code,{children:"--state-path"}),", see ",(0,n.jsx)(t.a,{href:"/docs/reference/commandLine",children:"Command Line"}),".\nThe .json file used to store the overall state of your application will be extended with state information for this DataObject."]}),"\n",(0,n.jsxs)(t.p,{children:[(0,n.jsx)(t.code,{children:"DataObjectStateIncrementalMode"})," needs a main input DataObject that can create incremental output, e.g. ",(0,n.jsx)(t.code,{children:"JdbcTableDataObject"}),", ",(0,n.jsx)(t.code,{children:"SparkFileDataObject"})," or ",(0,n.jsx)(t.code,{children:"AirbyteDataObject"}),".\nThe inner meaning of the state is defined by the DataObject and not interpreted outside of it.\nSome DataObjects have a configuration option to define the incremental output, e.g. ",(0,n.jsx)(t.code,{children:"JdbcTableDataObject.incrementalOutputExpr"}),", others just use technical timestamps existing by design,\ne.g. ",(0,n.jsx)(t.code,{children:"SparkFileDataObject"}),"."]}),"\n",(0,n.jsx)(t.admonition,{type:"info",children:(0,n.jsxs)(t.p,{children:["This option also comes in handy if you can't use ",(0,n.jsx)(t.code,{children:"DataFrameIncrementalMode"})," because you can't access the output DataObject during initialization.\nFor example, if you push incremental Parquet files to a remote storage and these files are immediately processed and removed,\nyou will find an empty directory and therefore can't consult already uploaded data.\nIn this case, SDLB needs to remember what data increments were already uploaded."]})}),"\n",(0,n.jsx)(t.h3,{id:"sparkstreamingmode",children:"SparkStreamingMode"}),"\n",(0,n.jsxs)(t.p,{children:["SDLB also supports streaming with the ",(0,n.jsx)(t.code,{children:"SparkStreamingMode"}),".\nUnder the hood it uses ",(0,n.jsx)(t.a,{href:"https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html",children:"Spark Structured Streaming"}),".\nAn Action with SparkStreamingMode in streaming mode is an asynchronous action."]}),"\n",(0,n.jsxs)(t.p,{children:["Its rhythm can be configured by setting a ",(0,n.jsx)(t.code,{children:"triggerType"})," to either ",(0,n.jsx)(t.code,{children:"Once"}),", ",(0,n.jsx)(t.code,{children:"ProcessingTime"})," or ",(0,n.jsx)(t.code,{children:"Continuous"})," (default is ",(0,n.jsx)(t.code,{children:"Once"}),").\nWhen using ",(0,n.jsx)(t.code,{children:"ProcessingTime"})," or ",(0,n.jsx)(t.code,{children:"Continuous"})," you can configure the interval through the attribute ",(0,n.jsx)(t.code,{children:"triggerTime"}),"."]}),"\n",(0,n.jsx)(t.admonition,{type:"info",children:(0,n.jsxs)(t.p,{children:["You need to start your SDLB run with the ",(0,n.jsx)(t.code,{children:"--streaming"})," flag to enable streaming mode.\nIf you don't, SDLB will always use ",(0,n.jsx)(t.code,{children:"Once"})," as trigger type (single microbatch) and the action is executed synchronously."]})}),"\n",(0,n.jsxs)(t.p,{children:["Spark Structured Streaming is keeping state information about processed data.\nTo do so, it needs a ",(0,n.jsx)(t.code,{children:"checkpointLocation"})," configured on the SparkStreamingMode."]}),"\n",(0,n.jsxs)(t.p,{children:["Note that ",(0,n.jsx)(t.em,{children:"Spark Structured Streaming"})," needs an input DataObject that supports the creation of streaming DataFrames.\nFor the time being, only the input sources delivered with Spark Streaming are supported.\nThese are ",(0,n.jsx)(t.code,{children:"KafkaTopicDataObject"})," and all ",(0,n.jsx)(t.code,{children:"SparkFileDataObjects"}),", see also ",(0,n.jsx)(t.a,{href:"https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets",children:"Spark StructuredStreaming"}),"."]}),"\n",(0,n.jsx)(t.h3,{id:"kafkastateincrementalmode",children:"KafkaStateIncrementalMode"}),"\n",(0,n.jsx)(t.p,{children:"A special incremental execution mode for Kafka inputs, remembering the state from the last increment through the Kafka Consumer,\ne.g. committed offsets."}),"\n",(0,n.jsx)(t.h3,{id:"fileincrementalmovemode",children:"FileIncrementalMoveMode"}),"\n",(0,n.jsxs)(t.p,{children:["Another paradigm for incremental processing with files is to move or delete input files once they are processed.\nThis can be achieved by using FileIncrementalMoveMode. If option ",(0,n.jsx)(t.code,{children:"archiveSubdirectory"})," is configured, files are moved into that directory after processing, otherwise they are deleted."]}),"\n",(0,n.jsxs)(t.p,{children:["FileIncrementalMoveMode can be used with the file engine (see also ",(0,n.jsx)(t.a,{href:"/docs/reference/executionEngines",children:"Execution engines"}),"), but also with SparkFileDataObjects and the data frame engine."]}),"\n",(0,n.jsx)(t.h2,{id:"others",children:"Others"}),"\n",(0,n.jsx)(t.h3,{id:"custommode",children:"CustomMode"}),"\n",(0,n.jsx)(t.p,{children:"This execution mode allows to implement arbitrary processing logic using Scala."}),"\n",(0,n.jsxs)(t.p,{children:["Implement trait ",(0,n.jsx)(t.code,{children:"CustomModeLogic"})," by defining a function which receives main input and output DataObjects and returns an ",(0,n.jsx)(t.code,{children:"ExecutionModeResult"}),".\nThe result can contain input and output partition values, but also options which are passed to the transformations defined in the Action."]})]})}function h(e={}){const{wrapper:t}={...(0,a.R)(),...e.components};return t?(0,n.jsx)(t,{...e,children:(0,n.jsx)(l,{...e})}):l(e)}},8453:(e,t,i)=>{i.d(t,{R:()=>s,x:()=>r});var n=i(6540);const a={},o=n.createContext(a);function s(e){const t=n.useContext(o);return n.useMemo((function(){return"function"==typeof e?e(t):{...t,...e}}),[t,e])}function r(e){let t;return t=e.disableParentContext?"function"==typeof e.components?e.components(a):e.components||a:s(e.components),n.createElement(o.Provider,{value:t},e.children)}}}]);