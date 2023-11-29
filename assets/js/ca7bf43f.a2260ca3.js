"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[9850],{4359:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>l,contentTitle:()=>a,default:()=>p,frontMatter:()=>r,metadata:()=>s,toc:()=>d});var i=t(5893),o=t(1151);const r={title:"Housekeeping",description:"Use advanced housekeeping functions to optimize execution",slug:"sdl-housekeeping",authors:[{name:"Patrick Gr\xfctter",url:"https://github.com/pgruetter"}],tags:["housekeeping","performance","partitioning"],hide_table_of_contents:!1},a="Housekeeping",s={permalink:"/blog/sdl-housekeeping",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2023-06-08-SDL-Housekeeping/2023-06-08-housekeeping.md",source:"@site/blog/2023-06-08-SDL-Housekeeping/2023-06-08-housekeeping.md",title:"Housekeeping",description:"Use advanced housekeeping functions to optimize execution",date:"2023-06-08T00:00:00.000Z",formattedDate:"June 8, 2023",tags:[{label:"housekeeping",permalink:"/blog/tags/housekeeping"},{label:"performance",permalink:"/blog/tags/performance"},{label:"partitioning",permalink:"/blog/tags/partitioning"}],readingTime:5.87,hasTruncateMarker:!1,authors:[{name:"Patrick Gr\xfctter",url:"https://github.com/pgruetter"}],frontMatter:{title:"Housekeeping",description:"Use advanced housekeeping functions to optimize execution",slug:"sdl-housekeeping",authors:[{name:"Patrick Gr\xfctter",url:"https://github.com/pgruetter"}],tags:["housekeeping","performance","partitioning"],hide_table_of_contents:!1},unlisted:!1,prevItem:{title:"Data Mesh with SDL",permalink:"/blog/sdl-data-mesh"},nextItem:{title:"Incremental historization using CDC and Airbyte MSSQL connector",permalink:"/blog/sdl-hist"}},l={authorsImageUrls:[void 0]},d=[{value:"Context",id:"context",level:2},{value:"Partitioning",id:"partitioning",level:2},{value:"Drawback",id:"drawback",level:2},{value:"HousekeepingMode",id:"housekeepingmode",level:2},{value:"PartitionArchiveCompactionMode",id:"partitionarchivecompactionmode",level:3},{value:"Archive",id:"archive",level:4},{value:"Compaction",id:"compaction",level:4},{value:"PartitionRetentionMode",id:"partitionretentionmode",level:3},{value:"Result",id:"result",level:2}];function c(e){const n={a:"a",admonition:"admonition",br:"br",code:"code",h2:"h2",h3:"h3",h4:"h4",li:"li",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,o.a)(),...e.components};return(0,i.jsxs)(i.Fragment,{children:[(0,i.jsx)(n.p,{children:"In this article, we're taking a look on how we use SDLB's housekeeping features to keep our pipelines running efficiently."}),"\n",(0,i.jsxs)(n.p,{children:["Some DataObject contain housekeeping features of their own.\nMake sure you use them!\nFor example, Delta Tables support commands like ",(0,i.jsx)(n.code,{children:"optimize"})," and ",(0,i.jsx)(n.code,{children:"vacuum"})," to optimize storage and delete no longer needed files."]}),"\n",(0,i.jsx)(n.p,{children:"But usually, those commands do not re-organize your partitions.\nThis is where SDLBs housekeeping mode comes in."}),"\n",(0,i.jsx)(n.p,{children:"The example is taken from a real world project we've implemented."}),"\n",(0,i.jsx)(n.h2,{id:"context",children:"Context"}),"\n",(0,i.jsx)(n.p,{children:"In this particular project we are collecting data from various reporting units and process it in batches.\nThe reporting units use an Azure Function to upload JSON files to an Azure Data Lake Storage.\nFrom there, we pick them up for validation and processing.\nReporting units can upload data anytime, but it is only processed a few times a day."}),"\n",(0,i.jsx)(n.p,{children:"Once validated, we use Delta Lake tables in Databricks to process data through the layers of the Lakehouse."}),"\n",(0,i.jsx)(n.h2,{id:"partitioning",children:"Partitioning"}),"\n",(0,i.jsxs)(n.p,{children:["The Azure Function puts uploaded JSON files in a subfolder for each reporting unit.\nAs such, JSON files are already neatly partitioned by ",(0,i.jsx)(n.code,{children:"reporting_unit"}),":"]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{children:"uploadFolder/\n  reporting_unit=rp01\n    file1.json\n    file2.json\n    file3.json\n  reporting_unit=rp02\n    file1.json\n  reporting_unit=rp03\n    fileX.json\n"})}),"\n",(0,i.jsx)(n.p,{children:"To read these JSON files, we can therefore use the following DataObject definition:"}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{children:"import_json {\n    type = JsonFileDataObject\n    path = uploadFolder/\n    partitions = [reporting_unit]    \n}\n"})}),"\n",(0,i.jsxs)(n.p,{children:["These files are then processed with a ",(0,i.jsx)(n.code,{children:"FileTransferAction"})," into an output DataObject ",(0,i.jsx)(n.code,{children:"stage_json"}),":"]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{children:"stage_json {\n    type = FileTransferAction\n    inputId = import_json\n    outputId = stage_json\n    executionMode = { type = FileIncrementalMoveMode }\n    metadata.feed = stage_json\n}\n"})}),"\n",(0,i.jsxs)(n.p,{children:["Each time we start to process uploaded data, we use the ",(0,i.jsx)(n.code,{children:"run_id"})," to keep track of all batch jobs and version of files delivered.\nIf you use a state path (see ",(0,i.jsx)(n.a,{href:"../../docs/reference/commandLine",children:"commandLine"}),"),\nyour runs automatically generate a ",(0,i.jsx)(n.code,{children:"run_id"})," to identify the run, and you can use it by extending your DataObject:"]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{children:'stage_json {\n    type = JsonFileDataObject\n    path = processedFolder\n    partitions = [run_id,reporting_unit]\n    schema = """reporting_unit string, run_id string, ...."""\n}\n'})}),"\n",(0,i.jsxs)(n.p,{children:["Note how we just use run_id as part of the schema without any further declaration.\nSince we use the state path, SDLB uses a ",(0,i.jsx)(n.code,{children:"run_id"})," internally, and if it's referenced as partition column in a DataObject, processed data get automatically assigned the id of the current run."]}),"\n",(0,i.jsx)(n.h2,{id:"drawback",children:"Drawback"}),"\n",(0,i.jsxs)(n.p,{children:["Let's take a look at the resulting partition layout of ",(0,i.jsx)(n.code,{children:"stage_json"}),":"]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{children:"processedFolder/\n  run_id=1/\n    reporting_unit=rp01/\n      file1.json\n      file2.json\n      file3.json\n    reporting_unit=rp02/\n      file1.json\n    reporting_unit=rp03/\n      fileX.json\n"})}),"\n",(0,i.jsxs)(n.p,{children:["This partition layout has many advantages in our case as we know exactly\nduring which run a particular file was processed and which reporting unit uploaded it.\nIn further stages we can clearly work with files that were processed in the current run and not touch any old ",(0,i.jsx)(n.code,{children:"run_id"}),"s."]}),"\n",(0,i.jsx)(n.p,{children:"For this use case, a few things are important to note:"}),"\n",(0,i.jsxs)(n.ul,{children:["\n",(0,i.jsx)(n.li,{children:"Some reporting units don't upload data for days. You end up with only a few reporting_unit partitions per run_id."}),"\n",(0,i.jsx)(n.li,{children:"File sizes are rather small (< 1 MiB), partition sizes end up very small too."}),"\n",(0,i.jsx)(n.li,{children:"If you use hourly runs and run 24/7, you end up with 168 partitions per week, plus sub-partitions for reporting units."}),"\n",(0,i.jsx)(n.li,{children:"Once files are correctly processed, we don't read the uploaded files anymore.\nWe still keep them as raw files should we ever need to re-process them."}),"\n"]}),"\n",(0,i.jsx)(n.p,{children:"The drawback becomes apparent when you have actions working with all partitions, they will become very slow.\nSpark doesn't like a lot of small partitions."}),"\n",(0,i.jsx)(n.p,{children:"To mitigate that, we use SDLB's Housekeeping Feature."}),"\n",(0,i.jsx)(n.h2,{id:"housekeepingmode",children:"HousekeepingMode"}),"\n",(0,i.jsxs)(n.p,{children:["If you take a look at DataObject's parameters, you will see a ",(0,i.jsx)(n.code,{children:"housekeepingMode"}),".\nThere are two modes available:"]}),"\n",(0,i.jsxs)(n.ul,{children:["\n",(0,i.jsxs)(n.li,{children:[(0,i.jsx)(n.strong,{children:"PartitionArchiveCompactionMode"}),": to compact / archive partitions"]}),"\n",(0,i.jsxs)(n.li,{children:[(0,i.jsx)(n.strong,{children:"PartitionRetentionMode"}),": to delete certain partitions completely"]}),"\n"]}),"\n",(0,i.jsx)(n.h3,{id:"partitionarchivecompactionmode",children:"PartitionArchiveCompactionMode"}),"\n",(0,i.jsx)(n.p,{children:"In this mode, you solve two tasks at once:"}),"\n",(0,i.jsxs)(n.ul,{children:["\n",(0,i.jsx)(n.li,{children:"You define how many smaller partitions are aggregated into one larger partition (archive)"}),"\n",(0,i.jsx)(n.li,{children:"Rewrite all files in a partition to combine many small files into larger files (compact)"}),"\n"]}),"\n",(0,i.jsx)(n.h4,{id:"archive",children:"Archive"}),"\n",(0,i.jsxs)(n.p,{children:["In our example above, we stated that we don't want to alter any input files, so we won't use compaction.\nWe want to keep them as is (raw data).\nBut we do want to get rid of all the small partitions after a certain amount of time.\nFor that, we extend ",(0,i.jsx)(n.code,{children:"stage_json"})," to include the ",(0,i.jsx)(n.code,{children:"housekeepingMode"})," with a ",(0,i.jsx)(n.code,{children:"archivePartitionExpression"}),":"]}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{children:'stage_json {\n    type = JsonFileDataObject\n    path = processedFolder\n    partitions = [run_id,reporting_unit]\n    schema = """reporting_unit string, run_id string, ...."""\n    housekeepingMode = {\n      type = PartitionArchiveCompactionMode\n      archivePartitionExpression = "if( elements.run_id < (runId - 500), map(\'run_id\', (cast(elements.run_id as integer) div 500) * 500, \'reporting_unit\', elements.reporting_unit), elements)"\n    }\n}\n'})}),"\n",(0,i.jsxs)(n.p,{children:["This expression probably needs some explanation:",(0,i.jsx)(n.br,{}),"\n","The Spark SQL expression works with attributes of ",(0,i.jsx)(n.a,{href:"https://github.com/smart-data-lake/smart-data-lake/blob/master-spark3/sdl-core/src/main/scala/io/smartdatalake/workflow/dataobject/HousekeepingMode.scala#L136",children:(0,i.jsx)(n.code,{children:"PartitionExpressionData"})}),".\nIn this case we use ",(0,i.jsx)(n.code,{children:"runId"})," (the current runId) and ",(0,i.jsx)(n.code,{children:"elements"})," (all partition values as map(string,string)).\nIt needs to return a map(string,string) to define new partition values.\nIn our case, it needs to define ",(0,i.jsx)(n.code,{children:"run_id"})," and ",(0,i.jsx)(n.code,{children:"reporting_unit"})," because these are the partitions defined in ",(0,i.jsx)(n.code,{children:"stage_json"}),"."]}),"\n",(0,i.jsxs)(n.p,{children:["Let's take the expression apart:",(0,i.jsx)(n.br,{}),"\n",(0,i.jsx)(n.code,{children:"if(elements.run_id < (runId - 500), ..."}),(0,i.jsx)(n.br,{}),"\n","Only archive the partition if it's runId is older than 500 run_ids ago."]}),"\n",(0,i.jsxs)(n.p,{children:[(0,i.jsx)(n.code,{children:"map('run_id', (cast(elements.run_id as integer) div 500) * 500, 'reporting_unit', elements.reporting_unit)"}),(0,i.jsx)(n.br,{}),"\n","Creates the map with the new values for the partitions.\nThe run_id is floored to the next 500 value, so as example, the new value of run_id 1984 will be 1500 (because integer 1984/500=3, 3*500=1500).",(0,i.jsx)(n.br,{}),"\n","Remember that we need to return all partition values in the map, also the ones we don't want to alter.\nFor ",(0,i.jsx)(n.code,{children:"reporting_unit"})," we simply return the existing value ",(0,i.jsx)(n.code,{children:"elements.reporting_unit"}),"."]}),"\n",(0,i.jsxs)(n.p,{children:[(0,i.jsx)(n.code,{children:"..., elements)"}),(0,i.jsx)(n.br,{}),"\n","This is the else condition and simply returns the existing partition values if there is nothing to archive."]}),"\n",(0,i.jsx)(n.admonition,{type:"info",children:(0,i.jsx)(n.p,{children:"The housekeeping mode is applied after writing a DataObject.\nKeep in mind, that it is executed with every run."})}),"\n",(0,i.jsx)(n.h4,{id:"compaction",children:"Compaction"}),"\n",(0,i.jsxs)(n.p,{children:["We don't want to compact files in our case.\nBut from the documentation you can see that compaction works very similarly:",(0,i.jsx)(n.br,{}),"\n","You also work with attributes from ",(0,i.jsx)(n.code,{children:"PartitionExpressionData"})," but instead of new partition values,\nyou return a boolean to indicate for each partition if it should be compacted or not."]}),"\n",(0,i.jsx)(n.h3,{id:"partitionretentionmode",children:"PartitionRetentionMode"}),"\n",(0,i.jsx)(n.p,{children:"Again, not used in our example as we never delete old files.\nBut if you need to, you define a Spark SQL expression returning a boolean indicating if a partition should be retained or deleted."}),"\n",(0,i.jsx)(n.pre,{children:(0,i.jsx)(n.code,{children:'stage_json {\n    type = JsonFileDataObject\n    path = processedFolder\n    partitions = [run_id,reporting_unit]\n    schema = """reporting_unit string, run_id string, ...."""\n    housekeepingMode = {\n      type = PartitionRetentionMode\n      retentionCondition = "elements.run_id > (runId - 500)"\n    }\n}\n'})}),"\n",(0,i.jsx)(n.h2,{id:"result",children:"Result"}),"\n",(0,i.jsx)(n.p,{children:"In our example, we had performance gradually decreasing because Spark had to read more than 10'000 partitions and subpartitions.\nJust listing all available partitions, even if you only worked with the most recent one, took a few minutes and these operations added up."}),"\n",(0,i.jsx)(n.p,{children:"With the housekeeping mode enabled, older partitions continuously get merged into larger partitions containing up to 500 runs.\nThis brought the duration of list operations back to a few seconds."}),"\n",(0,i.jsx)(n.p,{children:"The operations are fully automated, no manual intervention is required."})]})}function p(e={}){const{wrapper:n}={...(0,o.a)(),...e.components};return n?(0,i.jsx)(n,{...e,children:(0,i.jsx)(c,{...e})}):c(e)}},1151:(e,n,t)=>{t.d(n,{Z:()=>s,a:()=>a});var i=t(7294);const o={},r=i.createContext(o);function a(e){const n=i.useContext(r);return i.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function s(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(o):e.components||o:a(e.components),i.createElement(r.Provider,{value:n},e.children)}}}]);