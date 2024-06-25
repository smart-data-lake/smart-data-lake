"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[5323],{6128:(e,a,t)=>{t.r(a),t.d(a,{assets:()=>c,contentTitle:()=>r,default:()=>A,frontMatter:()=>o,metadata:()=>s,toc:()=>l});var n=t(4848),i=t(8453);const o={id:"notebookCatalog",title:"Use in Notebooks"},r=void 0,s={id:"reference/notebookCatalog",title:"Use in Notebooks",description:"SDLB can also be used in Notebooks. On one side SDLB can provide a high-quality catalog of data objects, and on the other side it helps to easily prototype transformations in an interactive Notebook environment, and then transfer them to repository for production use.",source:"@site/docs/reference/notebookCatalog.md",sourceDirName:"reference",slug:"/reference/notebookCatalog",permalink:"/docs/reference/notebookCatalog",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/notebookCatalog.md",tags:[],version:"current",frontMatter:{id:"notebookCatalog",title:"Use in Notebooks"},sidebar:"tutorialSidebar",previous:{title:"Testing",permalink:"/docs/reference/testing"},next:{title:"Troubleshooting",permalink:"/docs/reference/troubleshooting"}},c={},l=[{value:"DataObject and Action Catalog",id:"dataobject-and-action-catalog",level:2},{value:"Using DataObjects",id:"using-dataobjects",level:2},{value:"Using Actions, Writing Transformations",id:"using-actions-writing-transformations",level:2},{value:"Define a new Transformers:",id:"define-a-new-transformers",level:2},{value:"Develop a new Action",id:"develop-a-new-action",level:2},{value:"Summary",id:"summary",level:2}];function d(e){const a={admonition:"admonition",code:"code",em:"em",h2:"h2",img:"img",li:"li",p:"p",pre:"pre",ul:"ul",...(0,i.R)(),...e.components};return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsx)(a.p,{children:"SDLB can also be used in Notebooks. On one side SDLB can provide a high-quality catalog of data objects, and on the other side it helps to easily prototype transformations in an interactive Notebook environment, and then transfer them to repository for production use."}),"\n",(0,n.jsx)(a.p,{children:"This page describes configuration of the project, installation in Databricks Notebooks and shows some examples."}),"\n",(0,n.jsx)(a.h2,{id:"dataobject-and-action-catalog",children:"DataObject and Action Catalog"}),"\n",(0,n.jsx)(a.p,{children:"By taking advantage of Notebooks Scala code completion functionalty, one can access DataObjects and Actions from the SDLB configuration in a Notebook like a catalog. This requires generating a corresponding catalog class with a variable for each DataObject and Action."}),"\n",(0,n.jsxs)(a.p,{children:["SDLB includes the LabCatalogGenerator command line tool, which can generate the corresponding Scala code. The generated code is compiled in a second compile phase and added to the dl-pipelines.jar. The LabCatalogGenerator can be activated by the following Maven Profile ",(0,n.jsx)(a.code,{children:"generate-catalog"})," (to be added to the project pom.xml) when executing maven ",(0,n.jsx)(a.code,{children:"package"})," command:"]}),"\n",(0,n.jsx)(a.pre,{children:(0,n.jsx)(a.code,{className:"language-xml",children:"<profile>\n    <id>generate-catalog</id>\n    <build>\n        <plugins>\n            \x3c!-- generate catalog scala code. --\x3e\n            <plugin>\n                <groupId>org.codehaus.mojo</groupId>\n                <artifactId>exec-maven-plugin</artifactId>\n                <version>3.1.0</version>\n                <executions>\n                    <execution>\n                        <id>generate-catalog</id>\n                        <phase>prepare-package</phase>\n                        <goals>\n                            <goal>java</goal>\n                        </goals>\n                        <configuration>\n                            <mainClass>io.smartdatalake.lab.LabCatalogGenerator</mainClass>\n                            <arguments>\n                                <argument>--config</argument>\n                                <argument>./config</argument>\n                                <argument>--srcDirectory</argument>\n                                <argument>./src/main/scala-generated</argument>\n                            </arguments>\n                            <classpathScope>compile</classpathScope>\n                        </configuration>\n                    </execution>\n                </executions>\n            </plugin>\n            \x3c!-- Compiles Scala sources incl. generated catalog --\x3e\n            <plugin>\n                <groupId>net.alchim31.maven</groupId>\n                <artifactId>scala-maven-plugin</artifactId>\n                <executions>\n                    \x3c!-- add additional execution to compile generated catalog (see id generate-catalog) --\x3e\n                    <execution>\n                        <id>compile-catalog</id>\n                        <phase>prepare-package</phase>\n                        <goals>\n                            <goal>compile</goal>\n                        </goals>\n                        <configuration>\n                            <sourceDir>./src/main/scala-generated</sourceDir>\n                        </configuration>\n                    </execution>\n                </executions>\n            </plugin>\n        </plugins>\n    </build>\n</profile>\n"})}),"\n",(0,n.jsxs)(a.p,{children:["The LabCatalogGenerator creates source code for class ",(0,n.jsx)(a.code,{children:"ActionCatalog"})," and ",(0,n.jsx)(a.code,{children:"DataObjectCatalog"})," in package ",(0,n.jsx)(a.code,{children:"io.smartdatalake.generated"}),". These names can be customized by command line parameters. The directory for the generated source code has to be provided as ",(0,n.jsx)(a.code,{children:"srcDirectory"})," parameter."]}),"\n",(0,n.jsxs)(a.p,{children:["Once generated, the fat-jar (created with maven profile ",(0,n.jsx)(a.code,{children:"fat-jar"}),") and SDLB configuration files have to be uploaded into a Databricks Volume. Then create a Databricks Cluster with the fat-jar as Library and Environment Variable ",(0,n.jsx)(a.code,{children:"JNAME=zulu17-ca-amd64"})," to use Java 17."]}),"\n",(0,n.jsx)(a.p,{children:"Make sure your SDLB data pipeline runs at least once to ensure that tables exist. A simple Databricks Job configuration can look as follows:"}),"\n",(0,n.jsx)(a.p,{children:(0,n.jsx)(a.img,{alt:"Databricks SDLB Job",src:t(3893).A+"",width:"821",height:"428"})}),"\n",(0,n.jsx)(a.p,{children:"Create a Databricks Notebook and add the following code into the first cell to initialize SDLB Lab:"}),"\n",(0,n.jsx)(a.pre,{children:(0,n.jsx)(a.code,{className:"language-scala",children:'import io.smartdatalake.generated._\nimport io.smartdatalake.lab.SmartDataLakeBuilderLab\nval sdlb = SmartDataLakeBuilderLab[DataObjectCatalog, ActionCatalog](\n  spark,\n  Seq("/Volumes/<catalog>/test/test-vol/application.conf"),\n  DataObjectCatalog(_, _), ActionCatalog(_, _)\n)\n'})}),"\n",(0,n.jsx)(a.p,{children:"Now the DataObject and Action catalog can be used as follows:"}),"\n",(0,n.jsx)(a.p,{children:(0,n.jsx)(a.img,{alt:"Code Completion on the DataObjects Catalog in Databricks Notebook",src:t(4610).A+"",width:"648",height:"165"})}),"\n",(0,n.jsx)(a.h2,{id:"using-dataobjects",children:"Using DataObjects"}),"\n",(0,n.jsx)(a.p,{children:"The functions of a DataObject have been adapted for interactive use in notebooks via a corresponding wrapper. The following functions are available for Spark-DataObjects (other types have not yet been implemented):"}),"\n",(0,n.jsxs)(a.ul,{children:["\n",(0,n.jsx)(a.li,{children:"get(): DataFrame, optionally filtered to topLevelPartitionValues."}),"\n",(0,n.jsx)(a.li,{children:"getWithPartitions(): Get DataFrame filtered via multi-level PartitionValues (Seq[Map[String,String]))"}),"\n",(0,n.jsx)(a.li,{children:"write(): Write DataFrame - disabled by default because DataObjects should be written via pipelines. To enable it, - SmartDataLakeBuilderLab.enableWritingDataObjects=true must be set."}),"\n",(0,n.jsx)(a.li,{children:"writeWithPartitions(): Overwrite DataFrame for defined partitions. Disabled by default, see also comment on write()."}),"\n",(0,n.jsx)(a.li,{children:"dropPartitions() / dropTopLevelPartitions() / dropTopLevelPartition()"}),"\n",(0,n.jsx)(a.li,{children:"infos(): returns a map with the most important information and statistics"}),"\n",(0,n.jsx)(a.li,{children:"partitionColumns()"}),"\n",(0,n.jsx)(a.li,{children:"partitions()"}),"\n",(0,n.jsx)(a.li,{children:"topLevelPartitions()"}),"\n",(0,n.jsx)(a.li,{children:"partitionModDates(): maximal Filedatum pro Partition"}),"\n",(0,n.jsx)(a.li,{children:"schema"}),"\n",(0,n.jsx)(a.li,{children:"printSchema()"}),"\n",(0,n.jsx)(a.li,{children:"refresh(): Short form for session.catalog.refreshTable() to update the tables/file information that Spark may have cached"}),"\n",(0,n.jsx)(a.li,{children:"dataObject: Access to the original SDP DataObject object"}),"\n"]}),"\n",(0,n.jsxs)(a.p,{children:[(0,n.jsx)(a.em,{children:"Pros"}),":"]}),"\n",(0,n.jsxs)(a.ul,{children:["\n",(0,n.jsx)(a.li,{children:"a DataObject can simply be queried and the common operations can be easily performed on it."}),"\n",(0,n.jsx)(a.li,{children:"the location and table name no longer need to be maintained in notebooks. This allows for easy renaming and migrations, and support for branching in the future."}),"\n"]}),"\n",(0,n.jsx)(a.h2,{id:"using-actions-writing-transformations",children:"Using Actions, Writing Transformations"}),"\n",(0,n.jsx)(a.p,{children:"Functions for working with actions have also been adapted for interactive use in notebooks via a corresponding wrapper, these are mainly aimed at adapting, replacing or developing transformations. The following features are available on a Spark action:"}),"\n",(0,n.jsxs)(a.ul,{children:["\n",(0,n.jsx)(a.li,{children:"buildDataFrames(): Returns a builder that can be used to fetch filtered and transformed DataFrames of the action, see below."}),"\n",(0,n.jsx)(a.li,{children:"getDataFrames(): Can do the same as buildDataFrames(), but it has to be configured in a parameter list, which is often less convenient."}),"\n",(0,n.jsx)(a.li,{children:"simulateExecutionMode(): The ExecutionMode defines what data the action will process the next time it runs. The function simulates the call to the Action's ExecutionMode and returns the corresponding result."}),"\n"]}),"\n",(0,n.jsx)(a.p,{children:"The DataFrame builder (buildDataFrames method) helps filter the input dataframes and apply the transformation of an action. To - do this, it offers the following methods:"}),"\n",(0,n.jsxs)(a.ul,{children:["\n",(0,n.jsx)(a.li,{children:"withPartitionValues(): Input-DataFrames mit Partition Values filtern."}),"\n",(0,n.jsx)(a.li,{children:"withFilter(): Filter input dataframes with any filter condition if the corresponding column exists."}),"\n",(0,n.jsx)(a.li,{children:"withFilterEquals(): Filter input dataframes with an equals condition if the appropriate column exists."}),"\n",(0,n.jsx)(a.li,{children:"withoutTransformers(): return input dataframes and do not apply any transformations."}),"\n",(0,n.jsx)(a.li,{children:"withLimitedTransformerNb(): Only apply a certain number of Transformers."}),"\n",(0,n.jsx)(a.li,{children:"withReplacedTransformer(): Replace a specific transformer with another, e.g. to test a bugfix. The transformer to be replaced is indicated by the corresponding index number (0 based)."}),"\n",(0,n.jsx)(a.li,{children:"withAdditionalTransformer(): Add an additional transformer at the end of the transformer chain."}),"\n",(0,n.jsx)(a.li,{children:"get/getOne(): fetch the DataFrames prepared by the builder. This function completes the builder process, up to which point the - previously mentioned methods can be called as many times as you like."}),"\n",(0,n.jsx)(a.li,{children:"getOne can be used to return a single DataFrame - if this is not unique, an appropriate name can be specified for selection."}),"\n"]}),"\n",(0,n.jsxs)(a.p,{children:["Example: build output DataFrames of ",(0,n.jsx)(a.code,{children:"join-departures-airports"})," Action (see getting-started for details) with a filter to reduce rows for debugging"]}),"\n",(0,n.jsx)(a.pre,{children:(0,n.jsx)(a.code,{className:"language-scala",children:'val dfs = sdlb.actions.joinDeparturesAirports\n.buildDataFrames\n.withFilter("dl_ts_captures", $"dl_ts_captures">"2024-04-02")\n.get\n'})}),"\n",(0,n.jsx)(a.admonition,{type:"info",children:(0,n.jsx)(a.p,{children:"The DataFrame builder of an action can only access DataFrames that originate from an Input-DataObject of the action, or are created by a transformer in the chain.\nIf a new Input-DataObject is required, the action configuration in the configuration files must be adapted and the initialization cell must be rerun.\nSee section below for developing new actions for an easier method."})}),"\n",(0,n.jsx)(a.h2,{id:"define-a-new-transformers",children:"Define a new Transformers:"}),"\n",(0,n.jsx)(a.p,{children:"To create a new transformer, simply derive a new class from CustomDfsTransformer (or CustomDfTransformer for 1:1 actions, e.g. CopyAction), and then add it with withAdditonalTransformer in the DataFrame builder"}),"\n",(0,n.jsx)(a.pre,{children:(0,n.jsx)(a.code,{className:"language-scala",children:'import org.apache.spark.sql.DataFrame\nimport io.smartdatalake.workflow.action.spark.customlogic.{CustomDfsTransformer, CustomDfTransformer}\nimport org.apache.spark.sql.functions._\nclass MyTransformer extends CustomDfsTransformer {\n  def transform(dfBtlDistances: DataFrame): DataFrame = {\n    dfBtlDistances.withColumn("test",lit("hello"))\n  }\n}\n'})}),"\n",(0,n.jsx)(a.h2,{id:"develop-a-new-action",children:"Develop a new Action"}),"\n",(0,n.jsx)(a.p,{children:"If a new action/transformation has to be developed, there is no corresponding starting point in the action catalog yet.\nSo, in order to develop a new transformation directly on the catalog, the SmartDataLakeBuilderLab class also provides a corresponding DataFrame builder. It has similar methods to those listed above, but can access all DataObjects from the DataObjects catalog. MyTransformer defined above can be used as follows:"}),"\n",(0,n.jsx)(a.pre,{children:(0,n.jsx)(a.code,{className:"language-scala",children:"sdlb.buildDataFrames.withTransformer(new MyTransformer).getOne.show\n"})}),"\n",(0,n.jsx)(a.h2,{id:"summary",children:"Summary"}),"\n",(0,n.jsx)(a.p,{children:"SDLB provides a nice coding interface for Notebooks to find and access DataObjects, and debug existing or implement new Actions."})]})}function A(e={}){const{wrapper:a}={...(0,i.R)(),...e.components};return a?(0,n.jsx)(a,{...e,children:(0,n.jsx)(d,{...e})}):d(e)}},3893:(e,a,t)=>{t.d(a,{A:()=>n});const n=t.p+"assets/images/databricks_job-af9f7fce13582c904f21f6f428e33062.png"},4610:(e,a,t)=>{t.d(a,{A:()=>n});const n="data:image/png;base64,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"},8453:(e,a,t)=>{t.d(a,{R:()=>r,x:()=>s});var n=t(6540);const i={},o=n.createContext(i);function r(e){const a=n.useContext(o);return n.useMemo((function(){return"function"==typeof e?e(a):{...a,...e}}),[a,e])}function s(e){let a;return a=e.disableParentContext?"function"==typeof e.components?e.components(i):e.components||i:r(e.components),n.createElement(o.Provider,{value:a},e.children)}}}]);