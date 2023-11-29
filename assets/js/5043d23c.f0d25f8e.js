"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[5270],{4202:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>l,contentTitle:()=>o,default:()=>u,frontMatter:()=>s,metadata:()=>i,toc:()=>d});var r=t(5893),a=t(1151);t(4866),t(5162);const s={id:"transformations",title:"Transformations"},o=void 0,i={id:"reference/transformations",title:"Transformations",description:"Overview",source:"@site/docs/reference/transformations.md",sourceDirName:"reference",slug:"/reference/transformations",permalink:"/docs/reference/transformations",draft:!1,unlisted:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/transformations.md",tags:[],version:"current",frontMatter:{id:"transformations",title:"Transformations"},sidebar:"tutorialSidebar",previous:{title:"Execution Modes",permalink:"/docs/reference/executionModes"},next:{title:"Streaming",permalink:"/docs/reference/streaming"}},l={},d=[{value:"Overview",id:"overview",level:2},{value:"Example",id:"example",level:4},{value:"Predefined Transformations",id:"predefined-transformations",level:2},{value:"Custom Transformations",id:"custom-transformations",level:2},{value:"SQL",id:"sql",level:3},{value:"SQL 1-to-1",id:"sql-1-to-1",level:5},{value:"SQL many-to-many",id:"sql-many-to-many",level:5},{value:"Scala",id:"scala",level:3},{value:"As Scala class",id:"as-scala-class",level:5},{value:"In Configuration",id:"in-configuration",level:5},{value:"Python",id:"python",level:3},{value:"Requirements",id:"requirements",level:5},{value:"Options / RuntimeOptions",id:"options--runtimeoptions",level:3},{value:"In SQL",id:"in-sql",level:5},{value:"In Scala",id:"in-scala",level:5},{value:"In Python",id:"in-python",level:5}];function c(e){const n={a:"a",admonition:"admonition",br:"br",code:"code",em:"em",h2:"h2",h3:"h3",h4:"h4",h5:"h5",li:"li",p:"p",pre:"pre",strong:"strong",table:"table",tbody:"tbody",td:"td",th:"th",thead:"thead",tr:"tr",ul:"ul",...(0,a.a)(),...e.components};return(0,r.jsxs)(r.Fragment,{children:[(0,r.jsx)(n.h2,{id:"overview",children:"Overview"}),"\n",(0,r.jsx)(n.p,{children:"On all Spark related actions, you can add one or multiple transformers that are used to transform data before handing\nit over to the next action or writing the Data Object.\nIf you define multiple transformers for one action, they are processed sequentially (output of one transformer becomes input of the next).\nThis page describes the many out-of-the-box transformations and explains how to write your own."}),"\n",(0,r.jsx)(n.p,{children:"In all cases, two types of transformers need to be distinguished:"}),"\n",(0,r.jsxs)(n.ul,{children:["\n",(0,r.jsxs)(n.li,{children:[(0,r.jsx)(n.strong,{children:"1-to-1"}),": One input Data Object is transformed into one output Data Object."]}),"\n",(0,r.jsxs)(n.li,{children:[(0,r.jsx)(n.strong,{children:"many-to-many"}),": One or more input Data Objects are transformed into (potentially) many output Data Object."]}),"\n"]}),"\n",(0,r.jsx)(n.admonition,{type:"info",children:(0,r.jsx)(n.p,{children:"Although it's possible to create many output Data Object, it's encouraged to only create actions with one output Data Object.\nThe resulting lineage of the pipelines will be clearer and impact analysis easier."})}),"\n",(0,r.jsx)(n.h4,{id:"example",children:"Example"}),"\n",(0,r.jsx)(n.p,{children:"As a simple example, we add a WhitelistTransformer to a CopyAction.\nThis will result in only the given columns being selected from the input Data Object and any other columns being discarded."}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{children:"SampleWhitelistAction {\n    type = CopyAction\n    inputId = someInput\n    outputId = someOutput\n    transformers = [{\n        type = WhitelistTransformer\n        columnWhitelist = [includedColumn1,anotherColumn2]\n    }]\n}\n"})}),"\n",(0,r.jsxs)(n.p,{children:["Note the square brackets: ",(0,r.jsx)(n.em,{children:"transformers"})," is an array as you can define multiple transformers."]}),"\n",(0,r.jsxs)(n.p,{children:["The parameter ",(0,r.jsx)(n.code,{children:"columnWhitelist"})," is dependent on the type of the transformer.\nFor details about given parameters / options, please see the ",(0,r.jsx)(n.a,{href:"../../json-schema-viewer",children:"Configuration Schema Viewer"}),"."]}),"\n",(0,r.jsx)(n.h2,{id:"predefined-transformations",children:"Predefined Transformations"}),"\n",(0,r.jsx)(n.p,{children:"SDLB comes with commonly used transformers out-of-the-box."}),"\n",(0,r.jsx)(n.p,{children:"The following predefined 1-to-1 transformations are supported:"}),"\n",(0,r.jsxs)(n.table,{children:[(0,r.jsx)(n.thead,{children:(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.th,{children:"Transformer"}),(0,r.jsx)(n.th,{children:"Description"})]})}),(0,r.jsxs)(n.tbody,{children:[(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"AdditionalColumnsTransformer"}),(0,r.jsx)(n.td,{children:"Add additional columns to the DataFrame by extracting information from the context  or derive new columns from existing columns"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"BlacklistTransformer"}),(0,r.jsx)(n.td,{children:"Apply a column blacklist to discard columns"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"DataValidationTransformer"}),(0,r.jsx)(n.td,{children:"validates DataFrame with a user defined set of rules and creates column with potential error messages"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"DecryptColumnsTransformer"}),(0,r.jsx)(n.td,{children:"Decrypts specified columns using AES/GCM algorithm"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"EncryptColumnsTransformer"}),(0,r.jsx)(n.td,{children:"Encrypts specified columns using AES/GCM algorithm"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"FilterTransformer"}),(0,r.jsx)(n.td,{children:"Filter DataFrame with expression"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"SparkRepartitionTransformer"}),(0,r.jsx)(n.td,{children:"Repartitons a DataFrame"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"StandardizeColNamesTransformer"}),(0,r.jsx)(n.td,{children:"Used to standardize column names according to configurable rules"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"StandardizeSparkDatatypesTransformer"}),(0,r.jsx)(n.td,{children:"Standardize data types of a DataFrame (decimal to corresponding int / float)"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"WhitelistTransformer"}),(0,r.jsx)(n.td,{children:"Apply a column whitelist to a DataFrame"})]})]})]}),"\n",(0,r.jsx)(n.h2,{id:"custom-transformations",children:"Custom Transformations"}),"\n",(0,r.jsxs)(n.p,{children:["When these predefined transformations are not enough, you can easily write your own ",(0,r.jsx)(n.em,{children:"Custom Transformations"}),".\nSDLB currently supports SQL, Scala and Python transformations, depending on the complexity and needed libraries."]}),"\n",(0,r.jsx)(n.p,{children:"The following custom transformations are available."}),"\n",(0,r.jsxs)(n.table,{children:[(0,r.jsx)(n.thead,{children:(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.th,{children:"Transformer"}),(0,r.jsx)(n.th,{children:"Description"})]})}),(0,r.jsxs)(n.tbody,{children:[(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"SQLDfTransformer"}),(0,r.jsx)(n.td,{children:"SQL Transformation 1-to-1"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"SQLDfsTransformer"}),(0,r.jsx)(n.td,{children:"SQL Transformation many-to-many"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"ScalaClassGenericDfTransformer"}),(0,r.jsx)(n.td,{children:"Spark DataFrame transformation in Scala 1-to-1"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"ScalaClassGenericDfsTransformer"}),(0,r.jsx)(n.td,{children:"Spark DataFrame transformation in Scala many-to-many"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"ScalaClassSnowparkDfTransformer"}),(0,r.jsx)(n.td,{children:"Snowpark (Snowflake) transformation in Scala 1-to-1"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"ScalaClassSnowparkDfsTransformer"}),(0,r.jsx)(n.td,{children:"Snowpark (Snowflake) transformation in Scala many-to-many"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"ScalaClassSparkDfTransformer"}),(0,r.jsx)(n.td,{children:"Spark DataFrame transformation in Scala 1-to-1"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"ScalaClassSparkDfsTransformer"}),(0,r.jsx)(n.td,{children:"Spark DataFrame transformation in Scala many-to-many"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"ScalaClassSparkDsTransformer"}),(0,r.jsx)(n.td,{children:"Spark DataSet transformation in Scala 1-to-1"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"ScalaClassSparkDsNTo1Transformer"}),(0,r.jsx)(n.td,{children:"Spark DataFrame transformation in Scala many-to-one"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"ScalaNotebookSparkDfTransformer"}),(0,r.jsx)(n.td,{children:"Loads custom code from a Notebook"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"PythonCodeSparkDfTransformer"}),(0,r.jsx)(n.td,{children:"Spark DataFrame transformation in Python 1-to-1 (using PySpark)"})]}),(0,r.jsxs)(n.tr,{children:[(0,r.jsx)(n.td,{children:"PythonCodeSparkDfsTransformer"}),(0,r.jsx)(n.td,{children:"Spark DataFrame transformation in Python many-to-many (using PySpark)"})]})]})]}),"\n",(0,r.jsxs)(n.p,{children:["There are usually two variants, one for 1-to-1 transformations called ",(0,r.jsx)(n.em,{children:"DfTransformer"})," and one\nfor many-to-many transformations called ",(0,r.jsx)(n.em,{children:"DfsTransformer"}),"."]}),"\n",(0,r.jsx)(n.admonition,{type:"info",children:(0,r.jsxs)(n.p,{children:["The type of the transformer needs to match your action.\nThis is also apparent in the ",(0,r.jsx)(n.a,{href:"../../json-schema-viewer",children:"Configuration Schema Viewer"}),":",(0,r.jsx)(n.br,{}),"\n","1-to-1 transformers are listed under 1-to-1 actions, i.e. CopyAction.",(0,r.jsx)(n.br,{}),"\n","Many-to-many transformers are only listed under many-to-many actions, i.e. CustomDataFrameAction."]})}),"\n",(0,r.jsx)(n.h3,{id:"sql",children:"SQL"}),"\n",(0,r.jsx)(n.p,{children:"Spark SQL is probably the easiest way to write a custom transformation, directly in your HOCON configuration.\nAll input Data Objects are available in the select statement with following naming:"}),"\n",(0,r.jsxs)(n.ul,{children:["\n",(0,r.jsx)(n.li,{children:"special characters are replaced by an underscore"}),"\n",(0,r.jsxs)(n.li,{children:["a postfix ",(0,r.jsx)(n.code,{children:"_sdltemp"})," is added.\nSo an input Data Object called ",(0,r.jsx)(n.code,{children:"table-with-hyphen"})," becomes ",(0,r.jsx)(n.code,{children:"table_with_hyphen_sdltemp"})," inside the SQL query.\nTo simplify this you can also use the special token ",(0,r.jsx)(n.code,{children:"%{inputViewName}"})," for 1-to-1 transformations, or ",(0,r.jsx)(n.code,{children:"${inputViewName_<inputDataObjectName>}"})," for n-to-m transformations, that will be replaced with the correct name at runtime."]}),"\n"]}),"\n",(0,r.jsx)(n.h5,{id:"sql-1-to-1",children:"SQL 1-to-1"}),"\n",(0,r.jsx)(n.p,{children:"1-to-1 transformations use type SQLDfTransformer."}),"\n",(0,r.jsx)(n.p,{children:"Let's assume we have an input Data Object called dataObject1.\nWe can then write a SQL transformation directly in our HOCON configuration:"}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{children:'transformers = [{\n  type = SQLDfTransformer\n  sqlCode = "select id, count(*) from %{inputViewName} group by id"\n}]\n'})}),"\n",(0,r.jsx)(n.p,{children:"The SQL code gets executed in Spark SQL so you can use all available functions."}),"\n",(0,r.jsx)(n.h5,{id:"sql-many-to-many",children:"SQL many-to-many"}),"\n",(0,r.jsx)(n.p,{children:"Many-to-many transformations use SQLDfsTransformer (note the additional s in Dfs).\nNow that we have multiple output Data Objects, we need to declare which SQL statements belongs to\nwhich Data Object.\nTherefore, we now have a map of objectIds and corresponding SQL statements:"}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{children:"transformers = [{\n  type = SQLDfsTransformer\n  code = {\n    dataObjectOut1 = \"select id, cnt from %{inputViewName_dataObjectIn1} where group = 'test1'\",\n    dataObjectOut2 = \"select id, cnt from %{inputViewName_dataObjectIn1} where group = 'test2'\"\n  }\n}\n"})}),"\n",(0,r.jsx)(n.h3,{id:"scala",children:"Scala"}),"\n",(0,r.jsx)(n.p,{children:"Once transformations get more complex, it's more convenient to implement them in Scala code.\nIn custom Scala code, the whole Spark Dataset API is available.\nIt's of course also possible to include additional libraries for your code,\nso anything you can do in Spark Scala, you can do with SDLB."}),"\n",(0,r.jsx)(n.h5,{id:"as-scala-class",children:"As Scala class"}),"\n",(0,r.jsx)(n.p,{children:"If you have a Java/Scala project, it usually makes sense to create separate classes for your custom Scala code.\nAny classes in your classpath are picked up and can be referenced."}),"\n",(0,r.jsx)(n.p,{children:"To transform data in a 1-to-1 action, i.e. CopyAction:"}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{children:"  my_action {\n    type = CopyAction\n    inputId = stg_input\n    outputId = int_output\n    transformers = [{\n      type = ScalaClassSparkDfTransformer\n      className = io.package.MyFirstTransformer\n    }]\n  }\n"})}),"\n",(0,r.jsxs)(n.p,{children:["Now SDLB expects to find a class ",(0,r.jsx)(n.em,{children:"MyFirstTransformer"})," in the Scala package ",(0,r.jsx)(n.em,{children:"io.package"}),".\nThe class needs to extend CustomDfTransformer and with that, overwrite the transform method from it:"]}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{children:'class MyFirstTransformer extends CustomDfTransformer {\n    override def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String) : DataFrame {\n        // manipulate df\n        val dfExtended = df.withColumn("newColumn", when($"df.desc".isNotNull, $"df.desc").otherwise(lit("na")))\n        dfExtended\n    }\n}\n'})}),"\n",(0,r.jsxs)(n.p,{children:["The ",(0,r.jsx)(n.em,{children:"transform"})," method you need to overwrite receives a single DataFrame called ",(0,r.jsx)(n.em,{children:"df"}),".\nYou can manipulate this DataFrame any way you want.\nIn the end, you simply need to return a DataFrame back to SDLB."]}),"\n",(0,r.jsxs)(n.p,{children:["Because a CopyAction is 1-to-1 only, the transformer also needs to extend the 1-to-1 ",(0,r.jsx)(n.code,{children:"CustomDfTransformer"}),"."]}),"\n",(0,r.jsxs)(n.p,{children:["If you have a many-to-many action and want to write a custom Scala transformer, you need to switch to a ",(0,r.jsx)(n.code,{children:"CustomDataFrameAction"}),"."]}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{children:"my_many_to_many_action {\n  type = CustomDataFrameAction\n  inputIds = [df1,df2]\n  outputIds = [dfOut]\n  transformers = [{\n    type = ScalaClassSparkDfsTransformer\n    class-name = io.package.MySecondTransformer\n  }]\n}\n"})}),"\n",(0,r.jsxs)(n.p,{children:["In this case, your Scala class also needs to extend the many-to-many ",(0,r.jsx)(n.code,{children:"CustomDfsTransformer"})," and overwrite the respective transform method:"]}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{children:'class MySecondTransformer extends CustomDfsTransformer {\n    def transform(session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]) : Map[String,DataFrame] {\n        // now you have multiple input DataFrames and can potentially return multiple DataFrames\n        val df1 = dfs("df1")\n        val df2 = dfs("df2")\n        val combined = df1.join(df2, $"df1.id"==$"df2.fk", "left")\n        Map("dfOut"->combined)\n    }\n}\n'})}),"\n",(0,r.jsxs)(n.p,{children:["The ",(0,r.jsx)(n.em,{children:"transform"})," method now gets a map with all your input DataFrames called ",(0,r.jsx)(n.em,{children:"dfs"}),".\nThe key in ",(0,r.jsx)(n.em,{children:"dfs"})," contains the dataObjectId, in this example we have two inputs called ",(0,r.jsx)(n.em,{children:"df1"})," and ",(0,r.jsx)(n.em,{children:"df2"}),",\nwe use it to extract the two DataFrames.\nAgain, you can manipulate all DataFrames as needed and this time, return a map with all output Data Objects.\nAs noted, it's best practice to only return one Data Object (many-to-one action) as in our example."]}),"\n",(0,r.jsxs)(n.admonition,{type:"info",children:[(0,r.jsx)(n.p,{children:"One thing that might be confusing at this point:"}),(0,r.jsxs)(n.p,{children:["CustomDataFrameAction is the type of the ",(0,r.jsx)(n.strong,{children:"action"})," itself.\nTake a look at the  ",(0,r.jsx)(n.a,{href:"../../json-schema-viewer",children:"Configuration Schema Viewer"}),":\nYou will see CustomDataFrameAction as a direct action type, similar to CopyAction or HistorizeAction."]}),(0,r.jsxs)(n.p,{children:["ScalaClassSparkDfsTransformer is the type of your ",(0,r.jsx)(n.strong,{children:"transformer"}),".\nIt needs to correspond to the action type. A 1-to-1 action, expects a 1-to-1 transformer."]})]}),"\n",(0,r.jsx)(n.h5,{id:"in-configuration",children:"In Configuration"}),"\n",(0,r.jsx)(n.p,{children:"If you don't work with a full Java/Scala project, it's still possible to define your transformations\nin your HOCON configuration and compile it at runtime.\nIn this case, use the transformer type ScalaCodeSparkDfTransformer."}),"\n",(0,r.jsx)(n.p,{children:"To include the Scala code inline:"}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{children:'myActionName {\n  type = CopyAction\n  inputId = stg-tbl1\n  outputId = int-tbl1\n  transformers = [{\n    type = ScalaCodeSparkDfTransformer\n    code = """\n      import org.apache.spark.sql.{DataFrame, SparkSession}\n      import org.apache.spark.sql.functions.explode\n      (session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) => {\n        import session.implicits._\n        df.select("col1","col2")\n      }\n    """\n  }]\n'})}),"\n",(0,r.jsx)(n.p,{children:"Check the method signature in CustomDfTransformer and CustomDfsTransformer according to your requirements."}),"\n",(0,r.jsx)(n.h3,{id:"python",children:"Python"}),"\n",(0,r.jsxs)(n.p,{children:["The transformer needs to use type ",(0,r.jsx)(n.code,{children:"PythonCodeSparkDfTransformer"})," (1-to-1) or ",(0,r.jsx)(n.code,{children:"PythonCodeSparkDfsTransformer"})," (many-to-many).\nYou can either provide it as separate file or inline as Python code again."]}),"\n",(0,r.jsx)(n.p,{children:"Inline 1-to-1 example:"}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{children:'transformers = [{\n  type = PythonCodeDfTransformer \n  code = """\n    |from pyspark.sql.functions import *\n    |udf_multiply = udf(lambda x, y: x * y, "int")\n    |dfResult = inputDf.select(col("name"), col("cnt"))\\\n    |  .withColumn("test", udf_multiply(col("cnt").cast("int"), lit(2)))\n    |setOutputDf(dfResult)\n  """\n}]\n'})}),"\n",(0,r.jsxs)(n.p,{children:["PySpark is initialized automatically and the PySpark session is available under the variables\n",(0,r.jsx)(n.code,{children:"sc"}),", ",(0,r.jsx)(n.code,{children:"session"})," or ",(0,r.jsx)(n.code,{children:"sqlContext"}),".\nSome additional variables are also available:"]}),"\n",(0,r.jsxs)(n.ul,{children:["\n",(0,r.jsxs)(n.li,{children:[(0,r.jsx)(n.code,{children:"inputDf"}),": Input DataFrame"]}),"\n",(0,r.jsxs)(n.li,{children:[(0,r.jsx)(n.code,{children:"dataObjectId"}),": Id of input DataObject as string"]}),"\n"]}),"\n",(0,r.jsxs)(n.p,{children:["The output DataFrame needs to be set with ",(0,r.jsx)(n.code,{children:"setOutputDf(df)"}),"."]}),"\n",(0,r.jsx)(n.p,{children:"And many-to-many inline example:"}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{children:'transformers = [{\n  type = PythonCodeDfsTransformer \n  code = """\n    |dfResult = inputDfs[df1].join(inputDfs[df2], inputDfs[df1].id == inputDfs[df2].fk, "left") \\\n    |  .select(inputDfs[df1].id, inputDfs[df1].desc, inputDfs[df2].desc)\n    |setOutputDf({"outputId": dfResult})\n  """\n}]\n'})}),"\n",(0,r.jsxs)(n.p,{children:["In this case, ",(0,r.jsx)(n.code,{children:"inputDfs"})," is a dictionary (dict) and ",(0,r.jsx)(n.code,{children:"setOutputDf"})," expects a dictionary."]}),"\n",(0,r.jsx)(n.h5,{id:"requirements",children:"Requirements"}),"\n",(0,r.jsxs)(n.p,{children:["Running Python transformations needs some additional setup.\nIn general, Python >= 3.4 is required and PySpark package needs to be installed with a version matching your SDL spark version. Further environment variable PYTHONPATH needs to be set to your python environment ",(0,r.jsx)(n.code,{children:".../Lib/site-packages"})," directory, and pyspark command needs to be accessible from the PATH environment variable."]}),"\n",(0,r.jsx)(n.h3,{id:"options--runtimeoptions",children:"Options / RuntimeOptions"}),"\n",(0,r.jsx)(n.p,{children:"So far these transformations have been quite static:\nCode written can probably only be used for one specific action."}),"\n",(0,r.jsx)(n.p,{children:"For custom transformers, you can therefore provide additional options:"}),"\n",(0,r.jsxs)(n.ul,{children:["\n",(0,r.jsxs)(n.li,{children:[(0,r.jsx)(n.code,{children:"options"}),": static options provided in your HOCON configuration"]}),"\n",(0,r.jsxs)(n.li,{children:[(0,r.jsx)(n.code,{children:"runtimeOptions"}),": extracted at runtime from the context."]}),"\n"]}),"\n",(0,r.jsx)(n.h5,{id:"in-sql",children:"In SQL"}),"\n",(0,r.jsx)(n.p,{children:"If you want to use options in SQL, the syntax is %{key}:"}),"\n",(0,r.jsx)(n.pre,{children:(0,r.jsx)(n.code,{children:'transformers = [{\n  type = SQLDfTransformer\n  sqlCode = "select id, cnt, \'%{test}\' as test, %{last_run_id} as last_run_id from dataObject1"\n  options = {\n    test = "test run"\n  }\n  runtimeOptions = {\n    last_run_id = "runId - 1" // runtime options are evaluated as spark SQL expressions against DefaultExpressionData\n  }\n}]\n'})}),"\n",(0,r.jsx)(n.h5,{id:"in-scala",children:"In Scala"}),"\n",(0,r.jsxs)(n.p,{children:["If you check the signature of the ",(0,r.jsx)(n.code,{children:"transform"})," method again, you can see that you get more than just the DataFrames\nyou can manipulate.\nYou also get a Map[String,String] called ",(0,r.jsx)(n.code,{children:"options"}),".\nThis Map contains the combined ",(0,r.jsx)(n.code,{children:"options"})," and ",(0,r.jsx)(n.code,{children:"runtimeOptions"}),".\nSo in your custom class, you can read all options and runtimeOptions and use them accordingly to parametrize your code."]}),"\n",(0,r.jsx)(n.h5,{id:"in-python",children:"In Python"}),"\n",(0,r.jsxs)(n.p,{children:["Similarly in Python, in addition to the variables ",(0,r.jsx)(n.code,{children:"inputDf"})," and ",(0,r.jsx)(n.code,{children:"dataObjectId"})," (resp. ",(0,r.jsx)(n.code,{children:"inputsDfs"}),"), you get a variable called ",(0,r.jsx)(n.code,{children:"options"}),"\ncontaining all ",(0,r.jsx)(n.code,{children:"options"})," and ",(0,r.jsx)(n.code,{children:"runtimeOptions"}),"."]})]})}function u(e={}){const{wrapper:n}={...(0,a.a)(),...e.components};return n?(0,r.jsx)(n,{...e,children:(0,r.jsx)(c,{...e})}):c(e)}},5162:(e,n,t)=>{t.d(n,{Z:()=>o});t(7294);var r=t(6010);const a={tabItem:"tabItem_Ymn6"};var s=t(5893);function o(e){let{children:n,hidden:t,className:o}=e;return(0,s.jsx)("div",{role:"tabpanel",className:(0,r.Z)(a.tabItem,o),hidden:t,children:n})}},4866:(e,n,t)=>{t.d(n,{Z:()=>g});var r=t(7294),a=t(6010),s=t(2466),o=t(6550),i=t(469),l=t(1980),d=t(7392),c=t(12);function u(e){return r.Children.toArray(e).filter((e=>"\n"!==e)).map((e=>{if(!e||(0,r.isValidElement)(e)&&function(e){const{props:n}=e;return!!n&&"object"==typeof n&&"value"in n}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)}))?.filter(Boolean)??[]}function h(e){const{values:n,children:t}=e;return(0,r.useMemo)((()=>{const e=n??function(e){return u(e).map((e=>{let{props:{value:n,label:t,attributes:r,default:a}}=e;return{value:n,label:t,attributes:r,default:a}}))}(t);return function(e){const n=(0,d.l)(e,((e,n)=>e.value===n.value));if(n.length>0)throw new Error(`Docusaurus error: Duplicate values "${n.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[n,t])}function m(e){let{value:n,tabValues:t}=e;return t.some((e=>e.value===n))}function p(e){let{queryString:n=!1,groupId:t}=e;const a=(0,o.k6)(),s=function(e){let{queryString:n=!1,groupId:t}=e;if("string"==typeof n)return n;if(!1===n)return null;if(!0===n&&!t)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return t??null}({queryString:n,groupId:t});return[(0,l._X)(s),(0,r.useCallback)((e=>{if(!s)return;const n=new URLSearchParams(a.location.search);n.set(s,e),a.replace({...a.location,search:n.toString()})}),[s,a])]}function f(e){const{defaultValue:n,queryString:t=!1,groupId:a}=e,s=h(e),[o,l]=(0,r.useState)((()=>function(e){let{defaultValue:n,tabValues:t}=e;if(0===t.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(n){if(!m({value:n,tabValues:t}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${n}" but none of its children has the corresponding value. Available values are: ${t.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return n}const r=t.find((e=>e.default))??t[0];if(!r)throw new Error("Unexpected error: 0 tabValues");return r.value}({defaultValue:n,tabValues:s}))),[d,u]=p({queryString:t,groupId:a}),[f,x]=function(e){let{groupId:n}=e;const t=function(e){return e?`docusaurus.tab.${e}`:null}(n),[a,s]=(0,c.Nk)(t);return[a,(0,r.useCallback)((e=>{t&&s.set(e)}),[t,s])]}({groupId:a}),j=(()=>{const e=d??f;return m({value:e,tabValues:s})?e:null})();(0,i.Z)((()=>{j&&l(j)}),[j]);return{selectedValue:o,selectValue:(0,r.useCallback)((e=>{if(!m({value:e,tabValues:s}))throw new Error(`Can't select invalid tab value=${e}`);l(e),u(e),x(e)}),[u,x,s]),tabValues:s}}var x=t(2389);const j={tabList:"tabList__CuJ",tabItem:"tabItem_LNqP"};var y=t(5893);function b(e){let{className:n,block:t,selectedValue:r,selectValue:o,tabValues:i}=e;const l=[],{blockElementScrollPositionUntilNextRender:d}=(0,s.o5)(),c=e=>{const n=e.currentTarget,t=l.indexOf(n),a=i[t].value;a!==r&&(d(n),o(a))},u=e=>{let n=null;switch(e.key){case"Enter":c(e);break;case"ArrowRight":{const t=l.indexOf(e.currentTarget)+1;n=l[t]??l[0];break}case"ArrowLeft":{const t=l.indexOf(e.currentTarget)-1;n=l[t]??l[l.length-1];break}}n?.focus()};return(0,y.jsx)("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,a.Z)("tabs",{"tabs--block":t},n),children:i.map((e=>{let{value:n,label:t,attributes:s}=e;return(0,y.jsx)("li",{role:"tab",tabIndex:r===n?0:-1,"aria-selected":r===n,ref:e=>l.push(e),onKeyDown:u,onClick:c,...s,className:(0,a.Z)("tabs__item",j.tabItem,s?.className,{"tabs__item--active":r===n}),children:t??n},n)}))})}function S(e){let{lazy:n,children:t,selectedValue:a}=e;const s=(Array.isArray(t)?t:[t]).filter(Boolean);if(n){const e=s.find((e=>e.props.value===a));return e?(0,r.cloneElement)(e,{className:"margin-top--md"}):null}return(0,y.jsx)("div",{className:"margin-top--md",children:s.map(((e,n)=>(0,r.cloneElement)(e,{key:n,hidden:e.props.value!==a})))})}function v(e){const n=f(e);return(0,y.jsxs)("div",{className:(0,a.Z)("tabs-container",j.tabList),children:[(0,y.jsx)(b,{...e,...n}),(0,y.jsx)(S,{...e,...n})]})}function g(e){const n=(0,x.Z)();return(0,y.jsx)(v,{...e,children:u(e.children)},String(n))}},1151:(e,n,t)=>{t.d(n,{Z:()=>i,a:()=>o});var r=t(7294);const a={},s=r.createContext(a);function o(e){const n=r.useContext(s);return r.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function i(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(a):e.components||a:o(e.components),r.createElement(s.Provider,{value:n},e.children)}}}]);