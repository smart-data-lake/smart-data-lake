"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[5270],{3905:(t,e,a)=>{a.d(e,{Zo:()=>m,kt:()=>d});var n=a(7294);function r(t,e,a){return e in t?Object.defineProperty(t,e,{value:a,enumerable:!0,configurable:!0,writable:!0}):t[e]=a,t}function o(t,e){var a=Object.keys(t);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(t);e&&(n=n.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),a.push.apply(a,n)}return a}function s(t){for(var e=1;e<arguments.length;e++){var a=null!=arguments[e]?arguments[e]:{};e%2?o(Object(a),!0).forEach((function(e){r(t,e,a[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(a,e))}))}return t}function i(t,e){if(null==t)return{};var a,n,r=function(t,e){if(null==t)return{};var a,n,r={},o=Object.keys(t);for(n=0;n<o.length;n++)a=o[n],e.indexOf(a)>=0||(r[a]=t[a]);return r}(t,e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(t);for(n=0;n<o.length;n++)a=o[n],e.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(t,a)&&(r[a]=t[a])}return r}var l=n.createContext({}),p=function(t){var e=n.useContext(l),a=e;return t&&(a="function"==typeof t?t(e):s(s({},e),t)),a},m=function(t){var e=p(t.components);return n.createElement(l.Provider,{value:e},t.children)},u={inlineCode:"code",wrapper:function(t){var e=t.children;return n.createElement(n.Fragment,{},e)}},f=n.forwardRef((function(t,e){var a=t.components,r=t.mdxType,o=t.originalType,l=t.parentName,m=i(t,["components","mdxType","originalType","parentName"]),f=p(a),d=r,c=f["".concat(l,".").concat(d)]||f[d]||u[d]||o;return a?n.createElement(c,s(s({ref:e},m),{},{components:a})):n.createElement(c,s({ref:e},m))}));function d(t,e){var a=arguments,r=e&&e.mdxType;if("string"==typeof t||r){var o=a.length,s=new Array(o);s[0]=f;var i={};for(var l in e)hasOwnProperty.call(e,l)&&(i[l]=e[l]);i.originalType=t,i.mdxType="string"==typeof t?t:r,s[1]=i;for(var p=2;p<o;p++)s[p]=a[p];return n.createElement.apply(null,s)}return n.createElement.apply(null,a)}f.displayName="MDXCreateElement"},7397:(t,e,a)=>{a.r(e),a.d(e,{contentTitle:()=>s,default:()=>m,frontMatter:()=>o,metadata:()=>i,toc:()=>l});var n=a(7462),r=(a(7294),a(3905));const o={id:"transformations",title:"Transformations"},s=void 0,i={unversionedId:"reference/transformations",id:"reference/transformations",title:"Transformations",description:"This page is under review and currently not visible in the menu.",source:"@site/docs/reference/transformations.md",sourceDirName:"reference",slug:"/reference/transformations",permalink:"/docs/reference/transformations",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/transformations.md",tags:[],version:"current",frontMatter:{id:"transformations",title:"Transformations"}},l=[{value:"Spark Transformations",id:"spark-transformations",children:[{value:"Predefined Transformations",id:"predefined-transformations",children:[],level:3},{value:"Custom Transformations",id:"custom-transformations",children:[{value:"Java/Scala",id:"javascala",children:[],level:4},{value:"SQL",id:"sql",children:[],level:4},{value:"Python",id:"python",children:[],level:4}],level:3}],level:2}],p={toc:l};function m(t){let{components:e,...a}=t;return(0,r.kt)("wrapper",(0,n.Z)({},p,a,{components:e,mdxType:"MDXLayout"}),(0,r.kt)("div",{className:"admonition admonition-warning alert alert--danger"},(0,r.kt)("div",{parentName:"div",className:"admonition-heading"},(0,r.kt)("h5",{parentName:"div"},(0,r.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,r.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,r.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M5.05.31c.81 2.17.41 3.38-.52 4.31C3.55 5.67 1.98 6.45.9 7.98c-1.45 2.05-1.7 6.53 3.53 7.7-2.2-1.16-2.67-4.52-.3-6.61-.61 2.03.53 3.33 1.94 2.86 1.39-.47 2.3.53 2.27 1.67-.02.78-.31 1.44-1.13 1.81 3.42-.59 4.78-3.42 4.78-5.56 0-2.84-2.53-3.22-1.25-5.61-1.52.13-2.03 1.13-1.89 2.75.09 1.08-1.02 1.8-1.86 1.33-.67-.41-.66-1.19-.06-1.78C8.18 5.31 8.68 2.45 5.05.32L5.03.3l.02.01z"}))),"warning")),(0,r.kt)("div",{parentName:"div",className:"admonition-content"},(0,r.kt)("p",{parentName:"div"},"This page is under review and currently not visible in the menu."))),(0,r.kt)("h2",{id:"spark-transformations"},"Spark Transformations"),(0,r.kt)("p",null,"To implement custom transformation logic, define the ",(0,r.kt)("strong",{parentName:"p"},"transformers")," attribute of an Action. It allows you to chain several transformation in a linear process,\nwhere output SubFeeds from one transformation are use as input for the next.",(0,r.kt)("br",{parentName:"p"}),"\n","Note that the definition of the transformations looks different for:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"1-to-1")," transformations (","*","DfTransformer): One input DataFrame is transformed into one output DataFrame. This is the case for CopyAction, DeduplicateAction and HistorizeAction."),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"many-to-many")," transformations (","*","DfsTransformer): Many input DataFrames can be transformed into many output DataFrames. This is the case for CustomDataFrameAction.")),(0,r.kt)("p",null,"The configuration allows you to use predefined standard transformations or to define custom transformation in various languages."),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"Deprecation Warning"),": there has been a refactoring of transformations in Version 2.0.5. The attribute ",(0,r.kt)("strong",{parentName:"p"},"transformer")," is deprecated and will be removed in future versions. Use ",(0,r.kt)("strong",{parentName:"p"},"transformers")," instead."),(0,r.kt)("h3",{id:"predefined-transformations"},"Predefined Transformations"),(0,r.kt)("p",null,"Predefined transformations implement generic logic to be reused in different actions. The following Transformers exist:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"FilterTransformer (1-to-1): Filter DataFrame with expression"),(0,r.kt)("li",{parentName:"ul"},"BlacklistTransformer (1-to-1): Apply a column blacklist to a DataFrame"),(0,r.kt)("li",{parentName:"ul"},"WhitelistTransformer (1-to-1): Apply a column whitelist to a DataFrame"),(0,r.kt)("li",{parentName:"ul"},"AdditionalColumnsTransformer (1-to-1): Add additional columns to the DataFrame by extracting information from the context"),(0,r.kt)("li",{parentName:"ul"},"StandardizeDatatypesTransformer (1-to-1): Standardize datatypes of a DataFrame"),(0,r.kt)("li",{parentName:"ul"},"DfTransformerWrapperDfsTransformer (many-to-many): use 1-to-1 transformer as many-to-many transformer by specifying the SubFeeds it should be applied to")),(0,r.kt)("h3",{id:"custom-transformations"},"Custom Transformations"),(0,r.kt)("p",null,"Custom transformers provide an easy way to define your own spark logic in various languages."),(0,r.kt)("p",null,"You can pass static ",(0,r.kt)("strong",{parentName:"p"},"options")," and ",(0,r.kt)("strong",{parentName:"p"},"runtimeOptions")," to custom transformations. runtimeOptions are extracted at runtime from the context.\nSpecifying options allows to reuse a transformation in different settings."),(0,r.kt)("h4",{id:"javascala"},"Java/Scala"),(0,r.kt)("p",null,"You can use Spark Dataset API in Java/Scala to define custom transformations.\nIf you have a Java project, create a class that extends CustomDfTransformer or CustomDfsTransformer and implement ",(0,r.kt)("inlineCode",{parentName:"p"},"transform")," method.\nThen use ",(0,r.kt)("strong",{parentName:"p"},"type = ScalaClassSparkDfTransformer")," or ",(0,r.kt)("strong",{parentName:"p"},"type = ScalaClassSparkDfsTransformer")," and configure ",(0,r.kt)("strong",{parentName:"p"},"className")," attribute."),(0,r.kt)("p",null,"If you work without Java project, it's still possible to define your transformation in Java/Scala and compile it at runtime.\nFor a 1-to-1 transformation use ",(0,r.kt)("strong",{parentName:"p"},"type = ScalaCodeSparkDfTransformer")," and configure ",(0,r.kt)("strong",{parentName:"p"},"code")," or ",(0,r.kt)("strong",{parentName:"p"},"file")," as a function that takes ",(0,r.kt)("inlineCode",{parentName:"p"},"session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectName: String")," as parameters and returns a ",(0,r.kt)("inlineCode",{parentName:"p"},"DataFrame"),".\nFor many-to-many transformations use ",(0,r.kt)("strong",{parentName:"p"},"type = ScalaCodeSparkDfsTransformer")," and configure ",(0,r.kt)("strong",{parentName:"p"},"code")," or ",(0,r.kt)("strong",{parentName:"p"},"file")," as a function that takes ",(0,r.kt)("inlineCode",{parentName:"p"},"session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]")," with DataFrames per input DataObject as parameter, and returns a ",(0,r.kt)("inlineCode",{parentName:"p"},"Map[String,DataFrame]")," with the DataFrame per output DataObject."),(0,r.kt)("p",null,"See ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/smart-data-lake/sdl-examples"},"sdl-examples")," for details."),(0,r.kt)("h4",{id:"sql"},"SQL"),(0,r.kt)("p",null,"You can use Spark SQL to define custom transformations.\nInput dataObjects are available as tables to select from. Use tokens %{","<","key",">","} to replace with runtimeOptions in SQL code.\nFor a 1-to-1 transformation use ",(0,r.kt)("strong",{parentName:"p"},"type = SQLDfTransformer")," and configure ",(0,r.kt)("strong",{parentName:"p"},"code")," as your SQL transformation statement.\nFor many-to-many transformations use ",(0,r.kt)("strong",{parentName:"p"},"type = SQLDfsTransformer")," and configure ",(0,r.kt)("strong",{parentName:"p"},"code"),' as a Map of "',"<","outputDataObjectId",">",", ","<","SQL transformation statement",">",'".'),(0,r.kt)("p",null,"Example - using options in sql code for 1-to-1 transformation:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'transformers = [{\n  type = SQLDfTransformer\n  name = "test run"\n  description = "description of test run..."\n  sqlCode = "select id, cnt, \'%{test}\' as test, %{run_id} as last_run_id from dataObject1"\n  options = {\n    test = "test run"\n  }\n  runtimeOptions = {\n    last_run_id = "runId - 1" // runtime options are evaluated as spark SQL expressions against DefaultExpressionData\n  }\n}]\n')),(0,r.kt)("p",null,"Example - defining a many-to-many transformation:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"transformers = [{\n  type = SQLDfsTransformer\n  code = {\n    dataObjectOut1 = \"select id,cnt from dataObjectIn1 where group = 'test1'\",\n    dataObjectOut2 = \"select id,cnt from dataObjectIn1 where group = 'test2'\"\n  }\n}\n")),(0,r.kt)("p",null,"See ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/smart-data-lake/sdl-examples"},"sdl-examples")," for details."),(0,r.kt)("h4",{id:"python"},"Python"),(0,r.kt)("p",null,"It's also possible to use Python to define a custom Spark transformation.\nFor a 1-to-1 transformation use ",(0,r.kt)("strong",{parentName:"p"},"type = PythonCodeDfTransformer")," and configure ",(0,r.kt)("strong",{parentName:"p"},"code")," or ",(0,r.kt)("strong",{parentName:"p"},"file")," as a python function.\nPySpark session is initialize and available under variables ",(0,r.kt)("inlineCode",{parentName:"p"},"sc"),", ",(0,r.kt)("inlineCode",{parentName:"p"},"session"),", ",(0,r.kt)("inlineCode",{parentName:"p"},"sqlContext"),".\nOther variables available are"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"inputDf"),": Input DataFrame"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"options"),": Transformation options as Map","[String,String]"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"dataObjectId"),": Id of input dataObject as String")),(0,r.kt)("p",null,"Output DataFrame must be set with ",(0,r.kt)("inlineCode",{parentName:"p"},"setOutputDf(df)"),"."),(0,r.kt)("p",null,"For now using Python for many-to-many transformations is not possible, although it would be not so hard to implement."),(0,r.kt)("p",null,"Example - apply some python calculation as udf:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},'transformers = [{\n  type = PythonCodeDfTransformer \n  code = """\n    |from pyspark.sql.functions import *\n    |udf_multiply = udf(lambda x, y: x * y, "int")\n    |dfResult = inputDf.select(col("name"), col("cnt"))\\\n    |  .withColumn("test", udf_multiply(col("cnt").cast("int"), lit(2)))\n    |setOutputDf(dfResult)\n  """\n}]\n')),(0,r.kt)("p",null,"Requirements:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Spark 2.4.x:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Python version ",">","= 3.4 an ","<","= 3.7"),(0,r.kt)("li",{parentName:"ul"},"PySpark package matching your spark version"))),(0,r.kt)("li",{parentName:"ul"},"Spark 3.x:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Python version ",">","= 3.4"),(0,r.kt)("li",{parentName:"ul"},"PySpark package matching your spark version")))),(0,r.kt)("p",null,"See Readme of ",(0,r.kt)("a",{parentName:"p",href:"https://github.com/smart-data-lake/sdl-examples"},"sdl-examples")," for a working example and instructions to setup python environment for IntelliJ"),(0,r.kt)("p",null,"How it works: under the hood a PySpark DataFrame is a proxy for a Java Spark DataFrame. PySpark uses Py4j to access Java objects in the JVM."))}m.isMDXComponent=!0}}]);