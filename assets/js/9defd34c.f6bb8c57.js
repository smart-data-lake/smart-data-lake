"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[8216],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>m});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),u=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},c=function(e){var t=u(e.components);return a.createElement(l.Provider,{value:t},e.children)},p="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},f=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),p=u(n),f=r,m=p["".concat(l,".").concat(f)]||p[f]||d[f]||i;return n?a.createElement(m,o(o({ref:t},c),{},{components:n})):a.createElement(m,o({ref:t},c))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=f;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[p]="string"==typeof e?e:r,o[1]=s;for(var u=2;u<i;u++)o[u]=n[u];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}f.displayName="MDXCreateElement"},9831:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>p,frontMatter:()=>i,metadata:()=>s,toc:()=>u});var a=n(7462),r=(n(7294),n(3905));const i={id:"testing",title:"Testing"},o=void 0,s={unversionedId:"reference/testing",id:"reference/testing",title:"Testing",description:"Testing is crucial for software quality and maintenance. This is also true for data pipelines.",source:"@site/docs/reference/testing.md",sourceDirName:"reference",slug:"/reference/testing",permalink:"/docs/reference/testing",draft:!1,editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/testing.md",tags:[],version:"current",frontMatter:{id:"testing",title:"Testing"},sidebar:"docs",previous:{title:"Deploy on Microsoft Azure Databricks",permalink:"/docs/reference/deploy-microsoft-azure"},next:{title:"Troubleshooting",permalink:"/docs/reference/troubleshooting"}},l={},u=[{value:"Config validation",id:"config-validation",level:2},{value:"Dry run",id:"dry-run",level:2},{value:"Custom transformation logic unit tests",id:"custom-transformation-logic-unit-tests",level:2},{value:"Simulation of dataframe data pipeline",id:"simulation-of-dataframe-data-pipeline",level:2}],c={toc:u};function p(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("p",null,"Testing is crucial for software quality and maintenance. This is also true for data pipelines."),(0,r.kt)("p",null,"SDL provides the following possibilities for Testing:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"config validation")," -> Basic CI test: configuration syntax test without accessing any environment"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"dry run")," -> Integration/Smoke Tests: validation of configuration together with a given environment, including validation of data schemas."),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"unit tests"),": test single transformation logic and UDFs by creating standard unit tests."),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("strong",{parentName:"li"},"data pipeline simulation"),": test transformation logic over several DataFrame actions by providing mocked input and validating output produced.")),(0,r.kt)("h2",{id:"config-validation"},"Config validation"),(0,r.kt)("p",null,"Parsing of configuration can be validated by specifying command line parameter ",(0,r.kt)("inlineCode",{parentName:"p"},"--test config")," or programmatically:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},'class ConfigTest extends FunSuite with TestUtil {\n  test("validate configuration") {\n    val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(Seq("src/main/resources"))\n    assert(registry.getActions.nonEmpty)\n  }\n}\n')),(0,r.kt)("p",null,"This should be done in continuous integration."),(0,r.kt)("h2",{id:"dry-run"},"Dry run"),(0,r.kt)("p",null,"A dry-run can be started by specifying command line parameter ",(0,r.kt)("inlineCode",{parentName:"p"},"--test dry-run"),".\nThe dry-run executes only prepare and init phase. It validates configuration, checks connections and validates Spark lineage.\nIt doesn't change anything in the environment."),(0,r.kt)("p",null,"This is suitable for smoke testing after deployment."),(0,r.kt)("h2",{id:"custom-transformation-logic-unit-tests"},"Custom transformation logic unit tests"),(0,r.kt)("p",null,"Logic of custom transformation can easily be unit tested. Example:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},'class MyCustomTransformerTest extends FunSuite {\n\n  // init spark session\n  val sparkSession = Map("spark.default.parallelism" -> "1", "spark.sql.shuffle.partitions" -> "1", "spark.task.maxFailures" -> "1")\n  lazy val session: SparkSession = GlobalConfig(enableHive = false, sparkOptions = Some(sparkSession))\n    .createSparkSession("test", Some("local[*]"))\n  import session.implicits._\n\n  test("test my custom transformer") {\n\n    // define input\n    val dfInput = Seq(("joe",1)).toDF("name", "cnt")\n\n    // transform\n    val transformer = new MyCustomTransformer\n    val dfsInput = Map("input" -> dfInput)\n    val dfsTransformed = transformer.transform(session, Map(), dfsInput)\n    val dfOutput = dfsTransformed("output")\n\n    // check\n    assert(dfOutput.count == 1)\n  }\n}\n')),(0,r.kt)("h2",{id:"simulation-of-dataframe-data-pipeline"},"Simulation of dataframe data pipeline"),(0,r.kt)("p",null,'Instead of testing single transformation logic, it can be interesting to test whole pipelines of DataFrame actions.\nFor this you can start a simulation run programmatically in a test suite by providing all input data frames and get the output data frames of the end nodes of the DAG.\nThe simulation mode only executes the init phase with special modification, so it runs without any environment. Of course there are some exceptions like kafka/confluent schema registry.\nNote that simulation mode only supports DataFrame actions for now, you might need to choose "feedSel" accordingly.'),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},'class MyDataPipelineTest extends FunSuite with TestUtil {\n\n  // load config\n  val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(Seq("src/main/resources"))\n\n  // init spark session\n  val sparkSession = Map("spark.default.parallelism" -> "1", "spark.sql.shuffle.partitions" -> "1", "spark.task.maxFailures" -> "1")\n  lazy val session: SparkSession = GlobalConfig(enableHive = false, sparkOptions = Some(sparkSession))\n    .createSparkSession("test", Some("local[*]"))\n  import session.implicits._\n\n  test("enrich and process: create alarm") {\n  \n    // get input1 from modified data object\n    val input1DO = registry.get[CsvFileDataObject]("input1")\n      .copy(connectionId = None, path = "src/test/resources/sample_input1.csv")\n    val dfInput1 = input1DO.getDataFrame()\n\n    // define input2 manually\n    val dfInput2 = Seq(("joe",1)).toDF("name", "cnt")\n\n    // transform\n    val inputSubFeeds = Seq(\n      SparkSubFeed(Some(dfInput1), DataObjectId("input1"), Seq()),\n      SparkSubFeed(Some(dfInput2), DataObjectId("ipnut2"), Seq())\n    )\n    val config = SmartDataLakeBuilderConfig(feedSel = s"my-feed", applicationName = Some("test"), configuration = Some("test"))\n    val sdlb = new DefaultSmartDataLakeBuilder()\n    val (finalSubFeeds, stats) = sdlb.startSimulation(config, inputSubFeeds)\n    val dfOutput = finalSubFeeds.find(_.dataObjectId.id == s"output").get.dataFrame.get.cache\n\n    // check\n    val output = dfOutput.select($"name", $"test").as[(String,Boolean)].collect.toSeq\n    assert(output == Seq(("joe", true)))\n  }\n}\n')))}p.isMDXComponent=!0}}]);