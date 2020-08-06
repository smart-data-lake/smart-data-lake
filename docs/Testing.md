# Testing

Testing is crucial for software quality and maintenance. This is also true for data pipelines.

SDL provides the following possibilities for Testing:

## Config validation

Parsing of configuration can be validated by specifying command line parameter `--test config` or programmatically:
```scala
class ConfigTest extends FunSuite with TestUtil {
  test("validate configuration") {
    val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(Seq("src/main/resources"))
    assert(registry.getActions.nonEmpty)
  }
}
```

This should be done in continuous integration. 

## Dry run

A dry-run can be started by specifying command line parameter `--test dry-run`.
The dry-run executes only prepare and init phase. It validates configuration, checks connections and validates Spark lineage.
It doesn't change anything in the environment.

This is suitable for smoke testing after deployment.

## Custom transformation logic unit tests

Logic of custom transformation can easily be unit tested. Example:
```scala
class MyCustomTransformerTest extends FunSuite {

  // init spark session
  val sparkSession = Map("spark.default.parallelism" -> "1", "spark.sql.shuffle.partitions" -> "1", "spark.task.maxFailures" -> "1")
  lazy val session: SparkSession = GlobalConfig(enableHive = false, sparkOptions = Some(sparkSession))
    .createSparkSession("test", Some("local[*]"))
  import session.implicits._

  test("test my custom transformer") {

    // define input
    val dfInput = Seq(("joe",1)).toDF("name", "cnt")

    // transform
    val transformer = new MyCustomTransformer
    val dfsInput = Map("input" -> dfInput)
    val dfsTransformed = transformer.transform(session, Map(), dfsInput)
    val dfOutput = dfsTransformed("output")

    // check
    assert(dfOutput.count == 1)
  }
}
```

## Simulation of spark data pipeline

Instead of testing single transformation logic, it would be interesting to test whole pipelines of spark actions.
For this you can start a simulation run programmatically. You have to provide all input data frames and get the output data frames of the end nodes of the DAG.
The simulation mode only executes the init phase with special modification, so it runs without any environment. Of course there are some exceptions like kafka/confluent schema registry.
Note that simulation mode only supports spark actions for now, you might need to choose "feedSel" accordingly.
```scala
class MyDataPipelineTest extends FunSuite with TestUtil {

  // load config
  val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(Seq("src/main/resources"))

  // init spark session
  val sparkSession = Map("spark.default.parallelism" -> "1", "spark.sql.shuffle.partitions" -> "1", "spark.task.maxFailures" -> "1")
  lazy val session: SparkSession = GlobalConfig(enableHive = false, sparkOptions = Some(sparkSession))
    .createSparkSession("test", Some("local[*]"))
  import session.implicits._

  test("enrich and process: create alarm") {
  
    // get input1 from modified data object
    val input1DO = registry.get[CsvFileDataObject]("input1")
      .copy(connectionId = None, path = "src/test/resources/sample_input1.csv")
    val dfInput1 = input1DO.getDataFrame()

    // define input2 manually
    val dfInput2 = Seq(("joe",1)).toDF("name", "cnt")

    // transform
    val inputSubFeeds = Seq(
      SparkSubFeed(Some(dfInput1), DataObjectId("input1"), Seq()),
      SparkSubFeed(Some(dfInput2), DataObjectId("ipnut2"), Seq())
    )
    val config = SmartDataLakeBuilderConfig(feedSel = s"my-feed", applicationName = Some("test"), configuration = Some("test"))
    val sdlb = new DefaultSmartDataLakeBuilder()
    val (finalSubFeeds, stats) = sdlb.startSimulation(config, inputSubFeeds)
    val dfOutput = finalSubFeeds.find(_.dataObjectId.id == s"output").get.dataFrame.get.cache

    // check
    val output = dfOutput.select($"name", $"test").as[(String,Boolean)].collect.toSeq
    assert(output == Seq(("joe", true)))
  }
}
```