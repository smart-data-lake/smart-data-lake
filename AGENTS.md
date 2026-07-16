# Smart Data Lake Builder - AI Agent Guidelines

## Architecture Overview

Smart Data Lake Builder is a declarative, configuration-driven data pipeline framework built on Apache Spark. The core architecture consists of:

- **Connections**: Configuration for external systems (databases, APIs, file systems)
- **DataObjects**: Abstract data sources/sinks (files, databases, APIs) implementing the `DataObject` trait
- **Actions**: Data transformations implementing the `Action` trait, connecting input/output DataObjects
- **Feeds**: Execution units forming directed acyclic graphs (DAGs) from Action dependencies

## Project Structure

```
sdl-core/           # Main framework (Actions, DataObjects, Workflows)
sdl-{connector}/    # Specialized connectors (kafka, deltalake, snowflake, etc.)
```

## Main frameworks / versions

- Scala 2.13
- Apache Spark 4.1
- Hadoop 3.4
- Build System**: Maven (multi-module project)

## Development Workflow

### Build Commands
```bash
# Fast development build (excludes slow Debezium shaded modules)
mvn -B clean test -pl '!sdl-debezium/debezium-connector-mysql-shaded,!sdl-debezium/debezium-connector-mariadb-shaded,!sdl-kafka/embedded-kafka-schema-registry-shaded'

# Build specific module
mvn clean install -pl sdl-core

# Run specific test suite (ScalaTest plugin filter), e.g. for AccessTableDataObjectTest:
mvn -B test -pl sdl-core -Dsuites=io.smartdatalake.workflow.dataobject.AccessTableDataObjectTest
```

## Configuration Patterns

### HOCON Structure (`application.conf`)
```hocon
dataObjects {
  myDataObject = {
    type = ParquetFileDataObject
    path = "s3://bucket/path"
    connectionId = myConnection
  }
}

actions {
  myAction = {
    type = CopyAction
    inputId = myDataObject
    outputId = myOutputDataObject
  }
}

connections {
  myConnection = {
    type = S3Connection
    accessKey = ${AWS_ACCESS_KEY}
    secretKey = ${AWS_SECRET_KEY}
  }
}
```

## Code Patterns

### DataObject Implementation
```scala
case class MyDataObject(
  override val id: DataObjectId,
  path: String,
  override val metadata: Option[DataObjectMetadata] = None
) extends DataObject with CanCreateSparkDataFrame {

  override def factory: FromConfigFactory[DataObject] = MyDataObject
}

object MyDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MyDataObject = {
    // parse config and create instance
  }
}
```

### Testing Patterns
```scala
class MyDataObjectTest extends DataObjectTestSuite {
  test("MyDataObject is parsable") {
    val config = ConfigFactory.parseString(
      """
        |dataObjects {
        |  testObj = {
        |    type = MyDataObject
        |    path = "/tmp/test"
        |  }
        |}
      """.stripMargin)
    
    val dataObject = MyDataObject.fromConfig(config.getConfig("dataObjects.testObj"))
    dataObject.id shouldBe DataObjectId("testObj")
  }
}
```

## Naming Conventions

### Variables
- DataFrames: `dfCamelCase` (e.g., `dfCustomerData`)
- Datasets: `dsCamelCase` (e.g., `dsOrderItems`)
- Spark Columns: `$"columnName"` shorthand

### Files
- Implementation: `*DataObject.scala`, `*Action.scala`
- Tests: `*DataObjectTest.scala`, `*ActionTest.scala`
- Integration tests: `*IT.scala`

## Key Traits & Classes

### Core Traits
- `DataObject`: Base trait for all data sources/sinks
- `Action`: Base trait for all transformations
- `Connection`: Base trait for external system connections

### Common Mixins
- `CanCreateSparkDataFrame`: DataObjects that can create Spark DataFrames
- `SmartDataLakeLogger`: Provides logging via `logger` member
- `ParsableFromConfig[T]`: Configuration parsing capability

## Development Best Practices

### License Headers
Every source file MUST include GPLv3 license header (see existing files for template).

### Commit Messages
- Subject: ≤50 chars, imperative mood, capitalized, no period
- Body: ≤72 chars per line, separated by blank line
- Reference issues: `#123`, `fixes #456`

### Code Style
- Follow Scala Style Guide
- IntelliJ IDEA config in `.idea/codeStyles/`
- scalafmt configured (`.scalafmt.conf`)

## Testing Strategy

### Test Categories
- **Unit tests**: `*Test.scala` - fast, isolated tests
- **Integration tests**: `*IT.scala` - external system integration (excluded by default)
- **DataObject tests**: Extend `DataObjectTestSuite`

### Test Utilities
- `TestUtil.session`: Implicit SparkSession for tests
- `MockSparkDataObject`: For mocking DataObjects in tests
- Test data in `src/test/resources/`

## Generic Tests: the Behaviour Pattern

Many features (Actions, transformers, the builder itself) are implemented against the engine-agnostic
`GenericDataFrame` abstraction (`io.smartdatalake.workflow.dataframe.*`) and therefore work on multiple engines:
Spark (`SparkSubFeed`, sdl-spark), plain-Scala (`ScalaSubFeed`, sdl-core), Snowpark (`SnowparkSubFeed`,
sdl-snowflake), etc. When adding or refactoring tests for such a feature, write the test logic **once**, engine-agnostic,
and instantiate it once per engine — instead of duplicating near-identical Spark-only test classes.

### Where behaviour traits live

Behaviour traits live in `sdl-core/src/test/scala/io/smartdatalake/testutils/*Behaviour.scala`, e.g.
`DeduplicateActionBehaviour`, `SmartDataLakeBuilderBehaviour`, `ColumnsTransformerBehaviour`, `SQLDfsTransformerBehaviour`.

This location is not arbitrary: sdl-core publishes a `test-jar` artifact so other modules (sdl-spark, sdl-snowflake, ...)
can reuse test utilities, but the jar only packages a few include patterns configured in `sdl-core/pom.xml`
(`maven-jar-plugin` execution `core-test-jar`), most importantly `io/smartdatalake/testutils/**`. **Any class referenced
from another module's tests must live under the `io.smartdatalake.testutils` package**, or `test-compile` in that module
fails with `not found: type X` even though the class exists and compiles fine in sdl-core itself.

### Shape of a behaviour trait

```scala
trait FooActionBehaviour extends GenericTestTool {
  this: SmartDataLakeLogger =>

  // engine-specific pieces the instantiating test class must supply
  def defaultEngineConnection: Connection with EngineConnection

  def testFooBasic(
      createSrcDataObject: (String, InstanceRegistry) => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: (String, Option[Seq[String]], InstanceRegistry) => TransactionalTableDataObject with CanMergeDataFrame
  ): Unit = {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    instanceRegistry.register(defaultEngineConnection)
    val srcDO = createSrcDataObject("src1", instanceRegistry)
    val helper = DataFrameSubFeed.getCompanion(srcDO.getSubFeedSupportedTypes.head)
    import helper.implicits._          // generic toDF(...), not session.implicits._
    val df = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    // ... exercise the Action using only the GenericDataFrame API (col, select, filter, collect[T], ...)
  }
}
```

Key rules:
- Depend only on the generic API (`GenericDataFrame`, `DataFrameFunctions`, `DataFrameSubFeed.getCompanion(subFeedType)`)
  — never import a Spark-specific `SparkSession.implicits._` or reference `org.apache.spark.sql.*` directly.
  Get engine functions/implicits via `DataFrameSubFeed.getCompanion(subFeedType)`.
- Parameterize everything engine-specific as constructor/method arguments or abstract `def`s: mock-DataObject
  factories, `defaultEngineConnection`, engine-specific transformers/expectations that have no generic equivalent
  (e.g. `failTransformer`, `testCountExpectation` in `SmartDataLakeBuilderBehaviour`).
- Each engine module implements its own Mock DataObject with matching constructor shape (`MockScalaDataObject` in
  sdl-core, `MockSparkDataObject` in sdl-spark) so the same factory-function signature works for every instantiation.

### Instantiating per engine

One thin test class per engine, same test names, each just calling into the trait:

```scala
// sdl-core: io.smartdatalake.workflow.action.plainScala.FooActionTest
class FooActionTest extends AnyFunSuite with SmartDataLakeLogger with FooActionBehaviour {
  override def defaultEngineConnection: Connection with EngineConnection = ScalaTestUtil.defaultScalaConnection
  test("foo basic") { testFooBasic((id, r) => MockScalaDataObject(id)(r), (id, pk, r) => MockScalaDataObject(id, primaryKey = pk)(r)) }
}

// sdl-spark: io.smartdatalake.workflow.action.spark.FooActionTest
class FooActionTest extends AnyFunSuite with SmartDataLakeLogger with FooActionBehaviour {
  override def defaultEngineConnection: Connection with EngineConnection = SparkTestUtil.defaultSparkConnection
  test("foo basic") { testFooBasic((id, r) => MockSparkDataObject(id)(r), (id, pk, r) => MockSparkDataObject(id, primaryKey = pk)(r)) }
}
```

If a feature genuinely isn't implemented for one engine (e.g. `DataFrameFunctions.sql` isn't implemented for
`ScalaSubFeed`), `ignore(...)` the test in *that engine's* instantiation with a one-line comment explaining why —
keep the shared trait itself universal, don't special-case engines inside it.

### Build/test workflow gotchas

- After changing anything under `sdl-core/.../testutils/`, run `mvn install -pl sdl-core -DskipTests` before
  test-compiling/testing a dependent module (e.g. sdl-spark) — that module resolves sdl-core's test-jar from the
  local repo, so a stale install causes confusing "not found" compile errors that look like a code bug.
- Filter to one suite with `-Dsuites=<FQCN>`, not `-Dtest=` (the scalatest-maven-plugin used here ignores `-Dtest`).

## Module Development

### Adding New Connectors
1. Create `sdl-{name}/` module directory
2. Add to parent `pom.xml` modules section
3. Create module POM inheriting from `sdl-parent`
4. Implement DataObjects/Actions following existing patterns
5. Add comprehensive tests
