# Smart Data Lake Builder - AI Agent Guidelines

## Architecture Overview

Smart Data Lake Builder is a declarative, configuration-driven data pipeline framework built on Apache Spark. The core architecture consists of:

- **DataObjects**: Abstract data sources/sinks (files, databases, APIs) implementing the `DataObject` trait
- **Actions**: Data transformations implementing the `Action` trait, connecting input/output DataObjects
- **Feeds**: Execution units forming directed acyclic graphs (DAGs) from Action dependencies
- **Connections**: Configuration for external systems (databases, APIs, file systems)

## Project Structure

```
sdl-core/           # Main framework (Actions, DataObjects, Workflows)
sdl-{connector}/    # Specialized connectors (kafka, deltalake, snowflake, etc.)
```

## Development Workflow

### Build Commands
```bash
# Fast development build (excludes slow Debezium shaded modules)
mvn -B clean test -pl '!sdl-debezium/debezium-connector-mysql-shaded,!sdl-debezium/debezium-connector-mariadb-shaded'

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

### Action Implementation
```scala
case class MyAction(
  override val id: ActionId,
  inputId: DataObjectId,
  outputId: DataObjectId,
  override val metadata: Option[ActionMetadata] = None
) extends Action {

  override def inputs: Seq[DataObject] = Seq(getInputDataObject[DataObject](inputId))
  override def outputs: Seq[DataObject] = Seq(getOutputDataObject[DataObject](outputId))

  override def transform(inputSubFeeds: Seq[SparkSubFeed], outputSubFeeds: Seq[SparkSubFeed])
                        (implicit context: ActionPipelineContext): Seq[SparkSubFeed] = {
    // transformation logic
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

## Integration Points

### File Systems
- Hadoop FileSystem (HDFS, S3, GCS, Azure)
- Local filesystem
- SFTP

### Databases
- JDBC (any JDBC-compatible database)
- Snowflake
- Delta Lake tables
- Apache Iceberg tables

### Streaming
- Apache Kafka
- Debezium CDC (MySQL, MariaDB, PostgreSQL, Oracle)

### APIs
- REST APIs (OpenAPI, OData)
- Web services

## Common Patterns

### Schema Handling
- `UserDefinedSchema`: For explicit schema definitions
- `SchemaValidation`: For schema validation against minimum requirements
- Lazy schema parsing in `prepare()` method

### Partitioning
- `PartitionValues`: Represents partition keys/values
- Automatic partition discovery and management
- Housekeeping modes for partition lifecycle management

### Error Handling
- `ConfigurationException`: For config parsing errors
- Custom exceptions with descriptive messages
- Logging via `SmartDataLakeLogger` trait

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
- No scalafmt enforcement (`scalafmt.skip=true`)

## Testing Strategy

### Test Categories
- **Unit tests**: `*Test.scala` - fast, isolated tests
- **Integration tests**: `*IT.scala` - external system integration (excluded by default)
- **DataObject tests**: Extend `DataObjectTestSuite`

### Test Utilities
- `TestUtil.session`: Implicit SparkSession for tests
- `MockSparkDataObject`: For mocking DataObjects in tests
- Test data in `src/test/resources/`

## Module Development

### Adding New Connectors
1. Create `sdl-{name}/` module directory
2. Add to parent `pom.xml` modules section
3. Create module POM inheriting from `sdl-parent`
4. Implement DataObjects/Actions following existing patterns
5. Add comprehensive tests

### Key Reference Files
- `sdl-core/src/main/scala/io/smartdatalake/workflow/dataobject/DataObject.scala`
- `sdl-core/src/main/scala/io/smartdatalake/workflow/action/Action.scala`
- `sdl-core/src/test/resources/application.conf`
- `.github/copilot-instructions.md`</content>
