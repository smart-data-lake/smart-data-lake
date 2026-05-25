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

## Module Development

### Adding New Connectors
1. Create `sdl-{name}/` module directory
2. Add to parent `pom.xml` modules section
3. Create module POM inheriting from `sdl-parent`
4. Implement DataObjects/Actions following existing patterns
5. Add comprehensive tests
