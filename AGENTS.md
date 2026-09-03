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

### Waiting for long-running builds

A full reactor build takes ~10 minutes, so it is usually started in the background, redirecting to a log file.

**Wait for it with a single blocking command that returns the result**, not with repeated progress checks:

```bash
# one call: blocks until done, then prints the verdict and any real compile errors
until grep -qE "BUILD SUCCESS|BUILD FAILURE" build.log 2>/dev/null; do sleep 15; done
echo "=== RESULT ==="
grep -m1 -E "BUILD SUCCESS|BUILD FAILURE" build.log
grep -E "\[ERROR\].*\.scala" build.log | head -20
```

Anti-patterns that waste a lot of time and context — all observed in practice:

- **Polling in a loop of separate tool calls** (`tail -1 build.log`, then again, then again...). Each call costs a
  round trip and tells you nothing actionable; a build that is "still compiling sdl-spark" needs no decision from
  you. Issue *one* blocking wait and read the result.
- **Launching a background waiter and then immediately checking on it.** The waiter returns via its own completion
  notification; querying the log right after starting it just re-polls by hand. Either block in the foreground
  (with a generous timeout) or start the waiter and genuinely do something else until it reports.
- **Detecting the running build with `pgrep -f "<part of the maven command line>"`**: the polling shell's own
  command line contains that pattern, so `pgrep` matches itself and reports "still running" forever.
- **Matching on `ERROR` alone to decide the build finished.** Every build logs
  `'dependencies.dependency.version' for org.scalactic/org.scalatest ... is missing` while resolving the invalid
  third-party POM of `com.databricks:databricks-dbutils-scala`. These lines are harmless and appear long before the
  build ends. Match `BUILD SUCCESS|BUILD FAILURE` for completion, and `\[ERROR\].*\.scala` for real compile errors.

Note that maven's elapsed time and `ps` timings are unreliable under WSL on `/mnt/c`; judge progress from the log
file's content and mtime, not from process elapsed time.

### Which modules to build

`mvn -pl <module>` alone fails for modules whose SDLB dependencies are not installed in `~/.m2`
(`Cannot access sonatype-snapshots ... in offline mode`). Use `-pl <module> -am` so the dependencies are built in
the same reactor, or `mvn install` them first. `sdl-lang` depends on nearly every other module, so
`-pl sdl-lang -am` is effectively a full build.

**Do not combine `-Dsuites=` with `-am`.** The suite filter is applied to *every* module in the reactor, and the
first module that does not contain the named class aborts the run:

```
*** RUN ABORTED ***
Unable to load a Suite class... Missing class: io.smartdatalake.workflow.dataobject.MyTest
```

So to run one suite in a module whose dependencies are not installed yet, install the reactor first and then run
the suite against the installed artifacts:

```bash
mvn -o -B install -DskipTests -Dlicense.skip=true -Dmaven.source.skip=true \
  -pl '!sdl-debezium/debezium-connector-mysql-shaded,!sdl-debezium/debezium-connector-mariadb-shaded,!sdl-kafka/embedded-kafka-schema-registry-shaded'
mvn -o -B test -pl sdl-deltalake -Dsuites=io.smartdatalake.workflow.dataobject.MyTest
```

A wrong FQCN in `-Dsuites` fails the same way (`RUN ABORTED ... Missing class`), so verify it rather than
guessing from the file path - the package does not always mirror the directory, e.g.
`FactoryMethodCompletenessTest` lives in `io.smartdatalake.config`, not `io.smartdatalake.meta`.

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
) extends DataObject with CanCreateSparkDataFrame

object MyDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): MyDataObject = {
    // parse config and create instance
  }
}
```

The companion object is mandatory: `ConfigParser` resolves it by reflection from the class named in the
Hocon `type` attribute, then looks up `fromConfig` by name and signature. There is no member on the class
pointing at it. `FactoryMethodCompletenessTest` (sdl-lang) asserts that every `ParsableFromConfig`
implementation has a valid companion object; mark a type with `ExcludeFromSchemaExport` if it is
intentionally not parsable from config.

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

### Visibility: do not restrict it

SDLB is a **library**. Everything a user might implement, override or call from their own project must be
reachable from their own package. Do **not** add `private[smartdatalake]` (or `private[workflow]`, ...) to
members of public traits and classes - especially not to abstract members, because a class outside
`io.smartdatalake` then cannot implement them at all and fails to compile with
*"class X needs to be abstract, missing implementations for ... private[package smartdatalake] def ..."*.
The same holds for whole traits: a `private[smartdatalake] trait` cannot be mixed in by user code.

Use `private`/`protected` for genuine implementation details of a single class, and keep the extension
surface public: `DataObject` and its capability traits (`CanCreateDataFrame`, `CanWriteDataFrame`,
`CanHandlePartitions`, `CanEvolveSchema`, `ExpectationValidation`, ...), `Action` and its base classes
(`ActionSubFeedsImpl`, `DataFrameActionImpl`, `DataFrameOneToOneActionImpl`, `FileOneToOneActionImpl`,
`ScriptActionImpl`), `ExecutionMode`, the transformer traits, `Connection`, `Expectation`, `HousekeepingMode`,
`AuthMode`.

`com.mycompany.sdlb.ExternalExtensionTest` (sdl-core tests) guards this: it implements a DataObject and an
Action outside the `io.smartdatalake` package and parses them from config. If it stops compiling, a
visibility restriction was reintroduced. See `docs/docs/reference/extending.md` for the user facing
documentation of the extension points.

### License Headers
Every source file MUST include GPLv3 license header (see existing files for template).

### Commit Messages
- Subject: ≤50 chars, imperative mood, capitalized, no period, reference issue at the end using `(#<issueNb>)`
- Body: ≤72 chars per line, separated by blank line

### Code Style
- Follow Scala Style Guide
- IntelliJ IDEA config in `.idea/codeStyles/`
- scalafmt configured (`.scalafmt.conf`)

### Json Schema
docs/static/json-schema-viewer/schemas/*.json are created and committed by a GitHub Action.
Do not hand-edit it. There is also no need to run maven to update it.

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

/### Where behaviour traits live

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
    val tgtDO = createSrcDataObject("tgt1", instanceRegistry)
    val helper = DataFrameSubFeed.getCompanion(getCommonSubFeed(srcDO, tgtDO))
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

Engine differences are not limited to the DataFrame API — **expression syntax differs too**. Expressions in
`executionCondition`, `executionMode` and `runtimeOptions` are evaluated by Spark SQL on Spark, but by the
hand-written parser in `io.smartdatalake.workflow.dataframe.plainScala.ExpressionParser` on the plain-Scala engine,
which supports considerably less syntax (no map/array indexing such as `predecessorActions['a'].state`, for example).
Such an expression fails already in the prepare phase, in `Condition.syntaxCheck`. A test that needs richer
expression syntax therefore belongs in the Spark suite, not in a behaviour trait.

### Build/test workflow gotchas

- After changing anything under `sdl-core/.../testutils/`, run `mvn install -pl sdl-core -DskipTests` before
  test-compiling/testing a dependent module (e.g. sdl-spark) — that module resolves sdl-core's test-jar from the
  local repo, so a stale install causes confusing "not found" compile errors that look like a code bug.
- Filter to one suite with `-Dsuites=<FQCN>`, not `-Dtest=` (the scalatest-maven-plugin used here ignores `-Dtest`).
- The existing tests group their assertions in anonymous blocks (`{ val stateStore = ...; assert(...) }`). Such a
  block must not directly follow an `assert(...)` call, otherwise Scala parses it as a second argument list and
  compilation fails with `not enough arguments for macro method assert: Unspecified value parameter pos`. Put a
  comment line or another statement in between, as the surrounding code does.

## Module Development

### Adding New Connectors
1. Create `sdl-{name}/` module directory
2. Add to parent `pom.xml` modules section
3. Create module POM inheriting from `sdl-parent`
4. Implement DataObjects/Actions following existing patterns
5. Add comprehensive tests
