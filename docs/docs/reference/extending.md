---
id: extending
title: Extending SDLB
---

SDLB is a library, not a closed product: Actions, DataObjects, execution modes, transformers and more are traits you can implement yourself.
Whenever a requirement cannot be expressed in HOCON configuration, the answer is usually a small Scala or Java class on the classpath, referenced from the configuration like a built-in type.

Before writing code, check whether configuration already covers your case. The cheapest extensions, in this order:

1. a SQL or Scala [transformation](transformations) on an existing Action,
2. a custom class implementing one of the *custom logic* interfaces (see [Classes referenced by className](#classes-referenced-by-classname)),
3. an own configuration object, e.g. an own ExecutionMode or DataObject,
4. an own Action - the biggest step, and rarely necessary.

## Setup

Create your own Maven or sbt project, add SDLB as dependency and put the resulting jar on the classpath of your SDLB job.
Using `sdl-parent` as parent POM gives you matching dependency versions and the *fat-jar* profiles, see [Build SDL](build.md) and the [sdl-examples](https://github.com/smart-data-lake/sdl-examples) template.

Depend on `sdl-core` for engine independent code, and additionally on the engine module you need, e.g. `sdl-spark` for classic Spark or `sdl-sparkconnect` for Spark Connect, see [Execution Engines](executionEngines).
Prefer `sdl-core` and the generic APIs whenever possible: such an extension runs on every engine.

Your classes live in your own package, e.g. `com.company.dataPipeline` - nothing needs to be placed under `io.smartdatalake`.

## The two extension patterns

SDLB knows two ways to get your code into a data pipeline.

### Configuration objects (`type`)

Most SDLB concepts are *configuration objects*: they are parsed from HOCON, and the `type` attribute selects the implementation.
Your own implementation is referenced exactly like a built-in one, and its constructor parameters become configuration attributes.

| Extension point | Trait to implement | Configured in |
| --------------- | ------------------ | ------------- |
| Action | `Action`, normally through one of its base classes, see [Own Action](#own-action) | `actions.<id>.type` |
| DataObject | `DataObject` plus capability traits, see [Own DataObject](#own-dataobject) | `dataObjects.<id>.type` |
| Connection | `Connection`, plus `EngineConnection` for a DataFrame engine | `connections.<id>.type` |
| ExecutionMode | `ExecutionMode`, see [Execution Modes](executionModes#implement-your-own-execution-mode) | `actions.<id>.executionMode.type` |
| Transformer 1:1 | `GenericDfTransformer` | `actions.<id>.transformers[].type` |
| Transformer n:m | `GenericDfsTransformer` | `actions.<id>.transformers[].type` |
| Expectation | `Expectation` (on a DataObject) or `ActionExpectation` (on an Action), see [Data Quality](dataQuality) | `dataObjects.<id>.expectations[].type` |
| HousekeepingMode | `HousekeepingMode` | `dataObjects.<id>.housekeepingMode.type` |
| AuthMode | `AuthMode`, or `HttpAuthMode` for webservices | `connections.<id>.authMode.type` |
| Script | `ParsableScriptDef` | `actions.<id>.scripts[].type` |

The contract for such a class has three parts:

1. a **case class** implementing the trait. Its constructor parameters are the configuration attributes: `Option[...]` makes an attribute optional, a default value makes it optional with that default. Add `(implicit instanceRegistry: InstanceRegistry)` as second parameter list if you need to look up other DataObjects or Connections.
2. a **companion object** extending `FromConfigFactory[<BaseTrait>]` and implementing `fromConfig` with `extract[<YourClass>](config)`. SDLB finds the companion by reflection; without it the configuration fails to parse with *"It does not have a companion object"*.
3. the **`type`** in the configuration. A simple name is resolved against the package `io.smartdatalake.workflow`, so your own class must be referenced with its **fully qualified class name**:

```
executionMode = {
  type = com.company.dataPipeline.LastNPartitionsMode
  nbOfPartitions = 3
}
```

:::info
Own classes are not part of the [Configuration Schema Viewer](../../json-schema-viewer), as it is generated from the SDLB modules.
Document the attributes in the Scaladoc of your case class, the same way the built-in types do.
:::

### Classes referenced by `className`

Some extension points do not need configuration attributes of their own. They are plain classes with a no-argument constructor, instantiated by reflection and referenced by `className`.
Parameters are passed as `options`, so no companion object is needed.

| Interface | Purpose | Configured in |
| --------- | ------- | ------------- |
| `CustomGenericDfTransformer` | engine independent 1:1 DataFrame transformation | `transformers[]` of type `ScalaClassGenericDfTransformer` |
| `CustomGenericDfsTransformer` | engine independent n:m DataFrame transformation | `transformers[]` of type `ScalaClassGenericDfsTransformer` |
| `CustomDfTransformer` / `CustomDfsTransformer` | classic Spark DataFrame transformation | `transformers[]` of type `ScalaClassSparkDfTransformer` / `...DfsTransformer` |
| `CustomFileTransformer` | transform one file as byte streams on Spark executors | `CustomFileAction.transformer.className` |
| `CustomPartitionModeLogic` | select partition values to process | `executionMode` of type `CustomPartitionMode` |
| `StateListener` | react on Action state changes and metrics | `global.stateListeners[].className` |
| `SecretProvider` | resolve secrets from an own store | `global.secretProviders` |

## Own transformer

The most common extension, and the one to reach for first. Implement `CustomGenericDfTransformer` to stay engine independent:

```scala
package com.company.dataPipeline

import io.smartdatalake.workflow.action.generic.customlogic.CustomGenericDfTransformer
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}

class FilterLatitudeTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String],
                         df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    import helper._
    df.where(col("latitude_deg") <= lit(options("maxLatitude").toDouble))
  }
}
```

```
    transformers = [{
      type = ScalaClassGenericDfTransformer
      className = com.company.dataPipeline.FilterLatitudeTransformer
      options = { maxLatitude = "60" }
    }]
```

Instead of overriding `transform`, you can declare any method with parameters of type `DataFrameFunctions`, `Map[String,String]`, `GenericDataFrame` and primitive types; SDLB fills the primitives from `options` and calls it dynamically.
See [Transformations](transformations) for all transformer types, the Spark-typed variants and strongly typed `Dataset` transformations.

Implement `GenericDfTransformer` instead of `CustomGenericDfTransformer` if your transformer needs its own configuration attributes and should be usable like a built-in transformer.

## Own ExecutionMode

Implement the trait `ExecutionMode` and override `apply` to return an `ExecutionModeResult` with the partition values, column filters, file references or options describing the data to process.
See [Implement your own execution mode](executionModes#implement-your-own-execution-mode) for the full example and the available hooks.

## Own DataObject

A DataObject describes a data asset and how to read and write it. Start from the trait `DataObject` and mix in the capabilities your asset supports.
SDLB Actions require these capabilities, so what you mix in decides which Actions can use it:

| Capability trait | Meaning |
| ---------------- | ------- |
| `CanCreateDataFrame` | can be read into a DataFrame - required to be an input of a DataFrame Action |
| `CanWriteDataFrame` | can be written from a DataFrame - required to be an output of a DataFrame Action |
| `CanHandlePartitions` | has partition columns, enables partition-wise processing |
| `TableDataObject` / `TransactionalTableDataObject` | is a table, resp. a table with atomic writes |
| `CanMergeDataFrame` | supports SQL merge - required by DeduplicateAction and HistorizeAction |
| `CanEvolveSchema` | supports schema evolution, see [Schema](schema.md) |
| `CanCreateIncrementalOutput` | can remember a state and deliver increments - required by `DataObjectStateIncrementalMode` |
| `ExpectationValidation` / `CanHandleConstraints` | supports [Data Quality](dataQuality) checks |
| `CanCreateInputStream` / `CanCreateOutputStream` | byte-stream access - required by FileTransferAction |

A minimal engine independent DataObject creating a DataFrame:

```scala
package com.company.dataPipeline

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataobject.generic.CanCreateDataFrame
import io.smartdatalake.workflow.dataobject.{DataObject, DataObjectMetadata}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import scala.reflect.runtime.universe.{Type, typeOf}

case class SequenceDataObject(override val id: DataObjectId,
                              nbOfRows: Int,
                              override val metadata: Option[DataObjectMetadata] = None)
                             (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame {

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(),
                            subFeedType: Type = getSubFeedSupportedTypes.head)
                           (implicit context: ActionPipelineContext): GenericDataFrame = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    helper.createDataFrame((1 to nbOfRows).map(Tuple1(_)), Seq("id"))
  }

  override def getSubFeed(partitionValues: Seq[PartitionValues], subFeedType: Type)
                         (implicit context: ActionPipelineContext): DataFrameSubFeed = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    helper.getSubFeed(getDataFrame(partitionValues, subFeedType), id, partitionValues)
  }

  // declaring the generic type means this DataObject works with every DataFrame engine
  override def getSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[DataFrameSubFeed])
}

object SequenceDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SequenceDataObject =
    extract[SequenceDataObject](config)
}
```

Declaring `Seq(typeOf[DataFrameSubFeed])` as supported type means the DataObject works with every DataFrame engine, because it only uses the generic API obtained from `DataFrameSubFeed.getCompanion`.
Declare a concrete type like `typeOf[SparkSubFeed]` if your implementation needs a specific engine.

Beside the read/write methods, a DataObject has lifecycle hooks worth knowing: `prepare` to validate prerequisites and connections, and `preRead`/`postRead`/`preWrite`/`postWrite` to run side effects around the data access.

### Engine specific implementations

If reading and writing needs engine specific code, keep the DataObject engine independent and let it delegate to implementations of `DataObjectEngine`.
This is how `DeltaLakeTableDataObject` and `IcebergTableDataObject` support classic Spark and Spark Connect with a single configuration object:
the DataObject lives in `sdl-core`, mixes in `HasEngineImplementation`, and the engine modules provide the implementations.
They are discovered on the classpath by reflection and must have a public constructor taking the concrete DataObject as single parameter.

## Own Action

Consider this the last resort: a new Action means new lineage semantics, and most requirements are better served by a transformer on an existing Action.
If you do need one, extend the base class matching your cardinality and engine instead of the bare `Action` trait:

| Base class | Use for |
| ---------- | ------- |
| `DataFrameOneToOneActionImpl` | 1:1 DataFrame Action - implement `input`, `output` and `transform(inputSubFeed, outputSubFeed)` |
| `DataFrameActionImpl` | n:m DataFrame Action - implement `inputs`, `outputs` and `transform(inputSubFeeds, outputSubFeeds)` |
| `FileOneToOneActionImpl` | 1:1 byte-stream Action - implement `transform` and `writeSubFeed` |
| `ScriptActionImpl` | Action executing scripts - implement `execScript` |

These base classes already handle the [execution phases](executionPhases), execution modes, filters, metrics, expectations and schema propagation. What is left to implement is the transformation itself:

```scala
package com.company.dataPipeline

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Condition
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.GenericDfTransformerDef
import io.smartdatalake.workflow.action.{Action, ActionMetadata, DataFrameOneToOneActionImpl}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

case class DeleteFlaggedAction(override val id: ActionId,
                               inputId: DataObjectId,
                               outputId: DataObjectId,
                               deletedFlagColumn: String = "is_deleted",
                               override val cacheInput: Boolean = false,
                               override val cacheOutput: Boolean = false,
                               override val executionMode: Option[ExecutionMode] = None,
                               override val executionCondition: Option[Condition] = None,
                               override val metricsFailCondition: Option[String] = None,
                               override val metadata: Option[ActionMetadata] = None)
                              (implicit val instanceRegistry: InstanceRegistry)
  extends DataFrameOneToOneActionImpl {

  override val input: DataObject with CanCreateDataFrame = getInputDataObject[DataObject with CanCreateDataFrame](inputId)
  override val output: DataObject with CanWriteDataFrame = getOutputDataObject[DataObject with CanWriteDataFrame](outputId)
  override val inputs: Seq[DataObject with CanCreateDataFrame] = Seq(input)
  override val outputs: Seq[DataObject with CanWriteDataFrame] = Seq(output)

  validateConfig()

  override def getTransformers(implicit context: ActionPipelineContext): Seq[GenericDfTransformerDef] = Seq()

  override def transform(inputSubFeed: DataFrameSubFeed, outputSubFeed: DataFrameSubFeed)
                        (implicit context: ActionPipelineContext): DataFrameSubFeed = {
    val functions = inputSubFeed.companion
    import functions._
    val df = inputSubFeed.dataFrame.get.where(not(col(deletedFlagColumn)))
    outputSubFeed.withDataFrame(Some(df))
  }
}

object DeleteFlaggedAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DeleteFlaggedAction =
    extract[DeleteFlaggedAction](config)
}
```

`cacheInput`, `cacheOutput`, `executionMode`, `executionCondition` and `metricsFailCondition` are abstract on the base classes, so they must be declared - as constructor parameters they become configuration attributes of your Action like for every built-in Action.
Add the other optional attributes from the [Actions](actions) reference the same way, e.g. `expectations` or `engineConnectionId`.

`inputSubFeed.companion` gives the `DataFrameFunctions` of the engine the Action was resolved to, so importing it keeps the transformation engine independent.

Use `getInputDataObject`/`getOutputDataObject` with the capability traits your Action needs. They resolve the id in the instance registry and produce a readable configuration error if the DataObject does not support what you require.
Override `validateConfig()` to add your own configuration checks, and call it in the constructor so problems surface when the configuration is parsed and not during the run.

## Testing your extension

Custom code is ordinary Scala, so it can be unit tested directly. Beyond that, SDLB offers configuration validation, dry runs and pipeline simulation to test your extension inside a data pipeline, see [Testing](testing).

Validating that your class can be parsed from configuration is a good first test:

```scala
val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(Seq("src/main/resources"))
assert(registry.getDataObjects.exists(_.isInstanceOf[SequenceDataObject]))
```

## See also

* [Transformations](transformations) - all transformer types and how to write custom transformation code
* [Execution Modes](executionModes#implement-your-own-execution-mode) - implementing an own execution mode
* [Execution Engines](executionEngines) - engine connections and how the engine of an Action is chosen
* [Actions](actions) and [Data Objects](dataObjects) - the built-in implementations and their attributes
* [Build SDL](build.md) - setting up a project depending on SDLB
* [Testing](testing) - config validation, dry run, unit tests and pipeline simulation
