/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.mycompany.sdlb

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigToolbox, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{Condition, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.GenericDfTransformerDef
import io.smartdatalake.workflow.action.{Action, ActionMetadata, DataFrameOneToOneActionImpl}
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.dataobject.generic._
import io.smartdatalake.workflow.dataobject.{DataObject, DataObjectMetadata}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Asserts that own DataObjects and Actions can be implemented **outside** the `io.smartdatalake` package.
 *
 * The classes below deliberately live in `com.mycompany.sdlb` and mix in the capability traits an own
 * implementation typically needs. If a member of one of these traits gets `private[smartdatalake]` visibility
 * again, this file no longer compiles, which is the point of the test: SDLB is a library and its extension
 * points must be implementable by third party code, see docs/docs/reference/extending.md.
 */
class ExternalExtensionTest extends AnyFunSuite with Matchers {

  test("own DataObject and Action outside the io.smartdatalake package are parsed from config") {
    val configPath = getClass.getResource("/config/externalExtension.conf").getPath
    val (registry, _) = ConfigToolbox.loadAndParseConfig(Seq(configPath))

    val dataObject = registry.get[ExternalDataObject](DataObjectId("ext-src"))
    dataObject.partitions shouldBe Seq("dt")

    val action = registry.get[ExternalAction](ActionId("ext-copy"))
    action.inputs.map(_.id.id) shouldBe Seq("ext-src")
    action.outputs.map(_.id.id) shouldBe Seq("ext-tgt")
  }
}

/**
 * An own DataObject implemented outside the `io.smartdatalake` package, mixing in the capability traits
 * a DataFrame DataObject typically needs.
 */
case class ExternalDataObject(override val id: DataObjectId,
                              override val partitions: Seq[String] = Seq(),
                              override val constraints: Seq[Constraint] = Seq(),
                              override val expectations: Seq[Expectation] = Seq(),
                              override val allowSchemaEvolution: Boolean = false,
                              override val expectedPartitionsCondition: Option[String] = None,
                              override val metadata: Option[DataObjectMetadata] = None)
                             (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with CanWriteDataFrame with CanHandlePartitions
    with CanEvolveSchema with ExpectationValidation {

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(),
                            subFeedType: Type = getSubFeedSupportedTypes.head)
                           (implicit context: ActionPipelineContext): GenericDataFrame = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    helper.createDataFrame(Seq(Tuple1("2024-01-01")), Seq("dt"))
  }

  override def getSubFeed(partitionValues: Seq[PartitionValues], subFeedType: Type)
                         (implicit context: ActionPipelineContext): DataFrameSubFeed = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    helper.getSubFeed(getDataFrame(partitionValues, subFeedType), id, partitionValues)
  }

  override def getSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[DataFrameSubFeed])

  override def writeSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[DataFrameSubFeed])

  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues],
                              isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])
                             (implicit context: ActionPipelineContext): MetricsMap = Map()

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = Seq()

  override def prepare(implicit context: ActionPipelineContext): Unit = super.prepare

  override def preRead(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = ()

  override def postWrite(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit =
    super.postWrite(partitionValues)
}

object ExternalDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ExternalDataObject =
    extract[ExternalDataObject](config)
}

/**
 * An own 1:1 DataFrame Action implemented outside the `io.smartdatalake` package.
 */
case class ExternalAction(override val id: ActionId,
                          inputId: DataObjectId,
                          outputId: DataObjectId,
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
                        (implicit context: ActionPipelineContext): DataFrameSubFeed =
    applyTransformers(getTransformers, inputSubFeed, outputSubFeed)

  override def transformPartitionValues(partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String, String])
                                       (implicit context: ActionPipelineContext): Map[PartitionValues, PartitionValues] =
    applyTransformers(getTransformers, partitionValues, executionModeResultOptions)
}

object ExternalAction extends FromConfigFactory[Action] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ExternalAction =
    extract[ExternalAction](config)
}
