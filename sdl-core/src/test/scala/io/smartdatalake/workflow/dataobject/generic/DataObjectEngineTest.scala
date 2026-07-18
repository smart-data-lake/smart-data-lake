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
package io.smartdatalake.workflow.dataobject.generic

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import io.smartdatalake.workflow.dataobject.{DataObject, DataObjectMetadata}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Dummy engine trait for testing discovery and routing of [[HasEngineImplementation]].
 */
private[smartdatalake] trait DummyTestEngine extends DataObjectEngine

// concrete implementations discovered by reflection
class DummyScalaTestEngine(dataObject: DummyEngineTestDataObject) extends DummyTestEngine {
  override val subFeedType: Type = typeOf[ScalaSubFeed]
}
class DummyGenericTestEngine(dataObject: DummyEngineTestDataObject) extends DummyTestEngine {
  override val subFeedType: Type = typeOf[DataFrameSubFeed]
}

case class DummyEngineTestDataObject(override val id: DataObjectId)(implicit instanceRegistry: InstanceRegistry)
  extends DataObject with HasEngineImplementation[DummyTestEngine] {
  override def metadata: Option[DataObjectMetadata] = None
  override def factory: FromConfigFactory[DataObject] = throw new NotImplementedError()
  override protected def createEngines: Seq[DummyTestEngine] =
    DataObjectEngine.createEngines[DummyTestEngine, DummyEngineTestDataObject](this, classOf[DummyEngineTestDataObject])
  override protected def engineNotFoundHint: String = "Add a dummy engine module."

  // expose protected routing methods for testing
  def engineForSubFeedType(tpe: Type): DummyTestEngine = engine(tpe)
  def engineForContext(implicit context: ActionPipelineContext): DummyTestEngine = engine

  // unused abstract members of CanCreateDataFrame/CanWriteDataFrame
  override def getDataFrame(partitionValues: Seq[PartitionValues], subFeedType: Type)(implicit context: ActionPipelineContext): GenericDataFrame = throw new NotImplementedError()
  override private[smartdatalake] def getSubFeed(partitionValues: Seq[PartitionValues], subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed = throw new NotImplementedError()
  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = throw new NotImplementedError()
}

/**
 * Dummy engine trait without implementations on the classpath.
 */
private[smartdatalake] trait DummyMissingTestEngine extends DataObjectEngine

case class DummyMissingEngineTestDataObject(override val id: DataObjectId)(implicit instanceRegistry: InstanceRegistry)
  extends DataObject with HasEngineImplementation[DummyMissingTestEngine] {
  override def metadata: Option[DataObjectMetadata] = None
  override def factory: FromConfigFactory[DataObject] = throw new NotImplementedError()
  override protected def createEngines: Seq[DummyMissingTestEngine] =
    DataObjectEngine.createEngines[DummyMissingTestEngine, DummyMissingEngineTestDataObject](this, classOf[DummyMissingEngineTestDataObject])
  override protected def engineNotFoundHint: String = "Add a dummy engine module."

  override def getDataFrame(partitionValues: Seq[PartitionValues], subFeedType: Type)(implicit context: ActionPipelineContext): GenericDataFrame = throw new NotImplementedError()
  override private[smartdatalake] def getSubFeed(partitionValues: Seq[PartitionValues], subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed = throw new NotImplementedError()
  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = throw new NotImplementedError()
}

class DataObjectEngineTest extends AnyFunSuite {

  test("engine implementations are discovered on classpath and derive supported subFeed types") {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    val dataObject = DummyEngineTestDataObject(DataObjectId("test1"))
    assert(dataObject.getSubFeedSupportedTypes.size == 2)
    assert(dataObject.getSubFeedSupportedTypes.exists(_ =:= typeOf[ScalaSubFeed]))
    assert(dataObject.writeSubFeedSupportedTypes.exists(_ =:= typeOf[DataFrameSubFeed]))
  }

  test("routing by explicit subFeedType selects matching engine, unknown type throws") {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    val dataObject = DummyEngineTestDataObject(DataObjectId("test2"))
    assert(dataObject.engineForSubFeedType(typeOf[ScalaSubFeed]).isInstanceOf[DummyScalaTestEngine])
    assert(dataObject.engineForSubFeedType(typeOf[DataFrameSubFeed]).isInstanceOf[DummyGenericTestEngine])
    val ex = intercept[IllegalStateException](dataObject.engineForSubFeedType(typeOf[String]))
    assert(ex.getMessage.contains("No engine implementation for subFeedType"))
  }

  test("routing by context selects engine matching default engine connection") {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    // ScalaConnection is registered with id default-engine and subFeedType ScalaSubFeed
    instanceRegistry.register(ScalaTestUtil.defaultScalaConnection)
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    val dataObject = DummyEngineTestDataObject(DataObjectId("test3"))
    assert(dataObject.engineForContext.isInstanceOf[DummyScalaTestEngine])
  }

  test("routing by context without engine connection falls back to first engine with warning") {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    val dataObject = DummyEngineTestDataObject(DataObjectId("test4"))
    // no engine connection registered -> fallback to first discovered engine, no exception
    assert(dataObject.engineForContext.isInstanceOf[DummyTestEngine])
  }

  test("missing engine implementation throws ConfigurationException with hint") {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    val dataObject = DummyMissingEngineTestDataObject(DataObjectId("test5"))
    val ex = intercept[ConfigurationException](dataObject.getSubFeedSupportedTypes)
    assert(ex.getMessage.contains("No engine implementation found on classpath"))
    assert(ex.getMessage.contains("Add a dummy engine module."))
  }
}
