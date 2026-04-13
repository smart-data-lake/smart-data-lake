/*
 * sdl-lang - Build your data lake the smart way.
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
package io.smartdatalake.lab

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.testutils.spark.dataset.Collection.dsComplex
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfsTransformer
import io.smartdatalake.workflow.dataobject.ParquetFileDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class SmartDataLakeBuilderLabTest extends AnyFunSuite {

  protected implicit val session: SparkSession = TestUtil.session

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  test("create one DataFrame using Transformer") {
    val lab = new SmartDataLakeBuilderLab(session, Seq("cp:/labConfigTest.conf"), new DataObjectCatalogTest(_,_), new ActionCatalogTest(_,_))
    SmartDataLakeBuilderLab.enableWritingDataObjects = true
    lab.dataObjects.parquetTest.write(dsComplex)
    val dfs = lab.buildDataFrames.withTransformer(new MySingleDfTransformer).get
    assert(dfs.size == 1)
    intercept[IllegalArgumentException](lab.buildDataFrames.withTransformer(new MyDfsTransformer).getOne("dfUnknown"))
  }

}

class MySingleDfTransformer extends CustomDfsTransformer {
  def transform(session: SparkSession, dfParquetTest: DataFrame): DataFrame = {
    dfParquetTest
  }
}

class MyDfsTransformer extends CustomDfsTransformer {
  def transform(dfParquetTest: DataFrame): Map[String,DataFrame] = {
    Map("dfParquetTest" -> dfParquetTest)
  }
}

class DataObjectCatalogTest(instanceRegistry: InstanceRegistry, context: ActionPipelineContext) {
  val parquetTest: LabSparkDataObjectWrapper[ParquetFileDataObject] = LabSparkDataObjectWrapper(instanceRegistry.get[ParquetFileDataObject](DataObjectId("parquetTest")), context)
}

class ActionCatalogTest(instanceRegistry: InstanceRegistry, context: ActionPipelineContext) {
}