/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.action

import io.smartdatalake.app.GlobalConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.workflow.action.generic.transformer.SQLDfsTransformer
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.workflow.dataobject.{IcebergTableDataObject, IcebergTestUtils, Table}
import io.smartdatalake.workflow.{ActionDAGRun, ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Files

class IcebergCustomDataFrameTest extends FunSuite with BeforeAndAfter {

  // set additional spark options for delta lake
  protected implicit val session: SparkSession = IcebergTestUtils.session

  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextPrep = contextInit.copy(phase = ExecutionPhase.Prepare)
  val contextExec = contextInit.copy(phase = ExecutionPhase.Exec) // note that mutable Map dataFrameReuseStatistics is shared between contextInit & contextExec like this!
  Environment._globalConfig = GlobalConfig()

  before {
    instanceRegistry.clear()
  }

  test("CustomDataFrameAction with recursiveInput") {
    // setup DataObjects
    val srcDO1 = MockDataObject("src1")
    instanceRegistry.register(srcDO1)
    val recTable = Table(catalog = Some("iceberg1"), db = Some("default"), name = "recursive1", primaryKey = Some(Seq("lastname", "cnt")))
    val recDO = IcebergTableDataObject("rec1", Some(tempPath + s"/${recTable.fullName}"), table = recTable, schemaMin = Some(SparkSchema(SchemaUtil.getSchemaFromDdl("lastname string, cnt long"))))
    recDO.dropTable
    instanceRegistry.register(recDO)
    val tgt1DO = MockDataObject("tgt1")
    instanceRegistry.register(tgt1DO)

    // prepare DAG
    val df1 = Seq(("doe", "john", 5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeSparkDataFrame(df1)
    val action = CustomDataFrameAction("a", Seq(srcDO1.id), Seq(tgt1DO.id, recDO.id), recursiveInputIds = Seq(recDO.id),
      transformers = Seq(SQLDfsTransformer(code = Map(
        tgt1DO.id.id -> "select * from src1",
        recDO.id.id -> "select lastname, sum(cnt) as cnt from (select lastname, cnt from %{inputViewName_rec1} union all select lastname, 1 from %{inputViewName_src1}) group by lastname"))
      )
    )
    instanceRegistry.register(action)

    val dag = ActionDAGRun(Seq(action))

    // first dag run
    dag.prepare(contextPrep)
    dag.init(contextInit)
    dag.exec(contextExec)

    assert(recDO.getSparkDataFrame().count() > 0)

  }

}
