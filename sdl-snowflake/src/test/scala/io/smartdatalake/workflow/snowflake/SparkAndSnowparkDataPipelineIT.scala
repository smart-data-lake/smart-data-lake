/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.snowflake

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.definitions.BasicAuthMode
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.sparktransformer.{AdditionalColumnsTransformer, FilterTransformer}
import io.smartdatalake.workflow.connection.SnowflakeConnection
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, SnowflakeTableDataObject, Table}

import java.nio.file.Files

/**
 * This is an integration test with data pipeline combining Spark and Snowpark
 * The first Action will write with Spark to Snowflake, creating a new table in Snowflake.
 * The second Action will read the new Snowflake table with Snowpark and create another Snowflake table.
 * The SubFeed between Action1 and Action2 will apply schema conversion which is needed in init phase, as the tables in Snowflake dont exist yet.
 * Action2 will also apply two generic transformation, e.g. filter and add runId column.
 * It needs to be run manually because you need to provide a Snowflake environment.
 * Please configure this in SnowflakeConnectionConfig.
 */
object SparkAndSnowparkDataPipelineIT extends App {

  val sdlb = new DefaultSmartDataLakeBuilder()
  implicit val instanceRegistry = sdlb.instanceRegistry
  implicit val sparkSession = TestUtil.sessionHiveCatalog
  implicit val context =  ConfigToolbox.getDefaultActionPipelineContext

  val tempDir = Files.createTempDirectory("test")
  val tempPath = tempDir.toAbsolutePath.toString

  instanceRegistry.register(SnowflakeConnectionConfig.sfConnection)

  // setup DataObjects
  val feed = "copy"
  val srcTable = Table(Some("default"), "copy_input")
  val srcDO = HiveTableDataObject( "src1", Some(tempPath+s"/${srcTable.fullName}"), table = srcTable, numInitialHdfsPartitions = 1)
  srcDO.dropTable
  instanceRegistry.register(srcDO)
  val tgt1Table = Table(Some("test"), "tgt1", None, Some(Seq("lastname")))
  val tgt1DO = SnowflakeTableDataObject( "tgt1", tgt1Table, connectionId = "sfCon")
  tgt1DO.dropTable
  instanceRegistry.register(tgt1DO)
  val tgt2Table = Table(Some("test"), "tgt2", None, Some(Seq("lastname")))
  val tgt2DO = SnowflakeTableDataObject( "tgt2", tgt2Table, connectionId = "sfCon")
  tgt2DO.dropTable
  instanceRegistry.register(tgt2DO)

  // first action copy with Spark from Hive to Snowflake
  val action1 = CopyAction("copySpark", srcDO.id, tgt1DO.id)
  instanceRegistry.register(action1)
  val action2 = CopyAction("copySnowpark", tgt1DO.id, tgt2DO.id,
    transformers = Seq(
      //ScalaClassDfTransformer(className = classOf[TestOptionsDfTransformer].getName, options = Map("test" -> "test"), runtimeOptions = Map("appName" -> "application")),
      FilterTransformer(filterClause = "lastname='jonson'"),
      AdditionalColumnsTransformer(additionalColumns = Map("run_id" -> "runId"))
    )
  )
  instanceRegistry.register(action2)

  // prepare data
  import sparkSession.implicits._
  val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
  srcDO.writeSparkDataFrame(l1, Seq())

  // run
  val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "ids:copy.*", applicationName = Some(feed))
  sdlb.run(sdlConfig)

}
