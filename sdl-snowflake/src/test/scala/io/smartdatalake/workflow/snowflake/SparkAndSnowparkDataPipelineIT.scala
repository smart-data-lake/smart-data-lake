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

import com.snowflake.snowpark.{DataFrame, Session}
import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.ConfigToolbox
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.action.{CopyAction, CustomDataFrameAction}
import io.smartdatalake.workflow.action.generic.customlogic.CustomGenericDfTransformer
import io.smartdatalake.workflow.action.generic.transformer.{AdditionalColumnsTransformer, FilterTransformer, SQLDfTransformer, SQLDfsTransformer, ScalaClassGenericDfTransformer}
import io.smartdatalake.workflow.action.snowflake.customlogic.CustomSnowparkDfTransformer
import io.smartdatalake.workflow.action.snowflake.transformer.ScalaClassSnowparkDfTransformer
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
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
 * Also ensure that the environment variable SNOWFLAKE_SCHEMA is set to a schema that is accessible
 * with the connection defined in SnowflakeConnectionConfig
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
  val tgt1Table = Table(Some(System.getenv("SNOWFLAKE_SCHEMA")), "tgt1", None, Some(Seq("lastname")))
  val tgt1DO = SnowflakeTableDataObject("tgt1", tgt1Table, connectionId = "sfCon")
  tgt1DO.dropTable
  instanceRegistry.register(tgt1DO)
  val tgt2Table = Table(Some(System.getenv("SNOWFLAKE_SCHEMA")), "tgt2", None, Some(Seq("lastname")))
  val tgt2DO = SnowflakeTableDataObject("tgt2", tgt2Table, connectionId = "sfCon")
  tgt2DO.dropTable
  instanceRegistry.register(tgt2DO)
  val tgt3Table = Table(Some(System.getenv("SNOWFLAKE_SCHEMA")), "tgt3", None, Some(Seq("lastname")))
  val tgt3DO = SnowflakeTableDataObject("tgt3", tgt3Table, connectionId = "sfCon")
  tgt3DO.dropTable
  instanceRegistry.register(tgt3DO)


  // first action copy with Spark from Hive to Snowflake
  val action1 = CopyAction("copySpark", srcDO.id, tgt1DO.id)
  instanceRegistry.register(action1)
  // second action copy with Snowpark from Snowflake to Snowflake
  val action2 = CopyAction("copySnowpark", tgt1DO.id, tgt2DO.id,
    transformers = Seq(
      // a custom transformer written in Snowpark
      ScalaClassSnowparkDfTransformer(className = classOf[TestOptionsSnowparkDfTransformer].getName, options = Map("test" -> "test"), runtimeOptions = Map("appName" -> "application")),
      // generic predefined transformers
      FilterTransformer(filterClause = "lastname='jonson'"),
      AdditionalColumnsTransformer(additionalColumns = Map("run_id" -> "runId")),
      // a custom generic transformer
      ScalaClassGenericDfTransformer(className = classOf[TestAdd1GenericDfTransformer].getName, options = Map("column" -> "rating")),
      SQLDfTransformer(code = "select %{inputViewName}.*, run_id + 1 as run_id2 from %{inputViewName}"),
      SQLDfTransformer(code = "select %{inputViewName}.*, run_id2 + 1 as run_id3 from %{inputViewName}")
    )
  )
  instanceRegistry.register(action2)
  val action3 = CustomDataFrameAction("customSnowpark", Seq(tgt1DO.id, tgt2DO.id), Seq(tgt3DO.id),
    transformers = Seq(
      SQLDfsTransformer(code = Map(
        tgt3DO.id -> """
          select tgt1.*, run_id2 + 1 as run_id3
          from %{inputViewName_tgt1} as tgt1
          join %{inputViewName_tgt2} as tgt2 on tgt1.firstname=tgt2.firstname and tgt1.lastname=tgt2.lastname
        """
      )),
    )
  )
  instanceRegistry.register(action3)

  // prepare data
  import sparkSession.implicits._
  val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
  srcDO.writeSparkDataFrame(l1, Seq())

  // run
  val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "ids:.*", applicationName = Some(feed))
  sdlb.run(sdlConfig)

}

class TestOptionsSnowparkDfTransformer extends CustomSnowparkDfTransformer {
  def transform(session: Session, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {
    import com.snowflake.snowpark.functions._
    df.withColumn("test", lit(options("test")+"-"+options("appName")))
  }
}

class TestAdd1GenericDfTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._
    val columnToAdd1 = options("column")
    df.withColumn(columnToAdd1, col(columnToAdd1) + lit(1))
  }
}