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

import com.snowflake.snowpark
import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataobject.{SnowflakeTableDataObject, Table}
import org.apache.spark


/**
 * This is an integration test to read & write to Snowflake with Spark and Snowpark.
 * It needs to be run manually because you need to provide a Snowflake environment.
 * Please configure this in SnowflakeConnectionConfig.
 */
object SnowflakeDataObjectIT extends App with SmartDataLakeLogger {

  implicit val sparkSession = TestUtil.session
  implicit val instanceRegistry = new InstanceRegistry()
  implicit val context =  ConfigToolbox.getDefaultActionPipelineContext

  instanceRegistry.register(SnowflakeConnectionConfig.sfConnection)

  val testDO = SnowflakeTableDataObject("test1", Table(Some(System.getenv("SNOWFLAKE_SCHEMA")), "abc"), connectionId = "sfCon", virtualPartitions = Seq("dt"), saveMode = SDLSaveMode.Overwrite)
  instanceRegistry.register(testDO)

  // cleanup
  testDO.dropTable

  // create table & write some data with Snowpark
  val sfSession = testDO.snowparkSession
  import sfSession.implicits._
  {
    val df = Seq(
      (1, "a", "A", "20210201"),
      (2, "b", "B", "20210201"),
      (3, "c", "C", "20210201"),
      (4, "d", "D", "20210201"),
      (5, "e", "E", "20210202")
    ).toDF("id", "s1", "s2", "dt")
    val metrics = testDO.writeSnowparkDataFrame(df, partitionValues = Seq(PartitionValues(Map("dt"->"20210201")),PartitionValues(Map("dt"->"20210202"))))
    logger.info("Finished writing using Snowpark " + metrics)

    // partitions
    val pvs = testDO.listPartitions
    assert(pvs.toSet == Set(PartitionValues(Map("dt" -> "20210201")), PartitionValues(Map("dt" -> "20210202"))))

    // read data with Snowpark and Spark
    println("SNOWPARK")
    val dfTestSnowpark = testDO.getSnowparkDataFrame()
    dfTestSnowpark.select("id","s1","S2","dt").show
    assert(dfTestSnowpark.count() == 5)
    // Interestingly, Snowpark converts a Scala Int to a LongType in the Snowpark DataFrame written to Snowflake
    assert(dfTestSnowpark.schema("id").dataType == snowpark.types.LongType)
    assert(dfTestSnowpark.schema.names == Seq("ID","S1","S2","DT"))

    println("SPARK")
    val dfTestSpark = testDO.getSparkDataFrame()
    dfTestSpark.select("id","s1","S2","dt").show
    assert(dfTestSpark.count() == 5)
    // Interestingly, Snowpark converts a Scala Int to a LongType in the Snowpark DataFrame written to Snowflake
    // This becomes a Decimal(19,0) in the Snowflake table.
    assert(dfTestSpark.schema("id").dataType == spark.sql.types.DecimalType(19,0))
    assert(dfTestSpark.schema.names.toSeq == Seq("id","s1","s2","dt"))
  }

  // overwrite virtualPartition dt=20210201, add dt=20210203
  {
    val df = Seq(
      (4, "d", "D", "20210201"),
      (6, "f", "F", "20210203")
    ).toDF("id", "s1", "s2", "dt")
    val metrics = testDO.writeSnowparkDataFrame(df, partitionValues = Seq(PartitionValues(Map("dt"->"20210201")),PartitionValues(Map("dt"->"20210203"))))
    logger.info("Finished writing using Snowpark " + metrics)
    assert(metrics("rows_inserted") == 2)

    // read data with Snowpark
    println("SNOWPARK: 20210201 overwritten, 20210203 added")
    val dfTestSnowpark = testDO.getSnowparkDataFrame()
    assert(dfTestSnowpark.count() == 3)
  }

  // cleanup
  testDO.dropTable

}
