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

import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.definitions.BasicAuthMode
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.connection.SnowflakeConnection
import io.smartdatalake.workflow.dataobject.{SnowflakeTableDataObject, Table}

/**
 * This is an integration test to read & write to Snowflake with Spark and Snowpark.
 * It needs to be run manually because you need to provide a Snowflake environment.
 * Please configure this in SnowflakeConnectionConfig.
 */
object SnowflakeDataObjectIT extends App {

  implicit val sparkSession = TestUtil.sessionHiveCatalog
  implicit val instanceRegistry = new InstanceRegistry()
  implicit val context =  ConfigToolbox.getDefaultActionPipelineContext

  instanceRegistry.register(SnowflakeConnectionConfig.sfConnection)
  val testDO = SnowflakeTableDataObject("test1", Table(Some("test"), "abc"), connectionId = "sfCon")
  instanceRegistry.register(testDO)

  // create table & write some data with Snowpark
  val sfSession = testDO.snowparkSession
  import sfSession.implicits._
  val dfComplex = Seq(
    (1, "a", "A"),
    (2, "b", "B"),
    (3, "c", "C"),
    (4, "d", "D"),
    (5, "e", "E")
  ).toDF("id","s1", "s2")
  testDO.writeSnowparkDataFrame(dfComplex, partitionValues = Seq())

  // read data with Snowpark and Spark
  println("SNOWPARK")
  val dfTestDOSnowparkTestDO = testDO.getSnowparkDataFrame()
  dfTestDOSnowparkTestDO.show
  assert(dfTestDOSnowparkTestDO.count() == 5)

  println("SPARK")
  val dfTestDOSparkTestDO = testDO.getSparkDataFrame()
  dfTestDOSparkTestDO.show
  assert(dfTestDOSparkTestDO.count() == 5)

  // cleanup
  testDO.dropTable

}
