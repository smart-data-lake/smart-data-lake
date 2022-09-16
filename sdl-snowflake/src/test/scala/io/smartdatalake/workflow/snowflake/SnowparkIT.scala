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

import com.snowflake.snowpark.types._
import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.definitions.BasicAuthMode
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.{DataFrameSubFeed, SubFeed}
import io.smartdatalake.workflow.connection.SnowflakeConnection
import io.smartdatalake.workflow.dataframe.snowflake.{SnowparkColumn, SnowparkSchema, SnowparkSubFeed}
import io.smartdatalake.workflow.dataobject.{SnowflakeTableDataObject, Table}
import org.scalatest.Matchers.intercept

import scala.reflect.runtime.universe.typeOf

/**
 * This is an integration test to check implementations of Snowpark.
 */
object SnowparkIT extends App {

  implicit val sparkSession = TestUtil.sessionHiveCatalog
  implicit val instanceRegistry = new InstanceRegistry()
  implicit val context =  ConfigToolbox.getDefaultActionPipelineContext

  instanceRegistry.register(SnowflakeConnectionConfig.sfConnection)
  val testDO = SnowflakeTableDataObject("test1", Table(Some("test"), "abc"), connectionId = "sfCon")
  instanceRegistry.register(testDO)

  val sfSession = testDO.snowparkSession
  import sfSession.implicits._

  // convert Snowpark struct to SQL data type is not possible
  val struct1 = StructType( Seq(
    StructField("c1", FloatType),
    StructField("c2", DoubleType),
  ))
  intercept[UnsupportedOperationException]{
    convertToSFType(struct1)
  }

  // convert Snowpark schema to SQL DDL
  val schema =  StructType( Seq(
    StructField("c1", FloatType),
    StructField("c2", DoubleType),
  ))
  intercept[UnsupportedOperationException]{
    convertToSFType(struct1)
  }

}
