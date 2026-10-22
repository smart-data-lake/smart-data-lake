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
package io.smartdatalake.meta

import io.smartdatalake.meta.jsonschema.{DefinitionRegistry, JsonSchemaUtil}
import io.smartdatalake.workflow.action.mlflow.MLflowPredictAction
import io.smartdatalake.workflow.action.{Action, CopyAction, CustomScriptAction, FileTransferAction}
import io.smartdatalake.workflow.dataobject._
import org.reflections.Reflections
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

class SubFeedTypeUtilTest extends AnyFunSuite {

  private val allDataFrameSubFeeds = Seq("SQLSubFeed", "ScalaSubFeed", "SnowparkSubFeed", "SparkConnectSubFeed", "SparkSubFeed")

  private def read(tpe: Type): Seq[String] = SubFeedTypeUtil.subFeedTypes(tpe).get.read.map(_.typeSymbol.name.toString)
  private def write(tpe: Type): Seq[String] = SubFeedTypeUtil.subFeedTypes(tpe).get.write.map(_.typeSymbol.name.toString)

  test("DataObjects with Spark, File and Parameter SubFeeds") {
    assert(read(typeOf[ParquetFileDataObject]) == Seq("FileSubFeed", "ParameterSubFeed", "SparkSubFeed"))
    assert(write(typeOf[ParquetFileDataObject]) == Seq("FileSubFeed", "SparkSubFeed"))
    assert(read(typeOf[SFtpFileRefDataObject]) == Seq("FileSubFeed", "ParameterSubFeed"))
    assert(write(typeOf[SFtpFileRefDataObject]) == Seq("FileSubFeed"))
    assert(read(typeOf[MLflowDataObject]) == Seq("ParameterSubFeed"))
    assert(write(typeOf[MLflowDataObject]) == Seq("ParameterSubFeed"))
    // connection is looked up and credentials evaluated lazily
    assert(write(typeOf[BigQueryTableDataObject]) == Seq("SparkSubFeed"))
  }

  test("DataObjects overriding supported SubFeed types") {
    assert(read(typeOf[SnowflakeTableDataObject]) == Seq("ParameterSubFeed", "SnowparkSubFeed", "SparkSubFeed"))
    assert(write(typeOf[SnowflakeTableDataObject]) == Seq("SnowparkSubFeed", "SparkSubFeed"))
    assert(write(typeOf[SparkConnectTableDataObject]) == Seq("SparkConnectSubFeed"))
    // generic DataFrameSubFeed is replaced by all known DataFrame SubFeeds
    assert(read(typeOf[PKViolatorsDataObject]) == (allDataFrameSubFeeds :+ "ParameterSubFeed").sorted)
    assert(write(typeOf[PKViolatorsDataObject]).isEmpty)
  }

  test("DataObjects with engine implementations") {
    assert(write(typeOf[DeltaLakeTableDataObject]) == Seq("SparkConnectSubFeed", "SparkSubFeed"))
    assert(write(typeOf[IcebergTableDataObject]) == Seq("SparkConnectSubFeed", "SparkSubFeed"))
    assert(write(typeOf[JdbcTableDataObject]) == Seq("SQLSubFeed", "SparkSubFeed"))
    assert(read(typeOf[JdbcViewDataObject]) == Seq("ParameterSubFeed", "SQLSubFeed", "SparkSubFeed"))
    assert(write(typeOf[JdbcViewDataObject]) == Seq("SQLSubFeed"))
  }

  test("Actions") {
    assert(read(typeOf[CopyAction]) == allDataFrameSubFeeds)
    assert(read(typeOf[FileTransferAction]) == Seq("FileSubFeed"))
    assert(read(typeOf[CustomScriptAction]) == Seq("ParameterSubFeed"))
    // MLflow Actions restrict the transformer SubFeed type to Spark
    assert(read(typeOf[MLflowPredictAction]) == Seq("SparkSubFeed"))
  }

  test("SubFeed types are added to json schema definitions") {
    val converter = new JsonSchemaUtil.JsonTypeConverter(new Reflections, new DefinitionRegistry)
    val dataObjectDef = converter.fromGenericTypeDef(GenericTypeUtil.typeDefForClass(typeOf[ParquetFileDataObject], baseType = Some(typeOf[DataObject])))
    assert(dataObjectDef.`x-readSubFeedTypes` == Seq("FileSubFeed", "ParameterSubFeed", "SparkSubFeed"))
    assert(dataObjectDef.`x-writeSubFeedTypes` == Seq("FileSubFeed", "SparkSubFeed"))
    assert(dataObjectDef.`x-subFeedTypes`.isEmpty)
    val actionDef = converter.fromGenericTypeDef(GenericTypeUtil.typeDefForClass(typeOf[FileTransferAction], baseType = Some(typeOf[Action])))
    assert(actionDef.`x-subFeedTypes` == Seq("FileSubFeed"))
    assert(actionDef.`x-readSubFeedTypes`.isEmpty)
    val json = org.json4s.jackson.JsonMethods.compact(actionDef.toJson)
    assert(json.contains(""""x-subFeedTypes":["FileSubFeed"]"""))
    assert(!json.contains("x-readSubFeedTypes"))
  }
}
