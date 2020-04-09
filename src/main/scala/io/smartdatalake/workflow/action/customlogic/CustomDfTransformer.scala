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
package io.smartdatalake.workflow.action.customlogic

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.misc.CustomCodeUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Interface to define custom logic for a DataFrame
  */
trait CustomDfTransformer extends Serializable {

  /**
   * Functions provided by the creator, used to transform a DataFrame
   *
    * @param session Spark Session
    * @param options additional options
    * @param df DataFrames to be transformed
    * @param dataObjectId Id of DataObject of SubFeed
    * @return Transformed DataFrame
    */
  def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String) : DataFrame

}

case class CustomDfTransformerConfig( className: Option[String] = None, scalaFile: Option[String] = None, scalaCode: Option[String] = None, sqlCode: Option[String] = None, options: Map[String,String] = Map()) {
  require(className.isDefined || scalaFile.isDefined || scalaCode.isDefined || sqlCode.isDefined, "Either className or scalaFile or scalaCode or sqlCode must be defined for CustomDfTransformer")

  val impl : CustomDfTransformer = className.map {
    clazz => CustomCodeUtil.getClassInstanceByName[CustomDfTransformer](clazz)
  }.orElse{
    scalaFile.map {
      file =>
        val fnTransform = CustomCodeUtil.compileFromFile[(SparkSession, Map[String,String], DataFrame, String) => DataFrame](file)
        new CustomDfTransformerWrapper( fnTransform )
    }
  }.orElse{
    scalaCode.map {
      code =>
        val fnTransform = CustomCodeUtil.compileCode[(SparkSession, Map[String,String], DataFrame, String) => DataFrame](code)
        new CustomDfTransformerWrapper( fnTransform )
    }
  }.orElse{
    sqlCode.map {
      code =>
        val fnTransform = CustomCodeUtil.compileSqlCode[(SparkSession, Map[String,String], DataFrame, String) => DataFrame](code)
        new CustomDfTransformerWrapper( fnTransform )
    }
  }.get

  override def toString: String = {
    if(className.isDefined)       "className: "+className.get
    else if(scalaFile.isDefined)  "scalaFile: "+scalaFile.get
    else if(sqlCode.isDefined)    "sqlCode: "+sqlCode.get
    else                          "scalaCode: "+scalaCode.get
  }

  def transform(df: DataFrame, dataObjectId: DataObjectId)(implicit session: SparkSession) : DataFrame = {
    impl.transform(session, options, df, dataObjectId.id)
  }
}
