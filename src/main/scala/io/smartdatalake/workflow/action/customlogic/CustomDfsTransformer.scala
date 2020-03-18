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

import io.smartdatalake.util.misc.CustomCodeUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Same trait as [[CustomDfTransformer]] but supported multiple input and outputs.
 */
trait CustomDfsTransformer extends Serializable {

  def transform(session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]) : Map[String,DataFrame]

}

case class CustomDfsTransformerConfig( className: Option[String] = None, scalaFile: Option[String] = None, scalaCode: Option[String] = None, options: Map[String,String] = Map()) {
  require(className.isDefined || scalaFile.isDefined || scalaCode.isDefined, "Either className or scalaFile must be defined for CustomDfTransformer")

  // Load Transformer code from appropriate location
  val impl: CustomDfsTransformer = className.map {
    clazz => CustomCodeUtil.getClassInstanceByName[CustomDfsTransformer](clazz)
  }.orElse{
    scalaFile.map {
      file =>
        val fnTransform = CustomCodeUtil.compileFromFile[(SparkSession, Map[String,String], Map[String,DataFrame]) => Map[String,DataFrame]](file)
        new CustomDfsTransformerWrapper( fnTransform )
    }
  }.orElse{
    scalaCode.map {
      code =>
        val fnTransform = CustomCodeUtil.compileCode[(SparkSession, Map[String,String], Map[String,DataFrame]) => Map[String,DataFrame]](code)
        new CustomDfsTransformerWrapper( fnTransform )
    }
  }.get

  override def toString: String = {
    if(className.isDefined)       "className: "+className.get
    else if(scalaFile.isDefined)  "scalaFile: "+scalaFile.get
    else                          "scalaCode: "+scalaCode.get
  }

  def transform(dfs: Map[String,DataFrame])(implicit session: SparkSession) : Map[String,DataFrame] = {
    impl.transform(session, options, dfs)
  }
}
