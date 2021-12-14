/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.smartdatalake.{SnowparkDataFrame, SnowparkSession}
import io.smartdatalake.workflow.action.snowparktransformer.SnowparkDfsTransformer
import io.smartdatalake.workflow.action.sparktransformer.{DfsTransformer, SQLDfsTransformer, ScalaClassDfsTransformer, ScalaCodeDfsTransformer}

trait CustomSnowparkDfsTransformer extends Serializable {

  def transform(options: Map[String, String], dfs: Map[String, SnowparkDataFrame])
  : Map[String, SnowparkDataFrame]

  case class CustomDfsTransformerConfig(className: Option[String] = None,
                                        options: Option[Map[String,String]] = None,
                                        runtimeOptions: Option[Map[String,String]] = None) {
    require(className.isDefined)

    // Load Transformer code from appropriate location
    val impl: SnowparkDfsTransformer =
      className.map(clazz => ScalaClassDfsTransformer(className = clazz, options = options.getOrElse(Map()), runtimeOptions = runtimeOptions.getOrElse(Map()))) }

    override def toString: String = {
      if(className.isDefined)       "className: "+className.get
      else if(scalaFile.isDefined)  "scalaFile: "+scalaFile.get
      else if(scalaCode.isDefined)  "scalaCode: "+scalaCode.get
      else                          "sqlCode: "+sqlCode.get
    }
  }
}
