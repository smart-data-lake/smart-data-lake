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
package io.smartdatalake.config

import configs.{ConfigError, Configs, Result}
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.definitions.{AuthMode, Condition, ExecutionMode}
import io.smartdatalake.util.hdfs.SparkRepartitionDef
import io.smartdatalake.workflow.action.customlogic.{CustomDfCreatorConfig, CustomDfTransformerConfig, CustomDfsTransformerConfig, CustomFileTransformerConfig, SparkUDFCreatorConfig}
import io.smartdatalake.workflow.dataobject.WebserviceFileDataObject
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

trait ConfigImplicits {

  /**
   * default naming strategy is to allow lowerCamelCase and hypen-separated key naming, and fail on superfluous keys
   */
  //TODO: this is needed for configs 0.5.0
  /*
  implicit def sdlDefaultNaming[A]: ConfigKeyNaming[A] =
    ConfigKeyNaming.hyphenSeparated[A].or(ConfigKeyNaming.lowerCamelCase[A].apply _)
      .withFailOnSuperfluousKeys()
  */

  /**
   * A ConfigReader reader that reads [[StructType]] values.
   *
   * This reader parses a [[StructType]] from a DDL string.
   */
  implicit val structTypeReader: Configs[StructType] = Configs.fromTry { (c, p) =>
    StructType.fromDDL(c.getString(p))
  }

  /**
   * A ConfigReader reader that reads [[OutputMode]].
   */
  implicit val outputModeReader: Configs[OutputMode] = {
    Configs.fromConfig(_.toString.toLowerCase match {
      case "append" => Result.successful(OutputMode.Append())
      case "complete" => Result.successful(OutputMode.Complete())
      case "update" => Result.successful(OutputMode.Update())
      case x => Result.failure(ConfigError(s"$x is not a value of OutputMode. Supported values are append, complete, update."))
    })
  }

  // --------------------------------------------------------------------------------
  // Config reader to circumvent problems related to a bug:
  // The problem is that kxbmap sometimes can not find the correct config reader for
  // some non-trivial nested types, e.g. List[CustomCaseClass] or Option[CustomCaseClass]
  // see: https://github.com/kxbmap/configs/issues/44
  // TODO: check periodically if still needed, should not be needed with scala 2.13+
  // --------------------------------------------------------------------------------
  implicit val customDfCreatorConfigReader: Configs[CustomDfCreatorConfig] = Configs.derive[CustomDfCreatorConfig]
  implicit val customDfTransformerConfigReader: Configs[CustomDfTransformerConfig] = Configs.derive[CustomDfTransformerConfig]
  implicit val customDfsTransformerConfigReader: Configs[CustomDfsTransformerConfig] = Configs.derive[CustomDfsTransformerConfig]
  implicit val customFileTransformerConfigReader: Configs[CustomFileTransformerConfig] = Configs.derive[CustomFileTransformerConfig]
  implicit val sparkUdfCreatorConfigReader: Configs[SparkUDFCreatorConfig] = Configs.derive[SparkUDFCreatorConfig]
  implicit val sparkRepartitionDefReader: Configs[SparkRepartitionDef] = Configs.derive[SparkRepartitionDef]
  implicit val executionModeReader: Configs[ExecutionMode] = Configs.derive[ExecutionMode]
  implicit val conditionReader: Configs[Condition] = Configs.derive[Condition]
  implicit val authModeReader: Configs[AuthMode] = Configs.derive[AuthMode]
  // --------------------------------------------------------------------------------

  implicit def mapDataObjectIdStringReader(implicit mapReader: Configs[Map[String,String]]): Configs[Map[DataObjectId, String]] = {
    Configs.fromConfig { c => mapReader.extract(c).map(_.map{ case (k,v) => (DataObjectId(k), v)})}
  }

  /**
   * A ConfigReader reader that reads [[ConnectionId]] values.
   */
  implicit val connectionIdReader: Configs[ConnectionId] = Configs.fromTry { (c, p) => ConnectionId(c.getString(p))}

  /**
   * A ConfigReader reader that reads [[DataObjectId]] values.
   */
  implicit val dataObjectIdReader: Configs[DataObjectId] = Configs.fromTry { (c, p) => DataObjectId(c.getString(p))}

  /**
   * A ConfigReader reader that reads [[ActionId]] values.
   */
  implicit val actionObjectIdReader: Configs[ActionId] = Configs.fromTry { (c, p) => ActionId(c.getString(p))}

}