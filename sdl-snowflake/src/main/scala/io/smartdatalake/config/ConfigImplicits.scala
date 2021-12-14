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

import configs.{ConfigError, ConfigKeyNaming, ConfigReader, Result}
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.definitions.{AuthMode, Condition, Environment, ExecutionMode}
import io.smartdatalake.util.hdfs.SparkRepartitionDef
import io.smartdatalake.util.secrets.SecretProviderConfig
import io.smartdatalake.workflow.action.customlogic._
import io.smartdatalake.workflow.action.script.ParsableScriptDef
import io.smartdatalake.workflow.action.sparktransformer.{ParsableDfTransformer, ParsableDfsTransformer}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

trait ConfigImplicits {

  // --------------------------------------------------------------------------------
  // Config reader to circumvent problems related to a bug:
  // The problem is that kxbmap sometimes can not find the correct config reader for
  // some non-trivial nested types, e.g. List[CustomCaseClass] or Option[CustomCaseClass]
  // see: https://github.com/kxbmap/configs/issues/44
  // TODO: check periodically if still needed, should not be needed with scala 2.13+
  // --------------------------------------------------------------------------------
  implicit val customSnowparkDfCreatorConfigReader: ConfigReader[CustomSnowparkDfCreatorConfig] = ConfigReader.derive[CustomSnowparkDfCreatorConfig]
  // --------------------------------------------------------------------------------
}