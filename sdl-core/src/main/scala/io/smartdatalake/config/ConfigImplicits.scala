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
import io.smartdatalake.definitions._
import io.smartdatalake.util.hdfs.SparkRepartitionDef
import io.smartdatalake.util.misc.SchemaUtil
import io.smartdatalake.util.secrets.{SecretProviderConfig, StringOrSecret}
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, GenericDfsTransformer}
import io.smartdatalake.workflow.action.script.ParsableScriptDef
import io.smartdatalake.workflow.action.spark.customlogic._
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataobject.{Expectation, HousekeepingMode}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

trait ConfigImplicits {

  /**
   * default naming strategy is to allow lowerCamelCase and hypen-separated key naming, and fail on superfluous keys
   */
  implicit def sdlDefaultNaming[A]: ConfigKeyNaming[A] =
    ConfigKeyNaming.hyphenSeparated[A].or(ConfigKeyNaming.lowerCamelCase[A].apply _)
      .withFailOnSuperfluousKeys

  /**
   * A [[ConfigReader]] reader that reads [[GenericSchema]] values.
   *
   * This reader parses a Spark [[StructType]] by using the desired schema provider.
   * The schema provider is included in the configuration value as prefix terminated by '#'.
   */
  implicit val genericSchemaReader: ConfigReader[GenericSchema] = ConfigReader.fromTry { (c, p) =>
    SchemaUtil.readSchemaFromConfigValue(c.getString(p), Environment.parseSchemaFilesLazy)
  }

  /**
   * A [[ConfigReader]] reader that reads [[OutputMode]].
   */
  implicit val outputModeReader: ConfigReader[OutputMode] = {
    ConfigReader.fromConfig(_.toString.toLowerCase match {
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
  implicit val customDfCreatorConfigReader: ConfigReader[CustomDfCreatorConfig] = ConfigReader.derive[CustomDfCreatorConfig]
  implicit val customDfTransformerConfigReader: ConfigReader[CustomDfTransformerConfig] = ConfigReader.derive[CustomDfTransformerConfig]
  implicit val customDfsTransformerConfigReader: ConfigReader[CustomDfsTransformerConfig] = ConfigReader.derive[CustomDfsTransformerConfig]
  implicit val customFileTransformerConfigReader: ConfigReader[CustomFileTransformerConfig] = ConfigReader.derive[CustomFileTransformerConfig]
  implicit val sparkUdfCreatorConfigReader: ConfigReader[SparkUDFCreatorConfig] = ConfigReader.derive[SparkUDFCreatorConfig]
  implicit val sparkRepartitionDefReader: ConfigReader[SparkRepartitionDef] = ConfigReader.derive[SparkRepartitionDef]
  implicit val secretProviderConfigReader: ConfigReader[SecretProviderConfig] = ConfigReader.derive[SecretProviderConfig]
  implicit val conditionReader: ConfigReader[Condition] = ConfigReader.derive[Condition]
  implicit val authModeReader: ConfigReader[AuthMode] = ConfigReader.derive[AuthMode]
  implicit val saveModeOptionsReader: ConfigReader[SaveModeOptions] = ConfigReader.derive[SaveModeOptions]
  // --------------------------------------------------------------------------------

  implicit def mapDataObjectIdStringReader(implicit mapReader: ConfigReader[Map[String,String]]): ConfigReader[Map[DataObjectId, String]] = {
    ConfigReader.fromConfig { c => mapReader.extract(c).map(_.map{ case (k,v) => (DataObjectId(k), v)})}
  }

  /**
   * A reader that reads [[ConnectionId]] values.
   */
  implicit val connectionIdReader: ConfigReader[ConnectionId] = ConfigReader.fromTry { (c, p) => ConnectionId(c.getString(p))}

  /**
   * A reader that reads [[DataObjectId]] values.
   */
  implicit val dataObjectIdReader: ConfigReader[DataObjectId] = ConfigReader.fromTry { (c, p) => DataObjectId(c.getString(p))}

  /**
   * A reader that reads [[ActionId]] values.
   */
  implicit val actionIdReader: ConfigReader[ActionId] = ConfigReader.fromTry { (c, p) => ActionId(c.getString(p))}

  /**
   * A reader that reads [[GenericDfTransformer]] values.
   * Note that GenericDfTransformer must be parsed according to it's 'type' attribute by using SDL ConfigParser.
   */
  implicit val dfTransformerReader: ConfigReader[GenericDfTransformer] = ConfigReader.fromTry { (c, p) =>
    implicit val instanceRegistry: InstanceRegistry = Environment._instanceRegistry
    ConfigParser.parseConfigObject[GenericDfTransformer](c.getConfig(p))
  }

  /**
   * A reader that reads [[GenericDfsTransformer]] values.
   * Note that GenericDfsTransformer must be parsed according to it's 'type' attribute by using SDL ConfigParser.
   */
  implicit val dfsTransformerReader: ConfigReader[GenericDfsTransformer] = ConfigReader.fromTry { (c, p) =>
    implicit val instanceRegistry: InstanceRegistry = Environment._instanceRegistry
    ConfigParser.parseConfigObject[GenericDfsTransformer](c.getConfig(p))
  }

  /**
   * A reader that reads [[ParsableScriptDef]] values.
   * Note that ParsableScriptDef must be parsed according to it's 'type' attribute by using SDL ConfigParser.
   */
  implicit val scriptDefReader: ConfigReader[ParsableScriptDef] = ConfigReader.fromTry { (c, p) =>
    implicit val instanceRegistry: InstanceRegistry = Environment._instanceRegistry
    ConfigParser.parseConfigObject[ParsableScriptDef](c.getConfig(p))
  }

  /**
   * A reader that reads [[Expectation]] values.
   * Note that Expectation must be parsed according to it's 'type' attribute by using SDL ConfigParser.
   */
  implicit val expectationReader: ConfigReader[Expectation] = ConfigReader.fromTry { (c, p) =>
    implicit val instanceRegistry: InstanceRegistry = Environment._instanceRegistry
    ConfigParser.parseConfigObject[Expectation](c.getConfig(p))
  }

  /**
   * A reader that reads [[Connection]] values inside [[Agent]].
   * Note that Connection must be parsed according to it's 'type' attribute by using SDL ConfigParser.
   */
  implicit val connectionDefReader: ConfigReader[Connection] = ConfigReader.fromTry { (c, p) =>
    implicit val instanceRegistry: InstanceRegistry = Environment._instanceRegistry
    ConfigParser.parseConfigObject[Connection](c.getConfig(p))
  }

  /**
   * A reader that reads [[ExecutionMode]] values.
   * Note that Expectation must be parsed according to it's 'type' attribute by using SDL ConfigParser.
   */
  implicit val executionModeReader: ConfigReader[ExecutionMode] = ConfigReader.fromTry { (c, p) =>
    implicit val instanceRegistry: InstanceRegistry = Environment._instanceRegistry
    ConfigParser.parseConfigObject[ExecutionMode](c.getConfig(p))
  }

  /**
   * A reader that reads [[HousekeepingMode]] values.
   * Note that Expectation must be parsed according to it's 'type' attribute by using SDL ConfigParser.
   */
  implicit val housekeepingModeReader: ConfigReader[HousekeepingMode] = ConfigReader.fromTry { (c, p) =>
    implicit val instanceRegistry: InstanceRegistry = Environment._instanceRegistry
    ConfigParser.parseConfigObject[HousekeepingMode](c.getConfig(p))
  }

  /**
   * A reader that reads [[StringOrSecret]] values.
   */
  implicit val stringOrSecretReader: ConfigReader[StringOrSecret] = ConfigReader.fromTry { (c, p) => StringOrSecret(c.getString(p)) }
}