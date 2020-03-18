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
package io.smartdatalake.workflow.dataobject

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigLoader, ConfigParser, FromConfigFactory, InstanceRegistry, ParsableFromConfig}
import io.smartdatalake.util.misc.ProductUtil.getFieldData
import io.smartdatalake.workflow.action.customlogic.{CustomDfTransformerConfig, CustomDfsTransformerConfig}
import io.smartdatalake.workflow.action.{Action, ActionMetadata}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Exports a util [[DataFrame]] that contains properties and metadata extracted from all [[io.smartdatalake.workflow.action.Action]]s
 * that are registered in the current [[InstanceRegistry]].
 *
 * Alternatively, it can export the properties and metadata of all [[io.smartdatalake.workflow.action.Action]]s defined in config files. For this, the
 * configuration "config" has to be set to the location of the config.
 *
 * Example:
 * {{{
 * dataObjects = {
 *  ...
 *  actions-exporter {
 *    type = ActionsExporterDataObject
 *    config = path/to/myconfiguration.conf
 *  }
 *  ...
 * }
 * }}}
 *
 * The config value can point to a configuration file or a directory containing configuration files.
 *
 * @see Refer to [[ConfigLoader.loadConfigFromFilesystem()]] for details about the configuration loading.
 */
case class ActionsExporterDataObject(id: DataObjectId,
                                     config: Option[String] = None,
                                     override val metadata: Option[DataObjectMetadata] = None)
                               (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with ParsableFromConfig[ActionsExporterDataObject] {

  /**
   *
   * @param session SparkSession to use
   * @return DataFrame including all Actions in the instanceRegistry, used for exporting the metadata
   */
  override def getDataFrame(implicit session: SparkSession): DataFrame = {
    import session.implicits._

    // Get all actions from registry
    val actions: Seq[Action with Product] = config match {
      case Some(configLocation) =>
        val config = ConfigLoader.loadConfigFromFilesystem(configLocation)
        ConfigParser.parse(config).getActions.map(_.asInstanceOf[Action with Product])
      case None => instanceRegistry.getActions.map(_.asInstanceOf[Action with Product])
    }

    // Extract data for export
    // type is derived from classname
    val exportActions = actions.map {
      f =>
        val metadata: Option[ActionMetadata] = getFieldData(f, "metadata") match {
          case Some(Some(meta: ActionMetadata)) => Some(meta)
          case _ => None
        }
        (
          // id
          getFieldData(f, "id") match {
            case Some(x: String) => x
            case _ => ""
          },
          // type
          f.getClass.getSimpleName,
          // metadata name
          metadata match {
            case Some(x:ActionMetadata) => x.name.getOrElse("")
            case _ => ""
          },
          // metadata description
          metadata match {
            case Some(x:ActionMetadata) => x.description.getOrElse("")
            case _ => ""
          },
          // metadata feed
          metadata match {
            case Some(x:ActionMetadata) => x.feed.getOrElse("")
            case _ => ""
          },
          // inputId
          getFieldData(f, "inputId") match {
            case Some(x:String) => x
            case _ =>
              getFieldData(f, "inputIds") match {
                case Some(x:Seq[_]) => x.asInstanceOf[Seq[DataObjectId]].map(_.id).mkString(",")
                case _ => ""
              }
          },
          // outputId
          getFieldData(f, "outputId") match {
            case Some(x:String) => x
            case _ =>
              getFieldData(f, "outputIds") match {
                case Some(x: Seq[_]) => x.asInstanceOf[Seq[DataObjectId]].map(_.id).mkString(",")
                case _ => ""
              }
          },
          // transformer
          getFieldData(f, "transformer") match {
            case Some(x: CustomDfTransformerConfig) => x.toString
            case Some(x: CustomDfsTransformerConfig) => x.toString
            case _ => ""
          },
          // columnBlacklist
          getFieldData(f, "columnBlacklist") match {
            case Some(Some(x:String)) => x.mkString(",")
            case _ => ""
          },
          // columnWhitelist
          getFieldData(f, "columnWhitelist") match {
            case Some(Some(x:String)) => x.mkString(",")
            case _ => ""
          },
          // breakDataFrameLineage
          getFieldData(f, "breakDataFrameLineage") match {
            case Some(x:Boolean) => x.toString
            case _ => ""
          },
          // persist
          getFieldData(f, "persist") match {
            case Some(x:Boolean) => x.toString
            case _ => ""
          }
        )
    }

    // Return dataframe
    exportActions.toDF(
      "id",
      "type",
      "name",
      "description",
      "feed",
      "inputId",
      "outputId",
      "transformer",
      "columnBlacklist",
      "columnWhitelist",
      "breakDataFrameLineage",
      "persist"
    )
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[ActionsExporterDataObject] = ActionsExporterDataObject
}

object ActionsExporterDataObject extends FromConfigFactory[ActionsExporterDataObject] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): ActionsExporterDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[ActionsExporterDataObject].value
  }
}


