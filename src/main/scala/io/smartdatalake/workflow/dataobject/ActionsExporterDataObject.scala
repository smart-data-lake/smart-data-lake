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
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.ProductUtil._
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
  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession): DataFrame = {
    import session.implicits._

    val listElementsSeparator = ","

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
      action =>
        val metadata = getOptionalFieldData[ActionMetadata](action, "metadata")
        // return tuple:
        (
          // id
          action.id.id,
          // type
          action.getClass.getSimpleName,
          // metadata name
          metadata.flatMap(_.name),
          // metadata description
          metadata.flatMap(_.description),
          // metadata feed
          metadata.flatMap(_.feed),
          // metadata tags
          metadata.map(_.tags).map(_.mkString(listElementsSeparator)),
          // inputId
          getFieldData[Any](action, "inputId").map(getIdFromConfigObjectIdOrString) // dont know why this is a String and not a DataObjectId. Seems to be a speciality with value classes.
            .orElse( getFieldData[Seq[Any]](action, "inputIds").map(_.map(getIdFromConfigObjectIdOrString).mkString(listElementsSeparator))),
          // outputId
          getFieldData[Any](action, "outputId").map(getIdFromConfigObjectIdOrString) // dont know why this is a String and not a DataObjectId. Seems to be a speciality with value classes.
            .orElse( getFieldData[Seq[Any]](action, "outputIds").map(_.map(getIdFromConfigObjectIdOrString).mkString(listElementsSeparator))),
          // transformer
          getEventuallyOptionalFieldData[Any](action, "transformer").map(_.toString),
          // columnBlacklist
          getOptionalFieldData[Seq[String]](action, "columnBlacklist").map(_.mkString(listElementsSeparator)),
          // columnWhitelist
          getOptionalFieldData[Seq[String]](action, "columnWhitelist").map(_.mkString(listElementsSeparator)),
          // breakDataFrameLineage
          getFieldData[Boolean](action, "breakDataFrameLineage").map(_.toString),
          // persist
          getFieldData[Boolean](action, "persist").map(_.toString)
        )
    }

    // Return dataframe
    exportActions.toDF(
      "id",
      "type",
      "name",
      "description",
      "feed",
      "tags",
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


