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
import io.smartdatalake.workflow.action.customlogic.CustomDfCreatorConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Exports a util [[DataFrame]] that contains properties and metadata extracted from all [[DataObject]]s
 * that are registered in the current [[InstanceRegistry]].
 *
 * Alternatively, it can export the properties and metadata of all [[DataObject]]s defined in config files. For this, the
 * configuration "config" has to be set to the location of the config.
 *
 * Example:
 * {{{
 * ```dataObjects = {
 *  ...
 *  dataobject-exporter {
 *    type = DataObjectsExporterDataObject
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
case class DataObjectsExporterDataObject(id: DataObjectId,
                                         config: Option[String] = None,
                                         override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with ParsableFromConfig[DataObjectsExporterDataObject] {

  /**
   *
   * @param session SparkSession to use
   * @return DataFrame including all Dataobjects in the instanceRegistry, used for exporting the metadata
   */
  override def getDataFrame(implicit session: SparkSession): DataFrame = {
    import session.implicits._

    // Get all DataObjects from registry
    val dataObjects: Seq[DataObject with Product] = config match {
      case Some(configLocation) =>
        val config = ConfigLoader.loadConfigFromFilesystem(configLocation)
        ConfigParser.parse(config).getDataObjects.map(_.asInstanceOf[DataObject with Product])
      case None => instanceRegistry.getDataObjects.map(_.asInstanceOf[DataObject with Product])
    }

    // Extract data for export
    // type and creator are derived from simple classnames
    val exportObjects = dataObjects.map {
      f =>
        val metadata: Option[DataObjectMetadata] = getFieldData(f, "metadata") match {
          case Some(Some(meta: DataObjectMetadata)) => Some(meta)
          case _ => None
        }
        (
          // id
          getFieldData(f, "id") match {
            case Some(x:String) => x
            case _ => ""
          },
          // type
          f.getClass.getSimpleName,
          // metadata name
          metadata match {
            case Some(x:DataObjectMetadata) => x.name.getOrElse("")
            case _ => ""
          },
          // metadata description
          metadata match {
            case Some(x:DataObjectMetadata) => x.description.getOrElse("")
            case _ => ""
          },
          // metadata layer
          metadata match {
            case Some(x:DataObjectMetadata) => x.layer.getOrElse("")
            case _ => ""
          },
          // metadata subjectArea
          metadata match {
            case Some(x:DataObjectMetadata) => x.subjectArea.getOrElse("")
            case _ => ""
          },
          // path
          getFieldData(f, "path") match {
            case Some(x:String) => x
            case _ => ""
          },
          // partitions
          getFieldData(f, "partitions") match {
            case Some(x: Seq[_]) => x.asInstanceOf[Seq[Any]].mkString(",")
            case _ => ""
          },
          // table
          getFieldData(f, "table") match {
            case Some(tbl: Table) => tbl.toString
            case _ => ""
          },
          // creator
          getFieldData(f, "creator") match {
            case Some(x: CustomDfCreatorConfig) => x.toString
            case _ => ""
          }
        )
    }

    // Return dataframe
    exportObjects.toDF(
      "id",
      "type",
      "name",
      "description",
      "layer",
      "subjectArea",
      "path",
      "partitions",
      "table",
      "creator"
    )
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObjectsExporterDataObject] = DataObjectsExporterDataObject
}

object DataObjectsExporterDataObject extends FromConfigFactory[DataObjectsExporterDataObject] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): DataObjectsExporterDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[DataObjectsExporterDataObject].value
  }
}
