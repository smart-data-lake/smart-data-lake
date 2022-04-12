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
import io.smartdatalake.config._
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.ProductUtil._
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfCreatorConfig
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
  extends DataObject with CanCreateSparkDataFrame with ParsableFromConfig[DataObjectsExporterDataObject] {

  /**
   *
   * @param session SparkSession to use
   * @return DataFrame including all Dataobjects in the instanceRegistry, used for exporting the metadata
   */
  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    val session: SparkSession = context.sparkSession
    import session.implicits._

    val listElementsSeparator = ","

    // Get all DataObjects from registry
    val dataObjects: Seq[DataObject with Product] = config match {
      case Some(configLocation) =>
        val config = ConfigLoader.loadConfigFromFilesystem(configLocation.split(',').toSeq, context.hadoopConf)
        ConfigParser.parse(config).getDataObjects.map(_.asInstanceOf[DataObject with Product])
      case None => instanceRegistry.getDataObjects.map(_.asInstanceOf[DataObject with Product])
    }

    // Extract data for export
    // type and creator are derived from simple classnames
    val exportObjects = dataObjects.map {
      dataObject =>
        val metadata = getOptionalFieldData[DataObjectMetadata](dataObject, "metadata")
        // return tuple:
        (
          // id
          dataObject.id.id,
          // type
          dataObject.getClass.getSimpleName,
          // metadata name
          metadata.flatMap(_.name),
          // metadata description
          metadata.flatMap(_.description),
          // metadata layer
          metadata.flatMap(_.layer),
          // metadata subjectArea
          metadata.flatMap(_.subjectArea),
          // metadata tags
          metadata.map(_.tags).map(_.mkString(listElementsSeparator)),
          // path
          getEventuallyOptionalFieldData[String](dataObject, "path"),
          // partitions
          getFieldData[Seq[String]](dataObject, "partitions").map(_.mkString(listElementsSeparator)),
          // table
          getFieldData[Table](dataObject, "table").map(_.toString),
          // creator
          getFieldData[CustomDfCreatorConfig](dataObject, "creator").map(_.toString),
          // connectionId
          getEventuallyOptionalFieldData[Any](dataObject, "connectionId").map(getIdFromConfigObjectIdOrString)
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
      "tags",
      "path",
      "partitions",
      "table",
      "creator",
      "connectionId"
    )
  }

  override def factory: FromConfigFactory[DataObjectsExporterDataObject] = DataObjectsExporterDataObject
}

object DataObjectsExporterDataObject extends FromConfigFactory[DataObjectsExporterDataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DataObjectsExporterDataObject = {
    extract[DataObjectsExporterDataObject](config)
  }
}
