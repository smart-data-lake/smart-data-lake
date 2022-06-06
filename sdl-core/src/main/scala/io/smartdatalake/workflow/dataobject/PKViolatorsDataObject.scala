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
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Checks for Primary Key violations for all [[DataObject]]s with Primary Keys defined that are registered in the current [[InstanceRegistry]].
 * Returns the DataFrame of Primary Key violations.
 *
 * Alternatively, it can check for Primary Key violations of all [[DataObject]]s defined in config files. For this, the
 * configuration "config" has to be set to the location of the config.
 *
 * Example:
 * {{{
 * dataObjects = {
 *  ...
 *  primarykey-violations {
 *    type = PKViolatorsDataObject
 *    config = path/to/myconfiguration.conf
 *  }
 *  ...
 * }
 * }}}
 *
 * @param config: The config value can point to a configuration file or a directory containing configuration files.
 * @param flattenOutput: if true, key and data column are converted from type map<k,v> to string (default).
 *
 * @see Refer to [[ConfigLoader.loadConfigFromFilesystem()]] for details about the configuration loading.
 */
case class PKViolatorsDataObject(id: DataObjectId,
                                 config: Option[String] = None,
                                 flattenOutput: Boolean = false,
                                 override val metadata: Option[DataObjectMetadata] = None)
                                (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with ParsableFromConfig[PKViolatorsDataObject] {

  override def getDataFrame(partitionValues: Seq[PartitionValues], subFeedType: Type)(implicit context: ActionPipelineContext): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(subFeedType)
    import PKViolatorsDataObject.{columnNameName, columnValueName}
    import functions._
    // Get all DataObjects from registry
    val dataObjects: Seq[DataObject with Product] = config match {
      case Some(configLocation) =>
        val config = ConfigLoader.loadConfigFromFilesystem(configLocation.split(',').toSeq, context.hadoopConf)
        ConfigParser.parse(config).getDataObjects.map(_.asInstanceOf[DataObject with Product])
      case None => instanceRegistry.getDataObjects.map(_.asInstanceOf[DataObject with Product])
    }

    // filter TableDataObjects with primary key defined
    val dataObjectsWithPk: Seq[TableDataObject] = dataObjects.collect{ case x:TableDataObject if x.table.primaryKey.isDefined => x.asInstanceOf[TableDataObject] }
    logger.info(s"Prepare DataFrame with primary key violations for ${dataObjectsWithPk.map(_.id).mkString(", ")}")

    def colName2colRepresentation(colName: String) = struct(lit(colName).as(columnNameName),col(colName).cast(stringType).as(columnValueName))
    def optionalCastColToString(doCast: Boolean)(col: GenericColumn) = if (doCast) col.cast(functions.stringType) else col

    def getPKviolatorDf(dataObject: TableDataObject) = {
      val pkColNames = dataObject.table.primaryKey.get.toArray
      if (dataObject.isTableExisting) {
        val dfTable = dataObject.getDataFrame(Seq(), subFeedType)
        val dfPKViolators = dfTable.getPKviolators(pkColNames)
        val dataColumns = dfPKViolators.schema.columns.diff(pkColNames)
        val colSchema = lit(dfTable.schema.sql).as("schema")
        val keyCol = optionalCastColToString(flattenOutput) {
          array(pkColNames.map(colName2colRepresentation): _*).as("key")
        }
        val dataCol = optionalCastColToString(flattenOutput) {
          if (dataColumns.isEmpty) {
            lit(null).cast(arrayType(structType(Map(columnNameName -> stringType, columnValueName -> stringType))))
          } else {
            array(dataColumns.map(colName2colRepresentation): _*)
          }
        }.as("data")
        Some( dfPKViolators.select( Seq(
          lit(dataObject.id.id).as("data_object_id"),
          lit(dataObject.table.db.get).as("db"),
          lit(dataObject.table.name).as("table"),
          colSchema, keyCol, dataCol
        )))
      } else {
        // ignore if table missing, it might be not yet created but only exists in metadata...
        logger.warn(s"(id) Table ${dataObject.table.fullName} doesn't exist")
        None
      }
    }
    val pkViolatorsDfs = dataObjectsWithPk.flatMap(getPKviolatorDf)
    if (pkViolatorsDfs.isEmpty) throw NoDataToProcessWarning(id.id, s"($id) No existing table with primary key found")

    // combine & return dataframe
    pkViolatorsDfs.reduce(_ unionByName _)
  }

  override private[smartdatalake] def getSubFeed(partitionValues: Seq[PartitionValues], subFeedType: universe.Type)(implicit context: ActionPipelineContext) = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    val df = getDataFrame(partitionValues, subFeedType)
    helper.getSubFeed(df, id, partitionValues)
  }

  override private[smartdatalake] def getSubFeedSupportedTypes = Seq(typeOf[DataFrameSubFeed])

  override def factory: FromConfigFactory[PKViolatorsDataObject] = PKViolatorsDataObject
}

object PKViolatorsDataObject extends FromConfigFactory[PKViolatorsDataObject] {

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): PKViolatorsDataObject = {
    extract[PKViolatorsDataObject](config)
  }
  private[smartdatalake] val columnNameName = "column_name"
  private[smartdatalake] val columnValueName = "column_value"
}
