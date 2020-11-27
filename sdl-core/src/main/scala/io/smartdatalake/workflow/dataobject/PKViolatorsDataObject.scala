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
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * Checks for Primary Key violations for all [[DataObject]]s with Primary Keys defined that are registered in the current [[InstanceRegistry]].
 * Returns the list of Primary Key violations as a [[DataFrame]].
 *
 * Alternatively, it can check for Primary Key violations of all [[DataObject]]s defined in config files. For this, the
 * configuration "config" has to be set to the location of the config.
 *
 * Example:
 * {{{
 * ```dataObjects = {
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

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession, context: ActionPipelineContext): DataFrame = {

    import PKViolatorsDataObject.{colListType, columnNameName, columnValueName}
    // Get all DataObjects from registry
    val dataObjects: Seq[DataObject with Product] = config match {
      case Some(configLocation) =>
        val config = ConfigLoader.loadConfigFromFilesystem(configLocation.split(',').toSeq)
        ConfigParser.parse(config).getDataObjects.map(_.asInstanceOf[DataObject with Product])
      case None => instanceRegistry.getDataObjects.map(_.asInstanceOf[DataObject with Product])
    }

    // filter TableDataObjects with primary key defined
    val dataObjectsWithPk: Seq[TableDataObject] = dataObjects.collect{ case x:TableDataObject if x.table.primaryKey.isDefined => x.asInstanceOf[TableDataObject] }
    logger.info(s"Prepare DataFrame with primary key violations for ${dataObjectsWithPk.map(_.id).mkString(", ")}")

    def colName2colRepresentation(colName: String) = struct(lit(colName).as(columnNameName),col(colName).as(columnValueName))
    def optionalCastColToString(doCast: Boolean)(col: Column) = if (doCast) col.cast(StringType) else col

    def getPKviolatorDf(tobj: TableDataObject) = {
      val pkColNames = tobj.table.primaryKey.get.toArray
      if (tobj.isTableExisting) {
        val dfTable = tobj.getDataFrame()
        val dfPKViolators = dfTable.getPKviolators(pkColNames)
        val scmTable = dfTable.schema
        val dataColumns = dfPKViolators.columns.diff(pkColNames)
        val colSchema: Column = lit(scmTable.toDDL).as("schema")
        val keyCol: Column = optionalCastColToString(flattenOutput) {
          array(pkColNames.map(colName2colRepresentation): _*).cast(colListType(false)).as("key")
        }
        val dataCol: Column = optionalCastColToString(flattenOutput) {
          if (dataColumns.isEmpty) {
            lit(null).cast(colListType(true))
          } else {
            array(dataColumns.map(colName2colRepresentation): _*).cast(colListType(true))
          }.as("data")
        }
        Some( dfPKViolators.select(
          lit(tobj.id.id).as("data_object_id"),
          lit(tobj.table.db.get).as("db"),
          lit(tobj.table.name).as("table"),
          colSchema, keyCol, dataCol.as("data")
        ))
      } else {
        // ignore if table missing, it might be not yet created but only exists in metadata...
        logger.warn(s"(id) Table ${tobj.table.fullName} doesn't exist")
        None
      }
    }
    val pkViolatorsDfs = dataObjectsWithPk.flatMap(getPKviolatorDf)
    if (pkViolatorsDfs.isEmpty) throw NoDataToProcessWarning(id.id, s"($id) No existing table with primary key found")

    // combine & return dataframe
    def unionDf(df1: DataFrame, df2: DataFrame) = df1.union(df2)
    pkViolatorsDfs.reduce(unionDf)
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[PKViolatorsDataObject] = PKViolatorsDataObject
}

object PKViolatorsDataObject extends FromConfigFactory[PKViolatorsDataObject] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): PKViolatorsDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[PKViolatorsDataObject].value
  }

  val columnNameName = "column_name"
  val columnValueName = "column_value"
  def colListType(noColumnsPossible: Boolean): ArrayType = ArrayType(StructType(
    StructField(columnNameName,StringType,nullable = noColumnsPossible)::
      StructField(columnValueName,StringType,nullable = true):: Nil), containsNull = false)

}
