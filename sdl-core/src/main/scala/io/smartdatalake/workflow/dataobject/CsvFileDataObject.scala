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
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.definitions.DateColumnType.DateColumnType
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{DateColumnType, SDLSaveMode}
import io.smartdatalake.util.hdfs.{PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.misc.AclDef
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DateType, StringType}

/**
 * A [[DataObject]] backed by a comma-separated value (CSV) data source.
 *
 * It manages read and write access and configurations required for [[io.smartdatalake.workflow.action.Action]]s to
 * work on CSV formatted files.
 *
 * CSV reading and writing details are delegated to Apache Spark [[org.apache.spark.sql.DataFrameReader]]
 * and [[org.apache.spark.sql.DataFrameWriter]] respectively.
 *
 * Read Schema specifications:
 *
 * If a data object schema is not defined via the `schema` attribute (default) and `inferSchema` option is
 * disabled (default) in `csvOptions`, then all column types are set to String and the first row of the CSV file is read
 * to determine the column names and the number of fields.
 *
 * If the `header` option is disabled (default) in `csvOptions`, then the header is defined as "_c#" for each column
 * where "#" is the column index.
 * Otherwise the first row of the CSV file is not included in the DataFrame content and its entries
 * are used as the column names for the schema.
 *
 * If a data object schema is not defined via the `schema` attribute and `inferSchema` is enabled in `csvOptions`, then
 * the `samplingRatio` (default: 1.0) option in `csvOptions` is used to extract a sample from the CSV file in order to
 * determine the input schema automatically.
 *
 * @note This data object sets the following default values for `csvOptions`: delimiter = "|", quote = null, header = false, and inferSchema = false.
 *       All other `csvOption` default to the values defined by Apache Spark.
 *
 * @see [[org.apache.spark.sql.DataFrameReader]]
 * @see [[org.apache.spark.sql.DataFrameWriter]]
 *
 * @param schema An optional data object schema. If defined, any automatic schema inference is avoided.
 *               As this corresponds to the schema on write, it must not include the optional filenameColumn on read.
 *               Define schema by using a DDL-formatted string, which is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param csvOptions Settings for the underlying [[org.apache.spark.sql.DataFrameReader]] and [[org.apache.spark.sql.DataFrameWriter]].
 * @param dateColumnType Specifies the string format used for writing date typed data.
 * @param sparkRepartition Optional definition of repartition operation before writing DataFrame with Spark to Hadoop.
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param housekeepingMode Optional definition of a housekeeping mode applied after every write. E.g. it can be used to cleanup, archive and compact partitions.
 *                         See HousekeepingMode for available implementations. Default is None.
 **/
case class CsvFileDataObject( override val id: DataObjectId,
                              override val path: String,
                              csvOptions: Map[String, String] = Map(),
                              override val partitions: Seq[String] = Seq(),
                              override val schema: Option[GenericSchema] = None,
                              override val schemaMin: Option[GenericSchema] = None,
                              dateColumnType: DateColumnType = DateColumnType.Date,
                              override val saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                              override val sparkRepartition: Option[SparkRepartitionDef] = None,
                              override val acl: Option[AclDef] = None,
                              override val connectionId: Option[ConnectionId] = None,
                              override val filenameColumn: Option[String] = None,
                              override val expectedPartitionsCondition: Option[String] = None,
                              override val housekeepingMode: Option[HousekeepingMode] = None,
                              override val metadata: Option[DataObjectMetadata] = None
                            )(@transient implicit override val instanceRegistry: InstanceRegistry)
  extends SparkFileDataObject {

  override val format = "com.databricks.spark.csv"

  // this is only needed for FileRef actions
  override val fileName: String = "*.csv*"

  private val formatOptionsDefault = Map(
    "header" -> "true",
    "inferSchema" -> "false",
    "delimiter" -> ",",
    "quote" -> null
  )

  /**
   * @inheritdoc
   */
  override val options: Map[String, String] = formatOptionsDefault ++ csvOptions

  require(options("header").toBoolean || options("inferSchema").toBoolean || schema.isDefined, s"($id) Custom schema must be set or either csvOptions { header = true } or csvOptions { inferSchema = true } must be set.")

  /**
   * Formats date type column values according to the specified `dateColumnType` before writing to CSV file.
   */
  override def beforeWrite(df: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
    val dfSuper = super.beforeWrite(df)
    // standardize date column types
    dateColumnType match {
      case DateColumnType.String =>
        dfSuper.castDfColumnTyp(DateType, StringType)
      case DateColumnType.Date => dfSuper.castAllDate2Timestamp
    }
  }

  override def factory: FromConfigFactory[DataObject] = CsvFileDataObject
}

object CsvFileDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CsvFileDataObject = {
    extract[CsvFileDataObject](config)
  }
}
