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
import configs.ConfigReader
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigImplicits, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.{PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.misc.{AclDef, DataFrameUtil}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A [[DataObject]] backed by an Microsoft Excel data source.
 *
 * It manages read and write access and configurations required for [[io.smartdatalake.workflow.action.Action]]s to
 * work on Microsoft Excel (.xslx) formatted files.
 *
 * Reading and writing details are delegated to Apache Spark [[org.apache.spark.sql.DataFrameReader]]
 * and [[org.apache.spark.sql.DataFrameWriter]] respectively. The reader and writer implementation is provided by the
 * [[https://github.com/crealytics/spark-excel Crealytics spark-excel]] project.
 *
 * Read Schema:
 *
 * When `useHeader` is set to true (default), the reader will use the first row of the Excel sheet as column names for
 * the schema and not include the first row as data values. Otherwise the column names are taken from the schema.
 * If the schema is not provided or inferred, then each column name is defined as "_c#" where "#" is the column index.
 *
 * When a data object schema is provided, it is used as the schema for the DataFrame. Otherwise if `inferSchema` is
 * enabled (default), then the data types of the columns are inferred based on the first `excerptSize` rows
 * (excluding the first).
 * When no schema is provided and `inferSchema` is disabled, all columns are assumed to be of string type.
 *
 * @param excelOptions Settings for the underlying [[org.apache.spark.sql.DataFrameReader]] and [[org.apache.spark.sql.DataFrameWriter]].
 * @param schema An optional data object schema. If defined, any automatic schema inference is avoided. As this corresponds to the schema on write, it must not include the optional filenameColumn on read.
 * @param sparkRepartition Optional definition of repartition operation before writing DataFrame with Spark to Hadoop. Default is numberOfTasksPerPartition = 1.
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 */
case class ExcelFileDataObject(override val id: DataObjectId,
                               override val path: String,
                               excelOptions: ExcelOptions = ExcelOptions(),
                               override val partitions: Seq[String] = Seq(),
                               override val schema: Option[StructType] = None,
                               override val schemaMin: Option[StructType] = None,
                               override val saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                               override val sparkRepartition: Option[SparkRepartitionDef] = Some(SparkRepartitionDef(numberOfTasksPerPartition = 1)),
                               override val acl: Option[AclDef] = None,
                               override val connectionId: Option[ConnectionId] = None,
                               override val filenameColumn: Option[String] = None,
                               override val expectedPartitionsCondition: Option[String] = None,
                               override val metadata: Option[DataObjectMetadata] = None
                              )(@transient implicit override val instanceRegistry: InstanceRegistry)
  extends SparkFileDataObject with CanCreateDataFrame with CanWriteDataFrame {

  override val format = "com.crealytics.spark.excel"

  // this is only needed for FileRef actions
  override val fileName: String = "*.xlsx"

  /**
   * @inheritdoc
   */
  override val options: Map[String, String] = excelOptions.toMap(schema).filter {
      case (_, v) => v.isDefined
  }.mapValues(_.get.toString).map(identity) // make serializable

  /**
   * @inheritdoc
   */
  override def afterRead(df: DataFrame)(implicit session: SparkSession): DataFrame = {
    val dfSuper = super.afterRead(df)

    // cleanup header names
    val newNames = dfSuper.columns.map(name => DataFrameUtil.strCamelCase2LowerCaseWithUnderscores(cleanHeaderName(name)))
    dfSuper.toDF(newNames: _ *)
  }

  /**
   * Checks preconditions before writing.
   */
  override def beforeWrite(df: DataFrame)(implicit session: SparkSession): DataFrame = {
    val dfSuper = super.beforeWrite(df)

    // check for unsupported write options
    require(excelOptions.startColumn.isEmpty, s"($id) Writing Excel Files with startColumn defined is not supported.")
    require(excelOptions.numLinesToSkip.isEmpty, s"($id) Writing Excel Files with numLinesToSkip defined is not supported.")

    // return
    dfSuper
  }

  private val validHeaderChars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ Seq('_')

  private val cleanHeaderName = (name: String) => {
    name.map {
      case c if " -.".contains(c) => '_' case c => c
    }.filter(validHeaderChars.contains)
  }

  override def factory: FromConfigFactory[DataObject] = ExcelFileDataObject
}

object ExcelFileDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ExcelFileDataObject = {
    extract[ExcelFileDataObject](config)
  }
}

/**
 * This is a workaround needed with Scala 2.11 because configs doesn't read default values correctly in a scope with many macros.
 * If we let scala process the macro in a smaller scope, default values are handled correctly.
 */
object ExcelOptions extends ConfigImplicits {
  implicit val excelOptionsReader: ConfigReader[ExcelOptions] = ConfigReader.derive[ExcelOptions]
}

/**
 * Options passed to [[org.apache.spark.sql.DataFrameReader]] and [[org.apache.spark.sql.DataFrameWriter]] for
 * reading and writing Microsoft Excel files. Excel support is provided by the spark-excel project (see link below).
 *
 * @param sheetName Optional name of the Excel Sheet to read from/write to.
 * @param numLinesToSkip Optional number of rows in the excel spreadsheet to skip before any data is read.
 *                       This option must not be set for writing.
 * @param startColumn Optional first column in the specified Excel Sheet to read from (as string, e.g B).
 *                    This option must not be set for writing.
 * @param endColumn Optional last column in the specified Excel Sheet to read from (as string, e.g. F).
 * @param rowLimit Optional limit of the number of rows being returned on read.
 *                 This is applied after `numLinesToSkip`.
 * @param useHeader If `true`, the first row of the excel sheet specifies the column names (default: true).
 * @param treatEmptyValuesAsNulls Empty cells are parsed as `null` values (default: true).
 * @param inferSchema Infer the schema of the excel sheet automatically (default: true).
 * @param timestampFormat A format string specifying the format to use when writing timestamps (default: dd-MM-yyyy HH:mm:ss).
 * @param dateFormat A format string specifying the format to use when writing dates.
 * @param maxRowsInMemory The number of rows that are stored in memory.
 *                        If set, a streaming reader is used which can help with big files.
 * @param excerptSize Sample size for schema inference.
 * @see [[https://github.com/crealytics/spark-excel]]
 */
case class ExcelOptions(
                         sheetName: Option[String] = None,
                         numLinesToSkip: Option[Int] = None,
                         startColumn: Option[String] = None,
                         endColumn: Option[String] = None,
                         rowLimit: Option[Int] = None,
                         useHeader: Boolean = true,
                         treatEmptyValuesAsNulls: Option[Boolean] = Some(true),
                         inferSchema: Option[Boolean] = Some(true),
                         timestampFormat: Option[String] = Some("dd-MM-yyyy HH:mm:ss"),
                         dateFormat: Option[String] = None,
                         maxRowsInMemory: Option[Int] = None,
                         excerptSize: Option[Int] = None
                       ) {

  require(!startColumn.exists(_.exists(c => !c.isLetter)), s"ExcelOptions.startColumn must contain only letters (A-Z)+, but is ${startColumn.get}")
  require(!endColumn.exists(_.exists(c => !c.isLetter)), s"ExcelOptions.endColumn must contain only letters (A-Z)+, but is ${endColumn.get}")

  def getDataAddress: Option[String] = {
    if (sheetName.isDefined || startColumn.isDefined || endColumn.isDefined || numLinesToSkip.isDefined || rowLimit.isDefined) {
      val startLine = numLinesToSkip.map(_+1)
      val endLine = rowLimit.map(_+startLine.getOrElse(1))
      val xSheet = sheetName.map(name => s"'${name.trim}'!")
      val startAreaDefined = xSheet.orElse(startColumn).orElse(startLine).orElse(endColumn).orElse(endLine).isDefined
      val xStartArea = if (startAreaDefined) Some(startColumn.getOrElse("A") + startLine.getOrElse(1)) else None
      val endAreaDefined = endColumn.orElse(endLine).isDefined
      val xEndArea = if (endAreaDefined) Some(":" + endColumn.getOrElse("ZZ") + endLine.getOrElse(100000)) else None
      Some( xSheet.getOrElse("") + xStartArea.getOrElse("") + xEndArea.getOrElse(""))
    } else None
  }

  def toMap(schema: Option[StructType]): Map[String, Option[Any]] = Map(
      "dataAddress" -> getDataAddress,
      "treatEmptyValuesAsNulls" -> treatEmptyValuesAsNulls,
      "header" -> Some(useHeader),
      "inferSchema" -> Some(schema.isEmpty && inferSchema.getOrElse(true)),
      "treatEmptyValuesAsNulls" -> treatEmptyValuesAsNulls,
      "timestampFormat" -> timestampFormat,
      "dateFormat" -> dateFormat,
      "maxRowsInMemory" -> maxRowsInMemory,
      "excerptSize" -> excerptSize
    )
}
