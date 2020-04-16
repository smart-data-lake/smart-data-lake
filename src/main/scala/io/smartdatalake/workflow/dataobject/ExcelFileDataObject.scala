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
import io.smartdatalake.util.hdfs.SparkRepartitionDef
import io.smartdatalake.util.misc.{AclDef, DataFrameUtil}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

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
 * @param schema An optional data object schema. If defined, any automatic schema inference is avoided.
 * @param sparkRepartition Optional definition of repartition operation before writing DataFrame with Spark to Hadoop. Default is numberOfTasksPerPartition = 1.
 */
case class ExcelFileDataObject(override val id: DataObjectId,
                               override val path: String,
                               excelOptions: ExcelOptions,
                               override val partitions: Seq[String] = Seq(),
                               override val schema: Option[StructType] = None,
                               override val schemaMin: Option[StructType] = None,
                               override val saveMode: SaveMode = SaveMode.Overwrite,
                               override val sparkRepartition: Option[SparkRepartitionDef] = Some(SparkRepartitionDef(numberOfTasksPerPartition = 1)),
                               override val acl: Option[AclDef] = None,
                               override val connectionId: Option[ConnectionId] = None,
                               override val metadata: Option[DataObjectMetadata] = None
                              )(@transient implicit override val instanceRegistry: InstanceRegistry)
  extends SparkFileDataObject with CanCreateDataFrame with CanWriteDataFrame {

  excelOptions.validate(id)

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
  override def afterRead(df: DataFrame): DataFrame = {
    val dfSuper = super.afterRead(df)
    val ss = df.sparkSession

    // TODO: instead of startColumn/numLinesToSkip/sheetname, the current version of spark-excel has a 'dataAddress' option.
    //filter the first `numLinesToSkip` rows.
    val filteredDf = if (excelOptions.numLinesToSkip.isEmpty) {
      dfSuper
    } else {
      val rddWithId = dfSuper.rdd.zipWithIndex().map {
        case (row, id) => Row.fromSeq(row.toSeq :+ (id + 1))
      }
      ss.createDataFrame(rddWithId, StructType(dfSuper.schema.fields :+ StructField("id", LongType, nullable = false)))
        .filter(row => row.getAs[Long]("id") > excelOptions.numLinesToSkip.get)
        .drop("id")
    }

    //limit number of returned rows
    val limitedDf = excelOptions.rowLimit.map(limit => filteredDf.limit(limit)).getOrElse(filteredDf)

    // cleanup header names
    val oldNames = limitedDf.schema.map(_.name)
    val newNames = oldNames.map(name => DataFrameUtil.strCamelCase2LowerCaseWithUnderscores(cleanHeaderName(name)))
    limitedDf.toDF(newNames: _ *)
  }

  /**
   * Checks preconditions before writing.
   */
  override def beforeWrite(df: DataFrame): DataFrame = {
    val dfSuper = super.beforeWrite(df)
    // check for unsupported write options
    if (excelOptions.startColumn.exists(_ > 0)) {
      throw new UnsupportedOperationException(s"($id) Writing Excel Files with startColumn defined is not supported.")
    }
    if (excelOptions.numLinesToSkip.exists(_ > 0)) {
      throw new UnsupportedOperationException(s"($id) Writing Excel Files with numLinesToSkip defined is not supported.")
    }
    // return
    dfSuper
  }

  private val validHeaderChars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ Seq('_')

  private val cleanHeaderName = (name: String) => {
    name.map {
      case c if " -.".contains(c) => '_' case c => c
    }.filter(validHeaderChars.contains)
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = ExcelFileDataObject
}

object ExcelFileDataObject extends FromConfigFactory[DataObject] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): ExcelFileDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[ExcelFileDataObject].value
  }
}

/**
 * Options passed to [[org.apache.spark.sql.DataFrameReader]] and [[org.apache.spark.sql.DataFrameWriter]] for
 * reading and writing Microsoft Excel files. Excel support is provided by the spark-excel project (see link below).
 *
 * @param sheetName the name of the Excel Sheet to read from/write to.
 *                  This option is required.
 * @param numLinesToSkip the number of rows in the excel spreadsheet to skip before any data is read.
 *                       This option must not be set for writing.
 * @param startColumn the first column in the specified Excel Sheet to read from (1-based indexing).
 *                    This option must not be set for writing.
 * @param endColumn TODO: this is not used anymore as far as I can tell --> crealytics now uses dataAddress.
 * @param rowLimit Limit the number of rows being returned on read to the first `rowLimit` rows.
 *                 This is applied after `numLinesToSkip`.
 * @param useHeader If `true`, the first row of the excel sheet specifies the column names.
 *                  This option is required (default: true).
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
                         sheetName: String,
                         numLinesToSkip: Option[Int] = None,
                         startColumn: Option[Int] = None,
                         endColumn: Option[Int] = None,
                         rowLimit: Option[Int] = None,
                         useHeader: Boolean = true,
                         treatEmptyValuesAsNulls: Option[Boolean] = Some(true),
                         inferSchema: Option[Boolean] = Some(true),
                         timestampFormat: Option[String] = Some("dd-MM-yyyy HH:mm:ss"),
                         dateFormat: Option[String] = None,
                         maxRowsInMemory: Option[Int] = None,
                         excerptSize: Option[Int] = None
                       ) {

  def validate(id: DataObjectId): Unit = {
    require(sheetName != null && sheetName.trim().nonEmpty, s"($id) The sheetName option is required and must not be empty for Excel files.")
    require(!startColumn.exists(_ <= 0), s"($id) Invalid startColumn index $startColumn. The index of the first column of an excel sheet is 1 and not 0!")
  }

  def toMap(schema: Option[StructType]): Map[String, Option[Any]] = Map(
      "sheetName" -> Some(sheetName.trim()),
      "treatEmptyValuesAsNulls" -> treatEmptyValuesAsNulls,
      "startColumn" -> startColumn.map(_ - 1), // start columns are 0 based in the library, but 1 based in SDL
      "endColumn" -> endColumn,
      "useHeader" -> Some(useHeader),
      "inferSchema" -> Some(schema.isEmpty && inferSchema.getOrElse(true)),
      "treatEmptyValuesAsNulls" -> treatEmptyValuesAsNulls,
      "timestampFormat" -> timestampFormat,
      "dateFormat" -> dateFormat,
      "maxRowsInMemory" -> maxRowsInMemory,
      "excerptSize" -> excerptSize
    )
}
