/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.misc.{SmartDataLakeLogger, StringUtil}
import io.smartdatalake.util.spark.SparkRepartitionDef
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.dataobject.generic.{Constraint, HousekeepingMode}
import io.smartdatalake.workflow.dataobject.spark.SparkFileDataObject
import org.apache.spark.sql.DataFrame

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
 * Column names read from the header are cleaned up on read: blanks, dashes and dots become underscores, all other
 * non-alphanumeric characters are dropped and camel case is converted to lower case with underscores.
 *
 * Example:
 * {{{
 * dataObjects = {
 *   ext-airports {
 *     type = ExcelFileDataObject
 *     path = "~{env.basedir}/ext_airports"
 *     excelOptions {
 *       sheetName = "airports"
 *       maxRowsInMemory = 20000
 *     }
 *   }
 * }
 * }}}
 *
 * @note The Excel data source cannot read a whole directory at once, so files are read one by one.
 *       `numLinesToSkip` and `startColumn` are read-only options and must not be set when writing.
 * @param excelOptions Settings for the underlying [[org.apache.spark.sql.DataFrameReader]] and [[org.apache.spark.sql.DataFrameWriter]].
 */
case class ExcelFileDataObject(override val id: DataObjectId,
                               override val path: String,
                               excelOptions: ExcelOptions = ExcelOptions(),
                               override val partitions: Seq[String] = Seq(),
                               override val schema: Option[GenericSchema] = None,
                               override val schemaMin: Option[GenericSchema] = None,
                               override val saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                               override val sparkRepartition: Option[SparkRepartitionDef] = Some(SparkRepartitionDef(numberOfTasksPerPartition = 1)),
                               override val connectionId: Option[ConnectionId] = None,
                               override val filenameColumn: Option[String] = None,
                               override val expectedPartitionsCondition: Option[String] = None,
                               override val housekeepingMode: Option[HousekeepingMode] = None,
                               override val constraints: Seq[Constraint] = Seq(),
                               override val expectations: Seq[Expectation] = Seq(),
                               override val metadata: Option[DataObjectMetadata] = None
                              )(@transient implicit override val instanceRegistry: InstanceRegistry)
  extends SparkFileDataObject {

  override val format = "dev.mauch.spark.excel"

  override val fileName: String = "*.xls*"

  // spark excel data source does not support reading all files in a directory. Each file must be read one by one.
  override val handleFilesOneByOne: Boolean = true

  override val options: Map[String, String] = Map("pathGlobFilter" -> fileName) ++ excelOptions.toMap(schema).filter {
      case (_, v) => v.isDefined
  }.view.mapValues(_.get.toString).toMap.map(identity) // make serializable

  override def afterRead(df: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
    val dfSuper = super.afterRead(df)

    // cleanup header names
    val newNames = dfSuper.columns.map(name => StringUtil.strCamelCase2LowerCaseWithUnderscores(cleanHeaderName(name)))
    dfSuper.toDF(newNames.toIndexedSeq: _ *)
  }

  /**
   * Checks preconditions before writing.
   */
  override def beforeWrite(df: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
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
}

object ExcelFileDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ExcelFileDataObject = {
    extract[ExcelFileDataObject](config)
  }
}

/**
 * Options passed to [[org.apache.spark.sql.DataFrameReader]] and [[org.apache.spark.sql.DataFrameWriter]] for
 * reading and writing Microsoft Excel files. Excel support is provided by the spark-excel project (see link below).
 *
 * The attributes below are options with a specific meaning in SDLB: `sheetName`, `numLinesToSkip`, `startColumn`,
 * `endColumn` and `rowLimit` are translated into the spark-excel option `dataAddress`, `useHeader` is passed on
 * as `header` and `inferSchema` is disabled if an explicit schema is defined.
 * All other options of spark-excel can be set in `additionalOptions` with their spark-excel name, e.g.
 * {{{
 * excelOptions {
 *   sheetName = "airports"
 *   additionalOptions {
 *     useNullForErrorCells = true
 *     locale = "de-CH"
 *   }
 * }
 * }}}
 *
 * The keys of `additionalOptions` are validated against the options known by spark-excel, so that a misspelled or
 * unsupported option is reported as error. Set `allowUnknownOptions = true` to use an option which is not
 * (yet) known by SDLB, e.g. an option of a newer spark-excel version.
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
 *                  Corresponds to the spark-excel option `header`.
 * @param treatEmptyValuesAsNulls Empty cells are parsed as `null` values (default: true).
 *                                Deprecated: this option is not supported anymore by spark-excel v2 and therefore ignored.
 *                                Use the spark-excel options `useNullForErrorCells` and `nullValue` instead.
 * @param inferSchema Infer the schema of the excel sheet automatically (default: true).
 *                    It is ignored if an explicit `schema` is defined on the DataObject.
 * @param timestampFormat A format string specifying the format to use when reading/writing timestamps (default: dd-MM-yyyy HH:mm:ss).
 * @param dateFormat A format string specifying the format to use when reading/writing dates.
 * @param maxRowsInMemory The number of rows that are stored in memory.
 *                        If set, a streaming reader is used which can help with big files.
 * @param excerptSize Sample size (number of rows) for schema inference.
 * @param additionalOptions Further options passed to the spark-excel data source, using the spark-excel option names,
 *                          e.g. `dataAddress`, `sheetNameIsRegex`, `useNullForErrorCells` or `locale`.
 *                          See the spark-excel documentation for the available options.
 *                          Options which correspond to an attribute above must be set by that attribute.
 *                          `dataAddress` must not be combined with `sheetName`, `numLinesToSkip`, `startColumn`,
 *                          `endColumn` or `rowLimit`.
 * @param allowUnknownOptions If `true`, keys of `additionalOptions` which are not known as spark-excel option only
 *                            create a warning instead of an error (default: false).
 * @see [[https://github.com/nightscape/spark-excel]]
 */
@annotation.nowarn("msg=treatEmptyValuesAsNulls")
case class ExcelOptions(
                         sheetName: Option[String] = None,
                         numLinesToSkip: Option[Int] = None,
                         startColumn: Option[String] = None,
                         endColumn: Option[String] = None,
                         rowLimit: Option[Int] = None,
                         useHeader: Boolean = true,
                         @Deprecated @deprecated("Not supported by spark-excel v2 anymore. Use useNullForErrorCells and nullValue instead", "3.0.0")
                         treatEmptyValuesAsNulls: Option[Boolean] = Some(true),
                         inferSchema: Option[Boolean] = Some(true),
                         timestampFormat: Option[String] = Some("dd-MM-yyyy HH:mm:ss"),
                         dateFormat: Option[String] = None,
                         maxRowsInMemory: Option[Int] = None,
                         excerptSize: Option[Int] = None,
                         additionalOptions: Map[String, String] = Map(),
                         allowUnknownOptions: Boolean = false
                       ) {

  require(!startColumn.exists(_.exists(c => !c.isLetter)), s"ExcelOptions.startColumn must contain only letters (A-Z)+, but is ${startColumn.get}")
  require(!endColumn.exists(_.exists(c => !c.isLetter)), s"ExcelOptions.endColumn must contain only letters (A-Z)+, but is ${endColumn.get}")
  ExcelOptions.validateAdditionalOptions(additionalOptions, allowUnknownOptions, getDerivedDataAddress.isDefined)

  def getDataAddress: Option[String] = additionalOptions.collectFirst { case (k, v) if k.equalsIgnoreCase("dataAddress") => v }
    .orElse(getDerivedDataAddress)

  /**
   * Create the spark-excel `dataAddress` option from sheetName, numLinesToSkip, startColumn, endColumn and rowLimit.
   */
  private def getDerivedDataAddress: Option[String] = {
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

  def toMap(schema: Option[GenericSchema]): Map[String, Option[Any]] = additionalOptions.view.mapValues(Some(_)).toMap ++ Map(
      "dataAddress" -> getDataAddress,
      // treatEmptyValuesAsNulls is not passed on, as it is not supported by spark-excel v2 anymore, see deprecation note.
      "header" -> Some(useHeader),
      "inferSchema" -> Some(schema.isEmpty && inferSchema.getOrElse(true)),
      "timestampFormat" -> timestampFormat,
      "dateFormat" -> dateFormat,
      "maxRowsInMemory" -> maxRowsInMemory,
      "excerptSize" -> excerptSize
    )
}

object ExcelOptions extends SmartDataLakeLogger {

  /**
   * Options read by the spark-excel data source (see `dev.mauch.spark.excel.v2.ExcelOptionsTrait`), which have no
   * corresponding attribute in ExcelOptions.
   * ExcelFileDataObjectTest checks that this list is complete for the spark-excel version SDLB is built with.
   */
  private[dataobject] val sparkExcelOptions: Set[String] = Set(
    "addColorColumns", "columnNameOfCorruptRecord", "columnNameOfRowNumber", "dataAddress", "enforceSchema",
    "fileExtension", "ignoreAfterHeader", "ignoreLeadingWhiteSpace", "ignoreTrailingWhiteSpace", "keepUndefinedRows",
    "locale", "maxByteArraySize", "mode", "nanValue", "negativeInf", "nullValue", "positiveInf", "samplingRatio",
    "sheetNameIsRegex", "tempFileThreshold", "useNullForErrorCells", "usePlainNumberFormat", "workbookPassword"
  )

  /**
   * Generic options of Spark file data sources, which are also supported by spark-excel.
   */
  private[dataobject] val sparkFileSourceOptions: Set[String] = Set(
    "timeZone", "ignoreCorruptFiles", "ignoreMissingFiles", "modifiedBefore", "modifiedAfter"
  )

  /**
   * spark-excel options which are set through an attribute of ExcelOptions or by ExcelFileDataObject itself,
   * with a hint what to do instead.
   */
  private[dataobject] val reservedOptions: Map[String, String] = Seq("header" -> "useHeader", "inferSchema" -> "inferSchema",
    "timestampFormat" -> "timestampFormat", "dateFormat" -> "dateFormat", "maxRowsInMemory" -> "maxRowsInMemory", "excerptSize" -> "excerptSize")
    .map { case (option, attribute) => option -> s"use attribute '$attribute' of ExcelOptions instead" }.toMap +
    ("pathGlobFilter" -> "it is set by ExcelFileDataObject")

  private lazy val knownOptions: Set[String] = sparkExcelOptions ++ sparkFileSourceOptions ++ reservedOptions.keySet

  /**
   * Validate the keys of additionalOptions. Spark options are case-insensitive, therefore keys are compared ignoring case.
   */
  private def validateAdditionalOptions(additionalOptions: Map[String, String], allowUnknownOptions: Boolean, derivedDataAddressDefined: Boolean): Unit = {
    def find(names: Iterable[String], key: String) = names.find(_.equalsIgnoreCase(key))
    additionalOptions.keys.toSeq.sorted.foreach { key =>
      find(reservedOptions.keys, key).foreach { option =>
        throw ConfigurationException(s"(ExcelOptions) option '$key' must not be set in additionalOptions, ${reservedOptions(option)}")
      }
      if (key.equalsIgnoreCase("dataAddress") && derivedDataAddressDefined) {
        throw ConfigurationException("(ExcelOptions) option 'dataAddress' in additionalOptions must not be combined with sheetName, numLinesToSkip, startColumn, endColumn or rowLimit")
      }
      if (find(knownOptions, key).isEmpty) {
        val suggestion = knownOptions.map(o => (o, StringUtil.levenshteinDistance(o.toLowerCase, key.toLowerCase)))
          .filter(_._2 <= 3).toSeq.sortBy(_._2).headOption.map(o => s", did you mean '${o._1}'?").getOrElse("")
        val msg = s"(ExcelOptions) unknown spark-excel option '$key' in additionalOptions$suggestion"
        if (allowUnknownOptions) logger.warn(s"$msg - passed on to spark-excel as allowUnknownOptions=true")
        else throw ConfigurationException(s"$msg. Set allowUnknownOptions=true to pass on options unknown to SDLB, e.g. of a newer spark-excel version.")
      }
    }
  }
}
