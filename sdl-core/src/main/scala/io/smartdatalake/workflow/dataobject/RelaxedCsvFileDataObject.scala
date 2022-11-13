package io.smartdatalake.workflow.dataobject

import com.typesafe.config.Config
import com.univocity.parsers.csv.CsvParser
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.DateColumnType.DateColumnType
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{DateColumnType, SDLSaveMode}
import io.smartdatalake.util.hdfs.{PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.misc.{AclDef, SmartDataLakeLogger}
import io.smartdatalake.util.spark.DataFrameUtil._
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.csv.{CSVExprUtils, CSVOptions, UnivocityParser}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.datasources.csv.CSVUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

import java.util.{StringTokenizer, TimeZone}
import scala.reflect.runtime.universe.typeOf

/**
 * A [[DataObject]] which allows for more flexible CSV parsing.
 * The standard CsvFileDataObject doesnt support reading multiple CSV-Files with different column order, missing columns
 * or additional columns.
 * RelaxCsvFileDataObject works more like reading JSON-Files. You need to define a schema, then it tries to read every file
 * with that schema independently of the column order, adding missing columns and removing superfluous ones.
 *
 * CSV files are read by Spark as whole text files and then parsed manually with Sparks CSV parser class. You can therefore use the
 * normal CSV options of spark, but some properties are fixed, e.g. header=true, inferSchema=false, enforceSchema (ignored).
 *
 * @note This data object sets the following default values for `csvOptions`: delimiter = ",", quote = null
 *       All other `csvOption` default to the values defined by Apache Spark.
 * @see [[org.apache.spark.sql.DataFrameReader]]
 * @see [[org.apache.spark.sql.DataFrameWriter]]
 *
 * If mode is permissive you can retrieve the corrupt input record by adding <options.columnNameOfCorruptRecord> as field to the schema.
 * RelaxCsvFileDataObject also supports getting an error msg by adding "<options.columnNameOfCorruptRecord>_msg" as field to the schema.
 *
 * @param schema The data object schema.
 *               Define the schema by using one of the schema providers DDL, jsonSchemaFile, xsdFile or caseClassName.
 *               The schema provider and its configuration value must be provided in the format <PROVIDERID>#<VALUE>.
 *               A DDL-formatted string is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param csvOptions Settings for the underlying [[org.apache.spark.sql.DataFrameReader]] and [[org.apache.spark.sql.DataFrameWriter]].
 * @param dateColumnType Specifies the string format used for writing date typed data.
 * @param treatMissingColumnsAsCorrupt If set to true records from files with missing columns in its header are treated as corrupt (default=false).
 *                                   Corrupt records are handled according to options.mode (default=permissive).
 * @param treatSuperfluousColumnsAsCorrupt If set to true records from files with superfluous columns in its header are treated as corrupt (default=false).
 *                                   Corrupt records are handled according to options.mode (default=permissive).
 * @param sparkRepartition Optional definition of repartition operation before writing DataFrame with Spark to Hadoop.
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param housekeepingMode Optional definition of a housekeeping mode applied after every write. E.g. it can be used to cleanup, archive and compact partitions.
 *                         See HousekeepingMode for available implementations. Default is None.
 **/
case class RelaxedCsvFileDataObject(override val id: DataObjectId,
                                    override val path: String,
                                    csvOptions: Map[String, String] = Map(),
                                    override val partitions: Seq[String] = Seq(),
                                    override val schema: Option[GenericSchema] = None,
                                    override val schemaMin: Option[GenericSchema] = None,
                                    dateColumnType: DateColumnType = DateColumnType.Date,
                                    treatMissingColumnsAsCorrupt: Boolean = false,
                                    treatSuperfluousColumnsAsCorrupt: Boolean = false,
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

  assert(schema.isDefined, "RelaxedCsvFileDataObject needs schema defined")
  private val parserSchema = {
    val colsToRemove = (partitions.map(Some(_)) :+ filenameColumn).flatten
    colsToRemove.foldLeft(schema.get)(
      (schema,colToRemove) => schema.remove(colToRemove)
    )
  }

  // convert schema to spark schema
  private val sparkParserSchema = parserSchema.convert(typeOf[SparkSubFeed]).asInstanceOf[SparkSchema].inner

  override val format = "csv"
  override val readFormat = "text"// read with Spark as text and then parse csv with custom logic
  override val ignoreSchemaForReader = true // schema will be applied in customizeContent and not by SparkFileDataObject

  override val readOptions: Map[String, String] = Map("wholetext" -> "true") // options for Spark text DataSource

  // this is only needed for FileRef actions
  override val fileName: String = "*.csv*"

  private val formatOptionsDefault = Map(
    "delimiter" -> ",",
    "quote" -> null
  )

  private val formatOptionsOverride = Map(
    "header" -> "true", // header must be the first line of every file
    "inferSchema" -> "false", // no schema inference, schema is given for the DataObject
  )

  override val options: Map[String, String] = formatOptionsDefault ++ csvOptions ++ formatOptionsOverride

  // validate parser options
  private val defaultNameOfCorruptRecord = SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)
  private val parserOptions = new CSVOptions(options, columnPruning = true, defaultTimeZoneId = TimeZone.getDefault.getID, defaultColumnNameOfCorruptRecord = defaultNameOfCorruptRecord)
  assert(parserOptions.parseMode == PermissiveMode || parserOptions.parseMode == FailFastMode || parserOptions.parseMode == DropMalformedMode, s"($id) RelaxedCsvFileDataObject doesn't support the ${parserOptions.parseMode.name} mode. Acceptable modes are ${PermissiveMode.name}, ${FailFastMode.name} and ${DropMalformedMode.name}.")
  assert(parserOptions.parseMode == PermissiveMode || !schema.get.columns.contains(parserOptions.columnNameOfCorruptRecord), s"($id) Schema including columnNameOfCorruptRecord '${parserOptions.columnNameOfCorruptRecord}' makes no sense if options.mode != PermissiveMode. Remove ${parserOptions.columnNameOfCorruptRecord} from schema.")
  ExprUtils.verifyColumnNameOfCorruptRecord(sparkParserSchema, parserOptions.columnNameOfCorruptRecord)

  /**
   * parse CSV from DataFrame prepared by Sparks "text" DataSource
   */
  override def customizeContent(df: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession
    // parse csv files content
    assert(df.columns.head == "value") // reading format=text should give schema "value: string" (+ partition columns)
    df
      .flatMap { csvContentRow =>
        if (csvContentRow.isNullAt(0)) Iterator[Row]()
        else parseCsvContent(csvContentRow.getString(0), parserOptions)
          .map { parsedRow =>
            val values = parsedRow.toSeq ++ csvContentRow.toSeq.drop(1) // add partition column values
            Row(values: _*)
          }
      }(RowEncoder.apply(StructType(sparkParserSchema ++ df.schema.drop(1)))) // add partition cols
  }

  private def parseCsvContent(csvContent: String, parserOptions: CSVOptions)(implicit session: SparkSession): Iterator[Row] = {

    // parse header
    val (headerLine, dataIterator) = getHeaderLine(csvContent, parserOptions) match {
      case (Some(headerLine), dataIterator) => (headerLine, dataIterator)
      case _ => throw new SparkException("No header line found")
    }
    val caseSensitive = SQLConf.get.getConf(SQLConf.CASE_SENSITIVE)
    val csvParser = new CsvParser(parserOptions.asParserSettings)
    val header = CSVUtils.makeSafeHeader(csvParser.parseLine(headerLine), caseSensitive, parserOptions)
      .map(_.trim) // remove spaces from all column names and potential CR from last column name
    val headerNormalized = if (caseSensitive) header else header.map(_.toLowerCase)
    val schemaNormalizedMap = sparkParserSchema.map(field => (if (caseSensitive) field.name else field.name.toLowerCase, field)).toMap
    assert(headerNormalized.intersect(schemaNormalizedMap.keys.toSeq).nonEmpty, s"No column names match between header and schema. Please check CSV has header (header line: ${headerLine.trim})")

    // parse to row with file schema
    val fileSchema = StructType(headerNormalized.map(name => schemaNormalizedMap.getOrElse(name, StructField(name, StringType))))
    val parser = new RelaxedParser(fileSchema, sparkParserSchema, parserOptions, treatMissingColumnsAsCorrupt, treatSuperfluousColumnsAsCorrupt)
    dataIterator.flatMap( line => parser.parse(line.trim)) // remove potential CR from last column value
  }

  private def getHeaderLine(csvContent: String, parserOptions: CSVOptions): (Option[String], Iterator[String]) = {
    val tokenizer = new StringTokenizer(csvContent, parserOptions.lineSeparator.getOrElse(System.lineSeparator)) // lineSeparator can be only one character long according to assert statement in CSVOptions
    val lineIterator = new Iterator[String] {
      override def hasNext: Boolean = tokenizer.hasMoreElements
      override def next(): String = tokenizer.nextToken
    }
    (CSVExprUtils.extractHeader(lineIterator, parserOptions), lineIterator)
  }

  override def getStreamingDataFrame(options: Map[String,String], pipelineSchema: Option[StructType])(implicit context: ActionPipelineContext): DataFrame = {
    throw new UnsupportedOperationException(s"($id)getStreamingDataFrame is not yet supported RelaxedCsvFileDataObject")
  }

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

object RelaxedCsvFileDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): RelaxedCsvFileDataObject = {
    extract[RelaxedCsvFileDataObject](config)
  }
}

/**
 * Relaxed parser which reads CSV-lines with fileSchema and returns Spark Rows with tgtSchema
 */
class RelaxedParser( fileSchema:StructType, tgtSchema: StructType, parserOptions: CSVOptions
                   , treatMissingColumnsAsError: Boolean, treatSuperfluousColumnsAsError: Boolean = false) extends SmartDataLakeLogger {

  private val rawParser = new UnivocityParser(fileSchema, fileSchema, parserOptions)
  private val rowConverter = CatalystTypeConverters.createToScalaConverter(fileSchema)
  private val fnRowConverter = (internalRow: InternalRow) => rowConverter(internalRow).asInstanceOf[Row]
  private val columnNameOfCorruptRecordMsg = parserOptions.columnNameOfCorruptRecord + "_msg"
  private val corruptRecordFieldNames = Seq(parserOptions.columnNameOfCorruptRecord, columnNameOfCorruptRecordMsg).toSet
  private val missingFieldNames = tgtSchema.fieldNames.toSet.diff(fileSchema.fieldNames.toSet).diff(corruptRecordFieldNames)
  private val superfluousFieldNames = fileSchema.fieldNames.toSet.diff(tgtSchema.fieldNames.toSet)

  /**
   * Invoke parser and handle errors
   */
  def parse(input: String): Option[Row] = try {
    val parsedInternalRow = rawParser.parse(input)
    if (missingFieldNames.nonEmpty && treatMissingColumnsAsError) throw BadRecordException( () => UTF8String.fromString(input), () => parsedInternalRow, new SparkException(s"Missing field(s) ${missingFieldNames.mkString(", ")} in header"))
    if (superfluousFieldNames.nonEmpty && treatSuperfluousColumnsAsError) throw BadRecordException( () => UTF8String.fromString(input), () => parsedInternalRow, new SparkException(s"Superfluous field(s) ${superfluousFieldNames.mkString(", ")} in header") )
    parsedInternalRow.map(row => createResultRow(Some(fnRowConverter(row)), None, None))
  } catch {
    case e: BadRecordException => parserOptions.parseMode match {
      case PermissiveMode => Some(createResultRow(e.partialResult().map(fnRowConverter), Option(e.record()).map(_.toString), Option(e.cause).map(_.getMessage)))
      case DropMalformedMode => None
      case FailFastMode => throw new SparkException(s"Malformed records are detected in record parsing, failing because of FailFastMode. inputRecord=${input.take(100)}", e)
    }
  }

  /**
   * Combine parsed row and badRecord into result row
   */
  private def createResultRow(fileRow: Option[Row], badRecord: Option[String], errorMsg: Option[String]): Row = {
    val values = tgtSchema.fieldNames.map { name =>
      if (name == parserOptions.columnNameOfCorruptRecord) badRecord.orNull
      else if (name == columnNameOfCorruptRecordMsg) errorMsg.orNull
      else if (missingFieldNames.contains(name)) null
      else fileRow.map(row => row.getAs[Any](name)).orNull
    }
    val resultRow = Row.fromSeq(values)
    if (logger.isTraceEnabled) logger.trace(s"fileRow=$fileRow resultRow=$resultRow")
    resultRow
  }
}