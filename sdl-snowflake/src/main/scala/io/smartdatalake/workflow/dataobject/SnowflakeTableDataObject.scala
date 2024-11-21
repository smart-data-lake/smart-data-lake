/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

import com.snowflake.snowpark
import com.snowflake.snowpark.SaveMode
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode._
import io.smartdatalake.definitions.{Environment, SDLSaveMode, SaveModeOptions}
import io.smartdatalake.metrics.SparkStageMetricsListener
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{SQLUtil, SchemaUtil}
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.generic.transformer.GenericDfTransformer
import io.smartdatalake.workflow.connection.SnowflakeConnection
import io.smartdatalake.workflow.dataframe.snowflake.{SnowparkDataFrame, SnowparkSchema, SnowparkSubFeed}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import net.snowflake.spark.snowflake.Utils
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.{sql => spark}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * [[DataObject]] of type SnowflakeTableDataObject.
 * Provides details to access Snowflake tables via an action
 * Can be used both for interacting with Snowflake through Spark with JDBC,
 * as well as for actions written in the Snowpark API that run directly on Snowflake
 *
 * @param id           unique name of this data object
 * @param table        Snowflake table to be written by this output
 * @param constraints  List of row-level [[Constraint]]s to enforce when writing to this data object.
 * @param expectations List of [[Expectation]]s to enforce when writing to this data object. Expectations are checks based on aggregates over all rows of a dataset.
 * @param preReadSql SQL-statement to be executed in exec phase before reading input table. It uses the SnowflakeConnection for the target database.
 * @param postReadSql SQL-statement to be executed in exec phase after reading input table and before action is finished. It uses the SnowflakeConnection for the target database.
 * @param preWriteSql SQL-statement to be executed in exec phase before writing output table. It uses the SnowflakeConnection for the target database.
 * @param postWriteSql SQL-statement to be executed in exec phase after writing output table. It uses the SnowflakeConnection for the target database.
 * @param saveMode     spark [[SDLSaveMode]] to use when writing files, default is "overwrite"
 * @param connectionId The SnowflakeTableConnection to use for the table
 * @param virtualPartitions Virtual partition columns. Note that Snowflake has no partition concept, and SDLB is emulating partitions on its own.
 * @param readTransformer   An optional transformer that is applied on read. This is often used to adapt Snowflake decimal datatype to more accurate IntegralTypes like Long, Integer, Byte.
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param sparkOptions Options for the Snowflake Spark Connector, see https://docs.snowflake.com/en/user-guide/spark-connector-use#additional-options.
 *                     These options override connection.options.
 * @param metadata     meta data
 */
case class SnowflakeTableDataObject(override val id: DataObjectId,
                                    override var table: Table,
                                    override val schemaMin: Option[GenericSchema] = None,
                                    override val constraints: Seq[Constraint] = Seq(),
                                    override val expectations: Seq[Expectation] = Seq(),
                                    override val preReadSql: Option[String] = None,
                                    override val postReadSql: Option[String] = None,
                                    override val preWriteSql: Option[String] = None,
                                    override val postWriteSql: Option[String] = None,
                                    saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                    connectionId: ConnectionId,
                                    sparkOptions: Map[String, String] = Map(),
                                    virtualPartitions: Seq[String] = Seq(),
                                    readTransformer: Option[GenericDfTransformer] = None,
                                    override val expectedPartitionsCondition: Option[String] = None,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalTableDataObject with CanHandlePartitions with ExpectationValidation {

  private val connection = getConnection[SnowflakeConnection](connectionId)

  // Define partition columns
  override val partitions: Seq[String] = if (Environment.caseSensitive) virtualPartitions else virtualPartitions.map(_.toLowerCase)

  def snowparkSession: snowpark.Session = {
    connection.getSnowparkSession(table.db.get)
  }

  // check for invalid save modes
  assert(Seq(SDLSaveMode.Overwrite,SDLSaveMode.Append,SDLSaveMode.ErrorIfExists,SDLSaveMode.Ignore).contains(saveMode), s"($id) Unsupported saveMode $saveMode")

  // prepare final table
  table = table.overrideCatalogAndDb(Some(connection.database), None)
  if(table.catalog.isEmpty) throw ConfigurationException(s"($id) A Snowflake database name must be added as the 'table.catalog' parameter of SnowflakeTableDataObject or 'connection.database' of SnowflakeConnection.")
  if (table.db.isEmpty) throw ConfigurationException(s"($id) A Snowflake schema name must be added as the 'table.db' parameter of a SnowflakeTableDataObject.")

  // Note: Spark snowflake data source does not execute Spark observations. This is the same for Spark jdbc data source, see also JdbcTableDataObject.
  // Using generic observations is forced therefore.
  override val forceGenericObservation = true

  private val instanceSparkOptions = connection.sparkOptions ++ sparkOptions

  // Get a Spark DataFrame with the table contents for Spark transformations
  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): spark.DataFrame = {
    val queryOrTable = Map(table.query.map(q => ("query", q)).getOrElse("dbtable" -> table.fullName))
    val df = context.sparkSession
      .read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connection.getJdbcAuthOptions(table.db.get))
      .options(instanceSparkOptions)
      .options(queryOrTable)
      .load()
    applyReadTransformer(partitionValues, SparkDataFrame(df))
      .asInstanceOf[SparkDataFrame].inner
  }

  // Write a Spark DataFrame to the Snowflake table
  override def writeSparkDataFrame(df: spark.DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])
                                  (implicit context: ActionPipelineContext): MetricsMap = {
    assert(partitionValues.isEmpty, s"($id) SnowflakeTableDataObject can not handle partitions for now")
    validateSchemaMin(SparkSchema(df.schema), role = "write")
    var finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)

    // TODO: merge mode not yet implemented
    assert(finalSaveMode != SDLSaveMode.Merge, "($id) SaveMode.Merge not implemented for writeSparkDataFrame")

    // Handle overwrite partitions: delete partitions data and then append data
    if (partitionValues.nonEmpty && finalSaveMode == SDLSaveMode.Overwrite) {
      deletePartitions(partitionValues)
      finalSaveMode = SDLSaveMode.Append
    }

    val metrics = SparkStageMetricsListener.execWithMetrics(this.id,
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connection.getJdbcAuthOptions(table.db.get))
        .options(instanceSparkOptions)
        .options(Map("dbtable" -> table.fullName))
        .mode(SparkSaveMode.from(finalSaveMode))
        .save()
    )

    metadata.flatMap(_.description).foreach { comment =>
      val sql = s"comment on table ${table.fullName} is '$comment';"
      connection.execJdbcStatement(sql)
    }

    // return
    metrics
  }

  override def init(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    df match {
      // TODO: initSparkDataFrame has empty implementation
      case sparkDf: SparkDataFrame => initSparkDataFrame(sparkDf.inner, partitionValues, saveModeOptions)
      case sparkDf: SnowparkDataFrame => ()
      case _ => throw new IllegalStateException(s"($id) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method init")
    }
  }

  override private[smartdatalake] def getSubFeed(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    if (subFeedType =:= typeOf[SparkSubFeed]) SparkSubFeed(Some(SparkDataFrame(getSparkDataFrame(partitionValues))), id, partitionValues)
    else if (subFeedType =:= typeOf[SnowparkSubFeed]) SnowparkSubFeed(Some(SnowparkDataFrame(getSnowparkDataFrame(partitionValues))), id, partitionValues)
    else throw new IllegalStateException(s"($id) Unknown subFeedType ${subFeedType.typeSymbol.name}")
  }
  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext) : GenericDataFrame = {
    if (subFeedType =:= typeOf[SparkSubFeed]) SparkDataFrame(getSparkDataFrame(partitionValues))
    else if (subFeedType =:= typeOf[SnowparkSubFeed]) SnowparkDataFrame(getSnowparkDataFrame(partitionValues))
    else throw new IllegalStateException(s"($id) Unknown subFeedType ${subFeedType.typeSymbol.name}")
  }
  private[smartdatalake] override def getSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[SnowparkSubFeed], typeOf[SparkSubFeed]) // order matters... if possible Snowpark is preferred to Spark

  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = {
    df match {
      case sparkDf: SparkDataFrame => writeSparkDataFrame(sparkDf.inner, partitionValues, isRecursiveInput, saveModeOptions)
      case snowparkDf: SnowparkDataFrame => writeSnowparkDataFrame(snowparkDf.inner, partitionValues, isRecursiveInput, saveModeOptions)
      case _ => throw new IllegalStateException(s"($id) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method writeDataFrame")
    }
  }
  private[smartdatalake] override def writeSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[SnowparkSubFeed], typeOf[SparkSubFeed]) // order matters... if possible Snowpark is preferred to Spark


  // cache response to avoid jdbc query.
  private var cachedIsDbExisting: Option[Boolean] = None
  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    cachedIsDbExisting.getOrElse {
      cachedIsDbExisting = Option(connection.catalog.isDbExisting(table.db.get))
      cachedIsDbExisting.get
    }
  }
  // cache if table is existing to avoid jdbc query.
  private var cachedIsTableExisting: Option[Boolean] = None
  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    cachedIsTableExisting.getOrElse {
      val existing = connection.catalog.isTableExisting(table.fullName)
      if (existing) cachedIsTableExisting = Some(existing) // only cache if existing, otherwise query again later
      existing
    }
  }
  // cache response to avoid jdbc query.
  private var cachedExistingSchema: Option[GenericSchema] = None
  private def getExistingSchema(implicit context: ActionPipelineContext): Option[GenericSchema] = {
    if (isTableExisting && cachedExistingSchema.isEmpty) {
      cachedExistingSchema = Some(SnowparkSchema(getSnowparkDataFrame().schema))
      // convert to lowercase when Spark is in non-casesensitive mode
      if (!Environment.caseSensitive) cachedExistingSchema = Some(SchemaUtil.prepareSchemaForDiff(cachedExistingSchema.get, ignoreNullable = false, caseSensitive = false))
    }
    cachedExistingSchema
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    connection.execJdbcStatement(s"drop table if exists ${table.fullName}")
  }

  override def factory: FromConfigFactory[DataObject] = SnowflakeTableDataObject

  private def applyReadTransformer(partitionValues: Seq[PartitionValues], df: GenericDataFrame)(implicit context: ActionPipelineContext): GenericDataFrame = {
    readTransformer.map { t =>
      t.transform(context.currentAction.map(_.id).getOrElse(ActionId("undefined")), partitionValues, df, this.id, previousTransformerName = None, executionModeResultOptions = Map())
    }.getOrElse(df)
  }

  /**
   * Read the contents of a table as a Snowpark DataFrame
   */
  def getSnowparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): snowpark.DataFrame = {
    //val helper: DataFrameSubFeedCompanion = SnowparkSubFeed
    val df = SnowparkDataFrame(snowparkSession.table(table.fullName))
    applyReadTransformer(partitionValues, df)
      .asInstanceOf[SnowparkDataFrame].inner
  }

  /**
   * Write a Snowpark DataFrame to Snowflake, used in Snowpark actions
   */
  def writeSnowparkDataFrame(df: snowpark.DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                            (implicit context: ActionPipelineContext): MetricsMap = {
    var finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)

    // TODO: merge mode not yet implemented
    assert(finalSaveMode != SDLSaveMode.Merge, "($id) SaveMode.Merge not implemented for writeSparkDataFrame")

    // Handle overwrite partitions: delete partitions data and then append data
    if (partitionValues.nonEmpty && finalSaveMode == SDLSaveMode.Overwrite && isTableExisting) {
      deletePartitions(partitionValues)
      finalSaveMode = SDLSaveMode.Append
    }

    // use asynchronous writer to get query id
    val asyncWriter = df.write.mode(SnowparkSaveMode.from(finalSaveMode)).async.saveAsTable(table.fullName)
    asyncWriter.getResult()

    // retrieve metrics from result scan
    val dfResultScan = snowparkSession.sql(s"SELECT * FROM TABLE(RESULT_SCAN('${asyncWriter.getQueryId()}'))")
    dfResultScan.first().map(row => dfResultScan.schema.names.map(_.toLowerCase.replace(" ","_").replace("\"","")).zip(row.toSeq).toMap).getOrElse(Map())
      // standardize naming
      .map {
        case ("number_of_rows_inserted", v) => "rows_inserted" -> v
        case ("number_of_rows_updated", v) => "rows_updated" -> v
        case ("number_of_rows_deleted", v) => "rows_deleted" -> v
        case (k, v) => k -> v
      }
  }

  override def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    if (sqlOpt.nonEmpty) connection.execJdbcStatement(sqlOpt.get)
  }

  /**
   * Listing virtual partitions by a "select distinct partition-columns" query
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    if (partitions.nonEmpty) {
      if (isTableExisting) PartitionValues.fromDataFrame(SnowparkDataFrame(getSnowparkDataFrame().select(partitions.map(snowpark.functions.col)).distinct()))
      else Seq()
    } else Seq()
  }

  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    if (partitionValues.nonEmpty) {
      connection.execJdbcStatement(deletePartitionsStatement(partitionValues))
    }
  }

  /**
   * Delete virtual partitions by "delete from" statement
   * @param partitionValues nonempty list of partition values
   */
  private def deletePartitionsStatement(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): String = {
    SQLUtil.createDeletePartitionStatement(table.fullName, partitionValues, quoteCaseSensitiveColumn(_))
  }

  /**
   * if we generate sql statements with column names we need to care about quoting them properly
   */
  private def quoteCaseSensitiveColumn(column: String)(implicit context: ActionPipelineContext): String = {
    if (Environment.caseSensitive) Utils.quotedName(column)
    // quote identifier if it contains special characters
    else if (SQLUtil.hasIdentifierSpecialChars(column)) Utils.quotedName(column)
    else column
  }
}

object SnowflakeTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)
                         (implicit instanceRegistry: InstanceRegistry): SnowflakeTableDataObject = {
    extract[SnowflakeTableDataObject](config)
  }
}

/**
 * Mapping to Spark SaveMode
 * This is one-to-one except custom modes as OverwritePreserveDirectories
 */
object SnowparkSaveMode {
  def from(mode: SDLSaveMode): SaveMode = mode match {
    case Overwrite => SaveMode.Overwrite
    case Append => SaveMode.Append
    case ErrorIfExists => SaveMode.ErrorIfExists
    case Ignore => SaveMode.Ignore
    case OverwritePreserveDirectories => throw new NotImplementedError("SaveMode OverwritePreserveDirectories is not implemented for SnowflakeDataObject")
    case OverwriteOptimized => throw new NotImplementedError("SaveMode OverwriteOptimized is not implemented for SnowflakeDataObject")
  }
}