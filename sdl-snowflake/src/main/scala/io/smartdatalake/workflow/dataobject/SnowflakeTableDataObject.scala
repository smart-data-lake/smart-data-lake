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
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode._
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.connection.SnowflakeConnection
import io.smartdatalake.workflow.dataframe.snowflake.{SnowparkDataFrame, SnowparkSubFeed}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
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
 * @param saveMode     spark [[SDLSaveMode]] to use when writing files, default is "overwrite"
 * @param connectionId The SnowflakeTableConnection to use for the table
 * @param comment      An optional comment to add to the table after writing a DataFrame to it
 * @param metadata     meta data
 */
// TODO: we should add virtual partitions as for JdbcTableDataObject and KafkaDataObject, so that PartitionDiffMode can be used...
case class SnowflakeTableDataObject(override val id: DataObjectId,
                                    override var table: Table,
                                    override val schemaMin: Option[GenericSchema] = None,
                                    override val constraints: Seq[Constraint] = Seq(),
                                    override val expectations: Seq[Expectation] = Seq(),
                                    saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                    connectionId: ConnectionId,
                                    comment: Option[String] = None,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSparkTableDataObject with ExpectationValidation {

  private val connection = getConnection[SnowflakeConnection](connectionId)
  val fullyQualifiedTableName = connection.database + "." + table.fullName

  def snowparkSession: snowpark.Session = {
    connection.getSnowparkSession(table.db.get)
  }

  // check for invalid save modes
  assert(Seq(SDLSaveMode.Overwrite,SDLSaveMode.Append,SDLSaveMode.ErrorIfExists,SDLSaveMode.Ignore).contains(saveMode), s"($id) Unsupported saveMode $saveMode")

  if (table.db.isEmpty) {
    throw ConfigurationException(s"($id) A SnowFlake schema name must be added as the 'db' parameter of a SnowflakeTableDataObject.")
  }


  // Get a Spark DataFrame with the table contents for Spark transformations
  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): spark.DataFrame = {
    val queryOrTable = Map(table.query.map(q => ("query", q)).getOrElse("dbtable" -> fullyQualifiedTableName))
    val df = context.sparkSession
      .read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connection.getSnowflakeOptions(table.db.get))
      .options(queryOrTable)
      .load()
    df.colNamesLowercase
  }

  // Write a Spark DataFrame to the Snowflake table
  override def writeSparkDataFrame(df: spark.DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])
                                  (implicit context: ActionPipelineContext): Unit = {
    assert(partitionValues.isEmpty, s"($id) SnowflakeTableDataObject can not handle partitions for now")
    validateSchemaMin(SparkSchema(df.schema), role = "write")
    implicit val helper = SparkSubFeed
    var finalSaveMode: SDLSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)

    // TODO: merge mode not yet implemented
    assert(finalSaveMode != SDLSaveMode.Merge, "($id) SaveMode.Merge not implemented for writeSparkDataFrame")

    // Handle overwrite partitions: delete partitions data and then append data
    //if (partitionValues.nonEmpty && finalSaveMode == SDLSaveMode.Overwrite) {
    //  val deleteCondition = PartitionValues.createFilterExpr(partitionValues).exprSql
    //  snowparkSession.sql(s"delete from $fullyQualifiedTableName where $deleteCondition")
    //  finalSaveMode = SDLSaveMode.Append
    //}

    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connection.getSnowflakeOptions(table.db.get))
      .options(Map("dbtable" -> fullyQualifiedTableName))
      .mode(SparkSaveMode.from(finalSaveMode))
      .save()

    if (comment.isDefined) {
      val sql = s"comment on table ${connection.database}.${table.fullName} is '$comment';"
      connection.execSnowflakeStatement(sql)
    }
  }

  override def init(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    df match {
      // TODO: initSparkDataFrame has empty implementation
      case sparkDf: SparkDataFrame => initSparkDataFrame(sparkDf.inner, partitionValues, saveModeOptions)
      case sparkDf: SnowparkDataFrame => Unit
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

  private[smartdatalake] override def writeSubFeed(subFeed: DataFrameSubFeed, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): Unit = {
    writeDataFrame(subFeed.dataFrame.get, partitionValues, isRecursiveInput, saveModeOptions)
  }
  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): Unit = {
    df match {
      case sparkDf: SparkDataFrame => writeSparkDataFrame(sparkDf.inner, partitionValues, isRecursiveInput, saveModeOptions)
      case snowparkDf: SnowparkDataFrame => writeSnowparkDataFrame(snowparkDf.inner, partitionValues, isRecursiveInput, saveModeOptions)
      case _ => throw new IllegalStateException(s"($id) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method writeDataFrame")
    }
  }
  private[smartdatalake] override def writeSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[SnowparkSubFeed], typeOf[SparkSubFeed]) // order matters... if possible Snowpark is preferred to Spark

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    val sql = s"SHOW DATABASES LIKE '${connection.database}'"
    connection.execSnowflakeStatement(sql).next()
  }

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    val sql = s"SHOW TABLES LIKE '${table.name}' IN SCHEMA ${connection.database}.${table.db.get}"
    connection.execSnowflakeStatement(sql).next()
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    val sql = s"DROP TABLE IF EXISTS $fullyQualifiedTableName"
    connection.execSnowflakeStatement(sql).next()
  }

  override def factory: FromConfigFactory[DataObject] = SnowflakeTableDataObject

  /**
   * Read the contents of a table as a Snowpark DataFrame
   */
  def getSnowparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): snowpark.DataFrame = {
    //val helper: DataFrameSubFeedCompanion = SnowparkSubFeed
    this.snowparkSession.table(fullyQualifiedTableName)
  }

  /**
   * Write a Snowpark DataFrame to Snowflake, used in Snowpark actions
   */
  def writeSnowparkDataFrame(df: snowpark.DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                            (implicit context: ActionPipelineContext): Unit = {
    assert(partitionValues.isEmpty, s"($id) SnowflakeTableDataObject can not handle partitions for now")

    // TODO: what to do with isRecursiveInput...
    // TODO: merge mode not yet implemented
    assert(saveMode != SDLSaveMode.Merge, "($id) SaveMode.Merge not implemented for writeSparkDataFrame")

    df.write.mode(SnowparkSaveMode.from(saveMode)).saveAsTable(table.fullName)
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
    case OverwritePreserveDirectories => SaveMode.Append // Append with spark, but delete files before with hadoop
    case OverwriteOptimized => SaveMode.Append // Append with spark, but delete partitions before with hadoop
  }
}