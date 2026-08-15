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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{Environment, SDLSaveMode, SaveModeMergeOptions, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.connection.SparkConnectConnection
import io.smartdatalake.workflow.dataframe.sparkconnect.{SparkConnectDataFrame, SparkConnectSchema, SparkConnectSubFeed}
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.dataobject.generic.{CanEvolveSchema, CanHandlePartitions, CanMergeDataFrame, Constraint, ExpectationValidation, Table, TransactionalTableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * [[DataObject]] of type SparkConnectTableDataObject.
 * Provides access to tables of the catalog of a remote Spark Connect server through the normal Spark Table API,
 * e.g. spark.read.table and DataFrame.write.saveAsTable.
 *
 * Note that data is only accessed through the Spark Connect session - there is no Hadoop FileSystem access
 * to the underlying data from the SDLB client.
 *
 * Note that SDLSaveMode.Merge needs a table format supporting row-level operations on the server side, e.g. delta or iceberg.
 *
 * @param id           unique name of this data object
 * @param table        table to be read/written by this data object
 * @param schemaMin    An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 * @param partitions   partition columns for this data object
 * @param constraints  List of row-level [[Constraint]]s to enforce when writing to this data object.
 * @param expectations List of [[Expectation]]s to enforce when writing to this data object. Expectations are checks based on aggregates over all rows of a dataset.
 * @param saveMode     [[SDLSaveMode]] to use when writing the table, default is "Overwrite"
 * @param format       Optional table format used when creating the table, e.g. parquet or delta. Default is the servers default table format.
 * @param allowSchemaEvolution If set to true schema evolution will automatically occur when writing to this DataObject with different schema.
 *                             Note that this needs a table format supporting schema evolution on the server side, e.g. delta.
 * @param options      Options for the Spark DataFrameReader/DataFrameWriter, e.g. format specific options.
 * @param connectionId The SparkConnectConnection to use. If not defined, the default engine connection is used.
 * @param preReadSql SQL-statement to be executed in exec phase before reading input table.
 * @param postReadSql SQL-statement to be executed in exec phase after reading input table and before action is finished.
 * @param preWriteSql SQL-statement to be executed in exec phase before writing output table.
 * @param postWriteSql SQL-statement to be executed in exec phase after writing output table.
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param metadata     meta data
 */
case class SparkConnectTableDataObject(override val id: DataObjectId,
                                       override var table: Table,
                                       override val schemaMin: Option[GenericSchema] = None,
                                       override val partitions: Seq[String] = Seq(),
                                       override val constraints: Seq[Constraint] = Seq(),
                                       override val expectations: Seq[Expectation] = Seq(),
                                       saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                       format: Option[String] = None,
                                       override val allowSchemaEvolution: Boolean = false,
                                       options: Map[String, String] = Map(),
                                       connectionId: ConnectionId,
                                       override val preReadSql: Option[String] = None,
                                       override val postReadSql: Option[String] = None,
                                       override val preWriteSql: Option[String] = None,
                                       override val postWriteSql: Option[String] = None,
                                       override val expectedPartitionsCondition: Option[String] = None,
                                       override val metadata: Option[DataObjectMetadata] = None)
                                      (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalTableDataObject with CanMergeDataFrame with CanHandlePartitions with CanEvolveSchema
    with ExpectationValidation with SmartDataLakeLogger {

  val connection: SparkConnectConnection = getConnection[SparkConnectConnection](connectionId)

  def session(implicit context: ActionPipelineContext): SparkSession = connection.sparkSession

  // check for invalid save modes
  assert(Seq(SDLSaveMode.Overwrite, SDLSaveMode.Append, SDLSaveMode.ErrorIfExists, SDLSaveMode.Ignore, SDLSaveMode.Merge).contains(saveMode), s"($id) Unsupported saveMode $saveMode")

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    if (isTableExisting) validateSchemaHasPrimaryKeyCols(getSparkConnectDataFrame().inner.columns.toIndexedSeq, role = "prepare", obj = "Existing table")
  }

  def getSparkConnectDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): SparkConnectDataFrame = {
    var df = table.query.map(session.sql)
      .getOrElse(session.read.options(options).table(table.fullName))
    if (!context.isExecPhase) df = df.limit(1)
    validateSchemaMin(SparkConnectSchema(df.schema), "read")
    SparkConnectDataFrame(df)
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type = SparkConnectSubFeed.subFeedType)(implicit context: ActionPipelineContext): GenericDataFrame = {
    if (subFeedType =:= typeOf[SparkConnectSubFeed]) getSparkConnectDataFrame(partitionValues)
    else throw new IllegalStateException(s"($id) Unknown subFeedType ${subFeedType.typeSymbol.name}")
  }

  override private[smartdatalake] def getSubFeed(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    if (subFeedType =:= typeOf[SparkConnectSubFeed]) SparkConnectSubFeed(Some(getSparkConnectDataFrame(partitionValues)), id, partitionValues)
    else throw new IllegalStateException(s"($id) Unknown subFeedType ${subFeedType.typeSymbol.name}")
  }

  override private[smartdatalake] def getSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[SparkConnectSubFeed])

  override def init(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    df match {
      case sparkConnectDf: SparkConnectDataFrame => validateSchemaMin(sparkConnectDf.schema, role = "write")
      case _ => throw new IllegalStateException(s"($id) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method init")
    }
  }

  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = {
    df match {
      case sparkConnectDf: SparkConnectDataFrame => writeSparkConnectDataFrame(sparkConnectDf.inner, partitionValues, saveModeOptions)
      case _ => throw new IllegalStateException(s"($id) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method writeDataFrame")
    }
  }

  def writeSparkConnectDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): MetricsMap = {
    // remove columns from DataFrame which are only needed for merge operation, e.g. columns listed in insertColumnsToIgnore
    val targetDf = saveModeOptions.map(_.convertToTargetSchema(SparkConnectDataFrame(df))).getOrElse(SparkConnectDataFrame(df)).inner
    validateSchemaMin(SparkConnectSchema(targetDf.schema), role = "write")
    validateSchemaHasPartitionCols(targetDf.columns.toIndexedSeq, role = "write")
    validateSchemaHasPrimaryKeyCols(targetDf.columns.toIndexedSeq, role = "write")
    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)

    if (isTableExisting) {
      finalSaveMode match {
        case SDLSaveMode.Merge =>
          // merge operations still need all columns for potential insert/updateConditions. Therefore, df instead of targetDf is passed on.
          mergeDataFrameByPrimaryKey(df, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions).getOrElse(SaveModeMergeOptions()))
        case SDLSaveMode.Overwrite if partitions.nonEmpty =>
          if (partitionValues.nonEmpty) {
            // overwrite given partitions: delete partitions data and then append data
            deletePartitions(partitionValues)
            newDfWriter(targetDf).mode(SaveMode.Append).saveAsTable(table.fullName)
          } else {
            // dynamic partition overwrite: overwrite the partitions contained in the DataFrame
            SparkConnectTableUtil.insertIntoDynamicPartitionOverwrite(session, targetDf, table, options, id, this.getClass.getSimpleName)
          }
        case _ =>
          newDfWriter(targetDf)
            .option("overwriteSchema", allowSchemaEvolution) // allow overwriting schema when overwriting whole table (delta)
            .mode(SparkConnectTableDataObject.sparkSaveMode(finalSaveMode)).saveAsTable(table.fullName)
      }
    } else {
      // create new table
      var dfWriter = newDfWriter(targetDf)
      if (partitions.nonEmpty) dfWriter = dfWriter.partitionBy(partitions: _*)
      dfWriter.saveAsTable(table.fullName)
    }
    // Note: there is no QueryExecutionListener to collect metrics on the Spark Connect client side. Metrics are collected through observations if needed.
    Map()
  }

  override private[smartdatalake] def writeSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[SparkConnectSubFeed])

  private def newDfWriter(df: DataFrame): DataFrameWriter[Row] = {
    val dfWriter = df.write.options(options)
      .option("mergeSchema", allowSchemaEvolution) // allow schema evolution for SaveMode.Append (delta)
    format.map(dfWriter.format).getOrElse(dfWriter)
  }

  /**
   * Merges DataFrame with existing table data by using the Spark native merge API.
   *
   * Table.primaryKey is used as condition to check if a record is matched or not. If it is matched it gets updated (or deleted), otherwise it is inserted.
   *
   * This all is done in one transaction.
   * Note that the table format needs to support row-level operations on the server side, e.g. delta or iceberg.
   */
  def mergeDataFrameByPrimaryKey(df: DataFrame, saveModeOptions: SaveModeMergeOptions)(implicit context: ActionPipelineContext): MetricsMap = {
    SparkConnectTableUtil.mergeDataFrameByPrimaryKey(session, df, table, saveModeOptions, allowSchemaEvolution, id)
  }

  /**
   * Listing partitions by a "select distinct partition-columns" query
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    if (partitions.nonEmpty && isTableExisting) SparkConnectTableUtil.listPartitions(session, table, partitions)
    else Seq()
  }

  /**
   * Delete partition data with a SQL delete statement.
   * Note that this needs a table format supporting row-level operations on the server side, e.g. delta or iceberg.
   */
  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    SparkConnectTableUtil.deletePartitions(session, table, partitionValues)
  }

  /**
   * Move partition data with a SQL update statement.
   * Note that this needs a table format supporting row-level operations on the server side, e.g. delta or iceberg.
   */
  override def movePartitions(partitionValues: Seq[(PartitionValues, PartitionValues)])(implicit context: ActionPipelineContext): Unit = {
    SparkConnectTableUtil.movePartitions(session, table, partitionValues, id)
  }

  // cache response to avoid remote catalog query.
  private var cachedIsDbExisting: Option[Boolean] = None
  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    cachedIsDbExisting.getOrElse {
      cachedIsDbExisting = Option(table.db.forall(session.catalog.databaseExists))
      cachedIsDbExisting.get
    }
  }

  // cache if table is existing to avoid remote catalog query.
  private var cachedIsTableExisting: Option[Boolean] = None
  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    cachedIsTableExisting.getOrElse {
      val existing = session.catalog.tableExists(table.fullName)
      if (existing) cachedIsTableExisting = Some(existing) // only cache if existing, otherwise query again later
      existing
    }
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    session.sql(s"DROP TABLE IF EXISTS ${table.fullName}").collect()
    cachedIsTableExisting = None
  }

  override def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    sqlOpt.foreach(sql => session.sql(sql).collect())
  }

  override def factory: FromConfigFactory[DataObject] = SparkConnectTableDataObject
}

object SparkConnectTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SparkConnectTableDataObject = {
    extract[SparkConnectTableDataObject](config)
  }

  private[smartdatalake] def sparkSaveMode(saveMode: SDLSaveMode): SaveMode = saveMode match {
    case SDLSaveMode.Overwrite => SaveMode.Overwrite
    case SDLSaveMode.Append => SaveMode.Append
    case SDLSaveMode.ErrorIfExists => SaveMode.ErrorIfExists
    case SDLSaveMode.Ignore => SaveMode.Ignore
    case _ => throw new IllegalArgumentException(s"Unsupported saveMode $saveMode for SparkConnectTableDataObject")
  }
}
