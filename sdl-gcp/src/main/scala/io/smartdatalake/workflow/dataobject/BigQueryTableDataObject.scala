/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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

import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.{BigQuery, BigQueryFactory, BigQueryOptions, BigQuerySQLException, QueryJobConfiguration, TableId, TableResult}
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.connection.BigQueryTableConnection
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import org.apache.spark.sql.DataFrame

case class BigQueryTableDataObject(override val id: DataObjectId,
                                   override var table: Table,
                                   viewsEnabled: Boolean = true,
                                   materializationDataset: Option[String] = None,
                                   writeMethod: String = "direct",
                                   temporaryGscBucket: Option[String] = None,
                                   persistentGcsBucket: Option[String] = None,
                                   persistentGcsPath: Option[String] = None,
                                   project: Option[String] = None, //defaults to project of the project id of the service account being used.
                                   additionalSparkConnectorOptions: Option[Map[String, String]] = None,
                                   override val schemaMin: Option[GenericSchema] = None,
                                   override val constraints: Seq[Constraint] = Seq(),
                                   override val expectations: Seq[Expectation] = Seq(),
                                   saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                   connectionId: ConnectionId,

                                   override val metadata: Option[DataObjectMetadata] = None)
                                  (@transient implicit val instanceRegistry: InstanceRegistry) extends TransactionalTableDataObject with ExpectationValidation {

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    try {
      require(table.db.isDefined, "The Dataset of the BigQueryTable must be defined explicitly in the field table.db") //table dataset is not defined in connection
      require(Seq("direct", "indirect").contains(writeMethod), f"The write method should be 'direct' or 'indirect', and not the provided value of $writeMethod")
      if (writeMethod == "indirect") require(!Seq(persistentGcsPath, persistentGcsBucket, temporaryGscBucket).flatten.isEmpty, "When using indirect mode, a temporary/persistent bucket or path must be defined")

      require(isDbExisting, f"The provided dataset ${table.db.get} doesn't exist")
      require(isTableExisting, f"The provided table ${table.name} doesn't exist")
      require(viewsEnabled || !hasQuery, "If the table has a 'query' argument, the parameter 'viewsEnabled' cannot be false")
    }
    catch {
      case i: IllegalArgumentException => throw ConfigurationException(i.getMessage)
      case e: Exception => logAndThrowException(e.getMessage, e)
    }
  }

  private val connection = getConnection[BigQueryTableConnection](connectionId)

  private val bigquery = connection.bigQueryObject

  private val bigQueryTable = bigquery.getTable(table.db.get, table.name)

  private val bigQueryDataset = bigquery.getDataset(table.db.get)

  private val hasQuery: Boolean = table.query.isDefined

  private val additionalConnectorOptionsMap = if (additionalSparkConnectorOptions.isDefined) additionalSparkConnectorOptions.get else Map()

  //Using options that are only valid for write operations doesn't have any effect on the readDataFrame method and viceversa --> We can use one map for the entire data object.
  private val sparkOptions: Map[String, String] =
    connection.getConnectionOptions() ++ additionalConnectorOptionsMap ++Map(
    "viewsEnabled" -> viewsEnabled.toString,
    "materializationDataset" -> (if (materializationDataset.isEmpty) table.db.get else materializationDataset.get),
    "writeMethod" -> writeMethod
  ) ++ Map(
      "temporaryGscBucket" -> temporaryGscBucket,
      "persistentGcsBucket" -> persistentGcsBucket,
      "persistentGcsPath" -> persistentGcsPath,
      "project" -> project
  ).collect({case (key, Some(value)) => key -> value})

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = bigQueryDataset != null && bigQueryDataset.exists

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = bigQueryTable != null && bigQueryTable.exists

  override def dropTable(implicit context: ActionPipelineContext): Unit = bigQueryTable.delete()

  override def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    sqlOpt.foreach(stmt =>
      try {
        val queryObject = QueryJobConfiguration.newBuilder(stmt).build()
        val response = bigquery.query(queryObject)
        logger.info(f"The following query was carried out: \n $stmt")
      }
      catch {
        case e: Exception => logger.warn(s"Error in SQL statement '$stmt':\n${e.getMessage}")
      }
    )
  }

  override def factory: FromConfigFactory[DataObject] = ???

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = ???

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = ???
}