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
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.SparkStageMetricsListener
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.connection.BigQueryTableConnection
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import org.apache.spark.sql.DataFrame

/**
 * [[DataObject]] of type BigQueryTableDataObject.
 * Provides details to access BigQuery Tables to an Action.
 * The object uses the official Spark-connector by Google see: [[https://github.com/GoogleCloudDataproc/spark-bigquery-connector]].
 * As of now, the only authentication mode supported is a Service Account Key.
 * @param id unique name of this data object
 * @param table BigQuery table to by written by this output. BigQuery requires a table name and a dataset.
 * @param viewsEnabled Required if the table is being read with a query, as BigQuery will create a temporary table in the background.
 * @param materializationDataset Required if the table is being read with a query, as BigQuery will create a temporary table in the background.
 *                               This argument defines on which dataset the view will be created. Defaults to the same dataset as the table.
 * @param writeMethod Can be 'direct' or 'indirect'. The indirect mode stores the data in a Bucket or a path first and load it to BigQuery in a second step.
 *                    If 'indirect' mode is used, one of the options 'temporaryGcsBucket', 'persistentGscBucket' or 'persistentGscPath' must be set.
 *                    Defaults to 'direct'
 * @param temporaryGcsBucket Temporary Bucket to store data if saving with "indirect" mode.
 * @param persistentGcsBucket Persistent Bucket to store data if saving with "indirect" mode.
 * @param persistentGcsPath Temporary GSC Path to store data if saving with "indirect" mode.
 * @param project Defaults to the project of the project Id of the service account being used
 * @param options Additional Spark options provided by the connector.
 *                Please see [[https://github.com/GoogleCloudDataproc/spark-bigquery-connector]] for more information.
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 *                  Define schema by using a DDL-formatted string, which is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param constraints List of row-level [[Constraint]]s to enforce when writing to this data object.
 * @param expectations List of [[Expectation]]s to enforce when writing to this data object. Expectations are checks based on aggregates over all rows of a dataset.
 * @param saveMode [[SDLSaveMode]] to use when writing files, default is "append". Overwrite, Append are supported for now.
 * @param connectionId Id of [[io.smartdatalake.workflow.connection.BigQueryTableConnection]]
 * @param metadata Metadata of the Object in form Key-Value form.
 */
case class BigQueryTableDataObject(override val id: DataObjectId,
                                   override var table: Table,
                                   viewsEnabled: Boolean = true,
                                   materializationDataset: Option[String] = None,
                                   writeMethod: String = "direct",
                                   temporaryGcsBucket: Option[String] = None,
                                   persistentGcsBucket: Option[String] = None,
                                   persistentGcsPath: Option[String] = None,
                                   project: Option[String] = None,
                                   override val options: Map[String, String] = Map(),
                                   override val schemaMin: Option[GenericSchema] = None,
                                   override val constraints: Seq[Constraint] = Seq(),
                                   override val expectations: Seq[Expectation] = Seq(),
                                   saveMode: SDLSaveMode = SDLSaveMode.Append,
                                   connectionId: ConnectionId,

                                   override val metadata: Option[DataObjectMetadata] = None)
                                  (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalTableDataObject with CanCreateSparkDataFrame with CanWriteSparkDataFrame with ExpectationValidation {



  private val connection = getConnection[BigQueryTableConnection](connectionId)

  private val bigquery = connection.bigQueryObject

  lazy val bigQueryTable = bigquery.getTable(table.db.get, table.name)

  lazy val bigQueryDataset = bigquery.getDataset(table.db.get)

  private val hasQuery: Boolean = table.query.isDefined

  override val forceGenericObservation: Boolean = writeMethod == "direct" // in direct mode, Spark Observations are not working properly with the BigQuery connector as it uses an optimized write API.

  //Using options that are only valid for write operations doesn't have any effect on the readDataFrame method and viceversa --> We can use one map for the entire data object.
  private def sparkOptions: Map[String, String] =
    connection.getConnectionOptions() ++ options ++ Map(
      "viewsEnabled" -> viewsEnabled.toString,
      "materializationDataset" -> (if (materializationDataset.isEmpty) table.db.get else materializationDataset.get),
      "writeMethod" -> writeMethod
    ) ++ Map(
      "temporaryGcsBucket" -> temporaryGcsBucket,
      "persistentGcsBucket" -> persistentGcsBucket,
      "persistentGcsPath" -> persistentGcsPath,
      "project" -> project
    ).collect({case (key, Some(value)) => key -> value})

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    try {
      require(table.db.isDefined, "The Dataset of the BigQueryTable must be defined explicitly in the field table.db") //table dataset is not defined in connection
      require(Seq("direct", "indirect").contains(writeMethod), f"The write method should be 'direct' or 'indirect', and not the provided value of $writeMethod")
      if (writeMethod == "indirect") require(!Seq(persistentGcsPath, persistentGcsBucket, temporaryGcsBucket).flatten.isEmpty, "When using indirect mode, a temporary/persistent bucket or path must be defined")

      require(isDbExisting, f"The provided dataset ${table.db.get} doesn't exist")
      require(viewsEnabled || !hasQuery, "If the table has a 'query' argument, the parameter 'viewsEnabled' cannot be false")
      require(Seq(SDLSaveMode.Append, SDLSaveMode.Overwrite).contains(saveMode), "As of now, the BigQuery Data Object only supports Save modes Append and Overwrite")
    }
    catch {
      case i: IllegalArgumentException => logAndThrowException(i.getMessage, ConfigurationException(i.getMessage))
      case e: Exception => logAndThrowException(e.getMessage, e)
    }
  }

  override def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): Unit = {
    super.initSparkDataFrame(df, partitionValues, saveModeOptions)
    validateSchemaMin(SparkSchema(df.schema), role = "write")
  }

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = bigQueryDataset != null && bigQueryDataset.exists

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = bigQueryTable != null && bigQueryTable.exists

  override def dropTable(implicit context: ActionPipelineContext): Unit = bigQueryTable.delete()

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {
    require(isTableExisting, f"The provided table ${table.name} doesn't exist")
    val df = context.sparkSession
      .read
      .format("bigquery")
      .options(sparkOptions)
      .load(table.query.getOrElse(table.fullName))

    validateSchemaMin(SparkSchema(df.schema), "read")
    df
  }

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap = {
    SparkStageMetricsListener.execWithMetrics(this.id,
      df.write
        .format("bigquery")
        .options(sparkOptions)
        .mode(SparkSaveMode.from(saveMode))
        .save(table.fullName)
    )
  }

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

  override def factory: FromConfigFactory[DataObject] = BigQueryTableDataObject
}

object BigQueryTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): BigQueryTableDataObject = {
    extract[BigQueryTableDataObject](config)
  }
}