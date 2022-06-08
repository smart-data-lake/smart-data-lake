/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import org.apache.spark.sql.DataFrame
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.{BigQuery, BigQueryException, BigQueryOptions, DatasetId, TableId}
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeOptions}
import io.smartdatalake.workflow.connection.BigQueryTableConnection

/**
 * Allows reading a BigQuery table or view from a given Dataset.
 * Note the parameters project and parentProject and whether you need to set them or not.
 * If you don't deploy on GCE / Dataproc, you also need to set credentials, see
 * https://github.com/GoogleCloudDataproc/spark-bigquery-connector#how-do-i-authenticate-outside-gce--dataproc
 * for details. You can set the appropriate options through [[options]].
 *
 * @param id DataObjectId
 * @param metadata Metadata
 * @param options Any additional options, see https://github.com/GoogleCloudDataproc/spark-bigquery-connector#properties
 * @param schemaMin Minimal schema to check
 * @param table db correspondents to dataset, name to table name in bigquery
 * @param project The Google Cloud Project ID of the table or view. Defaults to the project of the Service Account being used.
 * @param parentProject The Google Cloud Project ID of the table to bill for the export. Defaults to the project of the Service Account being used.
 * @param viewsEnabled Enables the connector to read from views and not only tables. Read caveats in official documentation before activating : https://github.com/GoogleCloudDataproc/spark-bigquery-connector#reading-from-views
 * @param pushAllFilters If set to true, the connector pushes all the filters Spark can delegate to BigQuery Storage API. Reduces amount of data transferred.
 * @param temporaryGcsBucket  GCS bucket to use for indirect write
 * @param instanceRegistry InstanceRegistry
 */
case class BigQueryDataObject(override val id: DataObjectId,
                              override val metadata: Option[DataObjectMetadata] = None,
                              override val options: Map[String,String] = Map(),
                              override val schemaMin: Option[GenericSchema] = None,
                              override var table: Table, // db corresponds to dataset
                              connectionId: Option[ConnectionId] = None,
                              project: Option[String] = None,
                              parentProject: Option[String],
                              viewsEnabled: Boolean = false,
                              pushAllFilters: Boolean = true,
                              temporaryGcsBucket: Option[String] = None
                             )
                             (@transient implicit val instanceRegistry: InstanceRegistry)
extends TableDataObject with CanCreateSparkDataFrame with CanWriteSparkDataFrame {

  private val connection = connectionId.map(c => getConnection[BigQueryTableConnection](c))

  table = table.overrideDb(connection.map(_.db))
  if (table.db.isEmpty) {
    throw ConfigurationException(s"($id) Dataset (db) is not defined in table or connection for dataObject.")
  }

  assert(!table.name.contains("."), s"Bigquery table name must not contain a dot. Please use table.db to define dataset name.")

  @transient private var bigQueryOptionsHolder: BigQuery = _
  def bigQuery: BigQuery= {
    if(bigQueryOptionsHolder == null) {
      var bigQueryOptions = BigQueryOptions
        .newBuilder()
      if (project.isDefined) bigQueryOptions = bigQueryOptions.setProjectId(project.get)
      bigQueryOptionsHolder=bigQueryOptions
        .build
        .getService
    }
    bigQueryOptionsHolder
  }

  val generalOptions: Map[String,String] = Map() ++
    parentProject.map(pp => "parentProject"->pp) ++
    project.map(p => "project"->p) ++
    options

  val instanceReadOptions: Map[String,String] = Map(
    "viewsEnabled" -> viewsEnabled.toString,
    "pushAllFilters" -> pushAllFilters.toString,
  ) ++
  generalOptions

  val instanceWriteOptions: Map[String, String] = {
    (if(temporaryGcsBucket.isDefined) Map("temporaryGcsBucket"->temporaryGcsBucket.get) else Map()) ++
    generalOptions
  }


  // Adapted from https://cloud.google.com/bigquery/docs/samples/bigquery-dataset-exists
  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    try {
      val dataset = bigQuery.getDataset(DatasetId.of(table.db.get))
      dataset != null
    } catch {
      case e: BigQueryException =>
        logger.error(s"Error when checking if dataset exists ${e.getMessage}")
        false
    }
  }

  // Adapted from https://cloud.google.com/bigquery/docs/samples/bigquery-table-exists
  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    try {
      val tbl = bigQuery.getTable(TableId.of(table.db.get, table.name))
      if (tbl != null && tbl.exists) {
        true
      }
      else false
    } catch {
      case e: BigQueryException =>
        logger.error(s"Error when checking if table exists ${e.getMessage}")
        false
    }
  }

  @throws[BigQueryException]
  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    val success = bigQuery.delete(TableId.of(table.db.get, table.name))
    if (success) logger.info(s"BigQuery table ${table.db.get}.${table.name} dropped successfully.")
    else logger.warn(s"Table to drop was not found: ${table.db.get}.${table.name}")
  }

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {

    val queryOrTable = table.query.getOrElse(table.db.get+"."+table.name)

    val df = context.sparkSession
      .read
      .format("bigquery")
      .options(instanceReadOptions)
      .load(queryOrTable)

    validateSchemaMin(SparkSchema(df.schema), "read")
    df
  }

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])
                                  (implicit context: ActionPipelineContext): Unit = {

    validateSchemaMin(SparkSchema(df.schema), role = "write")

    assert(saveModeOptions.isEmpty, "SaveMode is currently not supported.")
    assert(temporaryGcsBucket.isDefined, "Only indirect writes are supported. You need to define a temporaryGcsBucket." )

    // TODO: Where do we put this?
    val hc = context.sparkSession.sparkContext.hadoopConfiguration
    hc.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hc.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    df
      .write
      .format("bigquery")
      .options(instanceWriteOptions)
      .save(table.db.get+"."+table.name)
  }

  override def factory: FromConfigFactory[DataObject] = BigQueryDataObject
}

object BigQueryDataObject extends FromConfigFactory[DataObject] {

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): BigQueryDataObject = {
    extract[BigQueryDataObject](config)
  }

}