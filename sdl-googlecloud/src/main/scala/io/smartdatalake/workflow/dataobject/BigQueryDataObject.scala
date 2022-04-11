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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import org.apache.spark.sql.DataFrame
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.{BigQuery, BigQueryException, BigQueryOptions, DatasetId, TableId}

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
 * @param instanceRegistry InstanceRegistry
 */
case class BigQueryDataObject(override val id: DataObjectId,
                              override val metadata: Option[DataObjectMetadata] = None,
                              override val options: Map[String,String] = Map(),
                              override val schemaMin: Option[GenericSchema] = None,
                              override var table: Table, // db corresponds to dataset
                              project: Option[String] = None,
                              parentProject: Option[String],
                              viewsEnabled: Boolean = false,
                              pushAllFilters: Boolean = true
                             )
                             (@transient implicit val instanceRegistry: InstanceRegistry)
extends TableDataObject with CanCreateSparkDataFrame {

  // as per documentation, you can omit dataset if it's defined in the table name
  assert(table.db.isDefined || table.name.split(".").length==2, "You must either define dataset or include it in the table name (dataset.table)")
  val tblDataset: String = table.db.getOrElse(table.name.split(".")(0))
  val tblName: String = if(table.name.contains(".")) table.name.split(".")(1) else table.name

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

  val instanceOptions: Map[String,String] = Map(
    "viewsEnabled" -> viewsEnabled.toString,
    "pushAllFilters" -> pushAllFilters.toString,
  ) ++
  parentProject.map(pp => ("parentProject"->pp)) ++
  project.map(p => ("project"->p)) ++
  options


  // Adapted from https://cloud.google.com/bigquery/docs/samples/bigquery-dataset-exists
  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    try {
      val dataset = bigQuery.getDataset(DatasetId.of(tblDataset))
      if (dataset != null) true
      else false
    } catch {
      case e: BigQueryException =>
        logger.error(s"Error when checking if dataset exists ${e.getMessage}")
        false
    }
  }

  // Adapted from https://cloud.google.com/bigquery/docs/samples/bigquery-table-exists
  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    try {
      val tbl = bigQuery.getTable(TableId.of(tblDataset, tblName))
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

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    try {
      val success = bigQuery.delete(TableId.of(tblDataset, tblName))
      if (success) logger.info(s"BigQuery table $tblDataset.$tblName dropped successfully.")
      else logger.warn(s"Table to drop was not found: $tblDataset.$tblName")
    } catch {
      case e: BigQueryException =>
        logger.error(s"Table $tblDataset.$tblName was not deleted. ${e.getMessage}")
    }
  }

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {

    val queryOrTable = table.query.getOrElse(tblDataset+"."+tblName)

    val df = context.sparkSession
      .read
      .format("bigquery")
      .options(instanceOptions)
      .load(queryOrTable)

    validateSchemaMin(SparkSchema(df.schema), "read")
    df
  }

  override def factory: FromConfigFactory[DataObject] = BigQueryDataObject

}

object BigQueryDataObject extends FromConfigFactory[DataObject] {

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): BigQueryDataObject = {
    extract[BigQueryDataObject](config)
  }

}