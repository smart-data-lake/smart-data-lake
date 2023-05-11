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
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.{PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.misc.AclDef
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericSchema
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 *
 * A [[io.smartdatalake.workflow.dataobject.DataObject]] backed by an Apache Hive data source.
 *
 * It manages read and write access and configurations required for [[io.smartdatalake.workflow.action.Action]]s to
 * work on Parquet formatted files.
 *
 * Reading and writing details are delegated to Apache Spark [[org.apache.spark.sql.DataFrameReader]]
 * and [[org.apache.spark.sql.DataFrameWriter]] respectively.
 *
 * @see [[org.apache.spark.sql.DataFrameReader]]
 * @see [[org.apache.spark.sql.DataFrameWriter]]

 * @param id unique name of this data object
 * @param path Hadoop directory where this data object reads/writes it's files.
 *             If it doesn't contain scheme and authority, the connections pathPrefix is applied. If pathPrefix is not
 *             defined or doesn't define scheme and authority, default schema and authority is applied.
 *             Optionally defined partitions are appended with hadoop standard partition layout to this path.
 *             Only files ending with *.parquet* are considered as data for this DataObject.
 * @param partitions partition columns for this data object
 * @param parquetOptions Settings for the underlying [[org.apache.spark.sql.DataFrameReader]] and
 *                       [[org.apache.spark.sql.DataFrameWriter]].
 * @param schema An optional schema for the spark data frame to be validated on read and write. Note: Existing Parquet files
 *               contain a source schema. Therefore, this schema is ignored when reading from existing Parquet files.
 *               As this corresponds to the schema on write, it must not include the optional filenameColumn on read.
 *               Define the schema by using one of the schema providers DDL, jsonSchemaFile, xsdFile or caseClassName.
 *               The schema provider and its configuration value must be provided in the format <PROVIDERID>#<VALUE>.
 *               A DDL-formatted string is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param saveMode spark [[SaveMode]] to use when writing files, default is "overwrite"
 * @param sparkRepartition Optional definition of repartition operation before writing DataFrame with Spark to Hadoop.
 * @param acl override connections permissions for files created with this connection
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.HadoopFileConnection]]
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param housekeepingMode Optional definition of a housekeeping mode applied after every write. E.g. it can be used to cleanup, archive and compact partitions.
 *                         See HousekeepingMode for available implementations. Default is None.
 * @param metadata Metadata describing this data object.
 */
case class ParquetFileDataObject( override val id: DataObjectId,
                                  override val path: String,
                                  override val partitions: Seq[String] = Seq(),
                                  parquetOptions: Option[Map[String,String]] = None,
                                  override val schema: Option[GenericSchema] = None,
                                  override val schemaMin: Option[GenericSchema] = None,
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

  override val format = "parquet"

  // this is only needed for FileRef actions
  override val fileName: String = "*.parquet*"

  override val options: Map[String, String] = parquetOptions.getOrElse(Map())

  override def factory: FromConfigFactory[DataObject] = ParquetFileDataObject
}

object ParquetFileDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ParquetFileDataObject = {
    extract[ParquetFileDataObject](config)
  }
}
