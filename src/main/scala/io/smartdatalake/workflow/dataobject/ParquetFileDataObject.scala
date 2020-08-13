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
import io.smartdatalake.util.hdfs.SparkRepartitionDef
import io.smartdatalake.util.misc.AclDef
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
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
 * @param saveMode spark [[SaveMode]] to use when writing files, default is "overwrite"
 * @param sparkRepartition Optional definition of repartition operation before writing DataFrame with Spark to Hadoop.
 * @param acl override connections permissions for files created with this connection
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.HadoopFileConnection]]
 * @param metadata Metadata describing this data object.
 */
case class ParquetFileDataObject( override val id: DataObjectId,
                                  override val path: String,
                                  override val partitions: Seq[String] = Seq(),
                                  override val schema: Option[StructType] = None,
                                  override val schemaMin: Option[StructType] = None,
                                  override val saveMode: SaveMode = SaveMode.Overwrite,
                                  override val sparkRepartition: Option[SparkRepartitionDef] = None,
                                  override val acl: Option[AclDef] = None,
                                  override val connectionId: Option[ConnectionId] = None,
                                  override val filenameColumn: Option[String] = None,
                                  override val metadata: Option[DataObjectMetadata] = None
                                )(@transient implicit override val instanceRegistry: InstanceRegistry)
  extends SparkFileDataObjectWithEmbeddedSchema with CanCreateDataFrame with CanWriteDataFrame{

  override val format = "parquet"

  // this is only needed for FileRef actions
  override val fileName: String = "*.parquet*"

  // when writing parquet files, schema column names are forced to lower,
  // because they can also be accessed by Hive which is case insensitive.
  // See: https://medium.com/@an_chee/why-using-mixed-case-field-names-in-hive-spark-sql-is-a-bad-idea-95da8b6ec1e0
  override def beforeWrite(df: DataFrame): DataFrame = super.beforeWrite(df).colNamesLowercase

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = ParquetFileDataObject
}

object ParquetFileDataObject extends FromConfigFactory[DataObject] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): ParquetFileDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[ParquetFileDataObject].value
  }
}
