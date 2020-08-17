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
import io.smartdatalake.util.hdfs.{PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.misc.AclDef
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType

/**
 * A [[io.smartdatalake.workflow.dataobject.DataObject]] backed by an Avro data source.
 *
 * It manages read and write access and configurations required for [[io.smartdatalake.workflow.action.Action]]s to
 * work on Avro formatted files.
 *
 * Reading and writing details are delegated to Apache Spark [[org.apache.spark.sql.DataFrameReader]]
 * and [[org.apache.spark.sql.DataFrameWriter]] respectively. The reader and writer implementations are provided by
 * the [[https://github.com/databricks/spark-avro databricks spark-avro]] project.
 *
 * @param schema An optional schema for the spark data frame used when writing new Avro files. Note: Existing Avro files
 *               contain a source schema. Therefore, this schema is ignored when reading from existing Avro files.
 * @param sparkRepartition Optional definition of repartition operation before writing DataFrame with Spark to Hadoop.
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 *
 * @see [[org.apache.spark.sql.DataFrameReader]]
 * @see [[org.apache.spark.sql.DataFrameWriter]]
 */
case class AvroFileDataObject( override val id: DataObjectId,
                               override val path: String,
                               override val partitions: Seq[String] = Seq(),
                               override val schema: Option[StructType] = None,
                               override val schemaMin: Option[StructType] = None,
                               override val saveMode: SaveMode = SaveMode.Overwrite,
                               override val sparkRepartition: Option[SparkRepartitionDef] = None,
                               override val acl: Option[AclDef] = None,
                               override val connectionId: Option[ConnectionId] = None,
                               override val filenameColumn: Option[String] = None,
                               override val expectedPartitionsCondition: Option[String] = None,
                               override val metadata: Option[DataObjectMetadata] = None
                             )(@transient implicit override val instanceRegistry: InstanceRegistry)
  extends SparkFileDataObjectWithEmbeddedSchema with CanCreateDataFrame with CanWriteDataFrame {

  override val format = "com.databricks.spark.avro"

  // this is only needed for FileRef actions
  override val fileName: String = "*.avro*"

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = AvroFileDataObject
}


object AvroFileDataObject extends FromConfigFactory[DataObject] {

  /**
   * @inheritdoc
   */
  def fromConfig(config: Config, instanceRegistry: InstanceRegistry): AvroFileDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[AvroFileDataObject].value
  }
}