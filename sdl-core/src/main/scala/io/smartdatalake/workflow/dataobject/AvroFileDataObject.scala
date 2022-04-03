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
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.{PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.misc.AclDef

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
 * @param avroOptions Settings for the underlying [[org.apache.spark.sql.DataFrameReader]] and
 *                    [[org.apache.spark.sql.DataFrameWriter]].
 * @param schema An optional schema for the spark data frame to be validated on read and write. Note: Existing Avro files
 *               contain a source schema. Therefore, this schema is ignored when reading from existing Avro files.
 *               As this corresponds to the schema on write, it must not include the optional filenameColumn on read.
 *               Define schema by using a DDL-formatted string, which is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param sparkRepartition Optional definition of repartition operation before writing DataFrame with Spark to Hadoop.
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param housekeepingMode Optional definition of a housekeeping mode applied after every write. E.g. it can be used to cleanup, archive and compact partitions.
 *                         See HousekeepingMode for available implementations. Default is None.
 *
 * @see [[org.apache.spark.sql.DataFrameReader]]
 * @see [[org.apache.spark.sql.DataFrameWriter]]
 */
case class AvroFileDataObject( override val id: DataObjectId,
                               override val path: String,
                               override val partitions: Seq[String] = Seq(),
                               avroOptions: Option[Map[String,String]] = None,
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

  override val format = "com.databricks.spark.avro"

  // this is only needed for FileRef actions
  override val fileName: String = "*.avro*"

  override val options: Map[String, String] = avroOptions.getOrElse(Map())

  override def factory: FromConfigFactory[DataObject] = AvroFileDataObject
}


object AvroFileDataObject extends FromConfigFactory[DataObject] {
  def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AvroFileDataObject = {
    extract[AvroFileDataObject](config)
  }
}