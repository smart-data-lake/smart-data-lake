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
import io.smartdatalake.util.misc.{AclDef, NestedColumnUtil}
import io.smartdatalake.util.spark.SparkRepartitionDef
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.dataobject.generic.{Constraint, HousekeepingMode}
import io.smartdatalake.workflow.dataobject.spark.SparkFileDataObject
import org.apache.spark.sql.DataFrame

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
                               override val constraints: Seq[Constraint] = Seq(),
                               override val expectations: Seq[Expectation] = Seq(),
                               override val metadata: Option[DataObjectMetadata] = None
                             )(@transient implicit override val instanceRegistry: InstanceRegistry)
  extends SparkFileDataObject {

  override val format = "com.databricks.spark.avro"

  // this is only needed for FileRef actions
  override val fileName: String = "*.avro*"

  override val options: Map[String, String] = Map("pathGlobFilter" -> fileName) ++ avroOptions.getOrElse(Map())

  // Avro files implicitly contain a schema.
  // If a schema is defined for the DataObject, it will be applied in customizeContent and not by SparkFileDataObject.getDataFrame directly.
  override val ignoreSchemaForReader: Boolean = true

  /**
   * Convert to target schema if defined
   */
  override def customizeContent(df: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
    schema.map(s => NestedColumnUtil.selectSchema(SparkDataFrame(df), s).asInstanceOf[SparkDataFrame].inner)
      .getOrElse(df)
  }


  override def factory: FromConfigFactory[DataObject] = AvroFileDataObject
}


object AvroFileDataObject extends FromConfigFactory[DataObject] {
  def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AvroFileDataObject = {
    extract[AvroFileDataObject](config)
  }
}