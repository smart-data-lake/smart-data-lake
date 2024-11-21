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

import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config._
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.{ActionPipelineContext, AtlasExportable}
import org.apache.spark.annotation.DeveloperApi

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * This is the root trait for every DataObject.
 */
@DeveloperApi
trait DataObject extends SdlConfigObject with ParsableFromConfig[DataObject] with SmartDataLakeLogger with AtlasExportable {

  /**
   * A unique identifier for this instance.
   */
  override val id: DataObjectId

  /**
   * Additional metadata for the DataObject
   */
  def metadata: Option[DataObjectMetadata]

  /**
   * Configure a housekeeping mode to e.g cleanup, archive and compact partitions.
   *
   * Default is None.
   */
  def housekeepingMode: Option[HousekeepingMode] = None

  /**
   * Prepare & test [[DataObject]]'s prerequisits
   *
   * This runs during the "prepare" operation of the DAG.
   */
  private[smartdatalake] def prepare(implicit context: ActionPipelineContext): Unit = {
    housekeepingMode.foreach(_.prepare(this))
    // check lazy parsed schema
    this match {
      case x: UserDefinedSchema => try {
        x.schema.foreach(_.columns)
      } catch {
        case e: Exception => throw ConfigurationException.fromException(s"($id) error parsing 'schema'", "schema", e)
      }
      case _ => ()
    }
    // check lazy parsed schemaMin (note that it can match schema and schemaMin, and we therefore need two match statements)
    this match {
      case x: SchemaValidation => try{
        x.schemaMin.foreach(_.columns)
      } catch {
        case e: Exception => throw ConfigurationException.fromException(s"($id) error parsing 'schemaMin'", "schemaMin", e)
      }
      case _ => ()
    }
  }

  /**
   * Runs operations before reading from [[DataObject]]
   */
  private[smartdatalake] def preRead(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = ()

  /**
   * Runs operations after reading from [[DataObject]]
   */
  private[smartdatalake] def postRead(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = ()

  /**
   * Runs operations before writing to [[DataObject]]
   * Note: As the transformed SubFeed doesnt yet exist in Action.preWrite, no partition values can be passed as parameters as in preRead
   */
  private[smartdatalake] def preWrite(implicit context: ActionPipelineContext): Unit = ()

  /**
   * Runs operations after writing to [[DataObject]]
   */
  private[smartdatalake] def postWrite(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    housekeepingMode.foreach(_.postWrite(this))
  }

  /**
   * Handle class cast exception when getting objects from instance registry
   */
  protected def getConnection[T <: Connection : TypeTag : ClassTag](connectionId: ConnectionId)(implicit registry: InstanceRegistry): T = {
    try {
      registry.get[T](connectionId)
    } catch {
      case _: NoSuchElementException => throw new NoSuchElementException(s"($id) $connectionId not found in instance registry")
      case TypeMismatchException(_, currentClass, expectedType) =>
        throw ConfigurationException(s"($id) $connectionId of type ${currentClass.getSimpleName} does not implement expected connection type $expectedType")
    }
  }
  protected def getConnectionReg[T <: Connection](connectionId: ConnectionId, registry: InstanceRegistry)(implicit ct: ClassTag[T], tt: TypeTag[T]): T = {
    implicit val registryImpl: InstanceRegistry = registry
    getConnection[T](connectionId)
  }

  /**
   * Returns statistics about this DataObject from the catalog. Depending on it's type this can be (see also [[io.smartdatalake.definitions.TableStatsType]])
   * - sizeInBytes
   * - numFiles
   * - numRows
   * - numPartitions, minPartition, maxPartition
   * - createdAt
   * - lastModifiedAt
   * - lastCommitMsg
   * - location
   * - columns -> column statistics
   * @param update if true, more costly operations such as "analyze table" are executed before returning results.
   * @return a map with statistics about this DataObject
   */
  def getStats(update: Boolean = false)(implicit context: ActionPipelineContext): Map[String,Any] = Map()

  def toStringShort: String = {
    s"$id[${this.getClass.getSimpleName}]"
  }

  override def atlasName: String = id.id
}

/**
 * Additional metadata for a DataObject
 * @param name Readable name of the DataObject
 * @param description Description of the content of the DataObject
 * @param layer Name of the layer this DataObject belongs to
 * @param subjectArea Name of the subject area this DataObject belongs to
 * @param tags Optional custom tags for this object
 */
case class DataObjectMetadata(
                               name: Option[String] = None,
                               description: Option[String] = None,
                               layer: Option[String] = None,
                               subjectArea: Option[String] = None,
                               tags: Seq[String] = Seq()
                             )
