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
package io.smartdatalake.workflow.connection

import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{ParsableFromConfig, SdlConfigObject}
import io.smartdatalake.workflow.{ActionPipelineContext, AtlasExportable}

import scala.reflect.runtime.universe.Type

trait Connection extends SdlConfigObject with ParsableFromConfig[Connection] with AtlasExportable {

  /**
   * A unique identifier for this instance.
   */
  override val id: ConnectionId

  /**
   * Additional metadata for the Connection
   */
  def metadata: Option[ConnectionMetadata]

  def toStringShort: String = {
    s"$id[${this.getClass.getSimpleName}]"
  }

  override def atlasName: String = id.id
}

/**
 * Additional metadata for a Connection
 * @param name Readable name of the Connection
 * @param description Description of the content of the Connection
 * @param layer Name of the layer this Connection belongs to
 * @param subjectArea Name of the subject area this Connection belongs to
 * @param tags Optional custom tags for this object
 */
case class ConnectionMetadata(
                               name: Option[String] = None,
                               description: Option[String] = None,
                               layer: Option[String] = None,
                               subjectArea: Option[String] = None,
                               tags: Seq[String] = Seq()
                             )

/**
 * A trait for connections that can be used as execution engine.
 */
trait EngineConnection {
  /**
   * The type of the sub-feed that this engine can execute.
   */
  def subFeedType: Type

  /**
   * A hook for subclasses to perform configurations before the Connection is used for an Action in Init/Exec-phase
   */
  def activate(operation: Option[String])(implicit context: ActionPipelineContext): Unit = ()

}