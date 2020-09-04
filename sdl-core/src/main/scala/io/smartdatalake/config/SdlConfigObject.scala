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
package io.smartdatalake.config

import io.smartdatalake.config.SdlConfigObject.{ConfigObjectId, validateId}

import scala.language.implicitConversions

/**
 * A first class object from which SDL [[ActionDAG]]s are built.
 */
private[smartdatalake] trait SdlConfigObject {

  /**
   * A unique identifier for this instance.
   */
  def id: ConfigObjectId

  validateId(id.id)
}

object SdlConfigObject {

  /**
   * trait for config object identifiers
   */
  sealed trait ConfigObjectId extends Any {
    def id: String
  }

  /**
   * Value class for connection identifiers.
   */
  case class ConnectionId(override val id: String) extends AnyVal with ConfigObjectId {
    override def toString: String = "Connection~"+id
  }

  /**
   * Value class for data object identifiers.
   */
  case class DataObjectId(override val id: String) extends AnyVal with ConfigObjectId {
    override def toString: String = "DataObject~"+id
  }

  /**
   * Value class for action object identifiers.
   */
  case class ActionObjectId(override val id: String) extends AnyVal with ConfigObjectId {
    override def toString: String = "Action~"+id
  }

  /**
   * Check allowed characters for id
   * Convention is important for custom sql transformation and Spark metrics parsing.
   */
  def validateId(id: String): Unit = {
    val regex = "([a-zA-Z0-9_-])+"
    if (!id.matches(regex)) throw ConfigurationException(s"Id $id is not valid. It must match regex '$regex'.")
  }

  implicit def stringToConnectionId(str: String): ConnectionId = ConnectionId(str)
  implicit def stringToDataObjectId(str: String): DataObjectId = DataObjectId(str)
  implicit def stringToActionObjectId(str: String): ActionObjectId = ActionObjectId(str)
}
