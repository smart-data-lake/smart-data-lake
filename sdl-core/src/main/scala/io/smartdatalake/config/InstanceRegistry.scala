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

import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataobject.DataObject

import scala.collection.mutable

/**
 * Registers instantiated SDL first class objects ([[io.smartdatalake.workflow.action.Action]]s,
 * [[io.smartdatalake.workflow.dataobject.DataObject]]s, etc.) and enables to retrieve instantiated instances.
 */
class InstanceRegistry {

  private[config] val instances: mutable.Map[ConfigObjectId, SdlConfigObject] = mutable.Map.empty

  /**
   * Add all instances from `instancesToAdd` to this instance registry, overwriting existing entries with the same id.
   *
   * @param instancesToAdd the instances to add.
   */
  def register[A <: ConfigObjectId, B <: SdlConfigObject](instancesToAdd: Map[A, B]): Unit = instances ++= instancesToAdd

  /**
   * Add all instances from `instancesToAdd` to this instance registry, overwriting existing entries the same ids.
   *
   * @param instancesToAdd the instances to add.
   */
  def register(instancesToAdd: Seq[SdlConfigObject]): Unit = instancesToAdd.foreach(register)

  /**
   * Register a new instance, overwriting an existing entry with the same id.
   *
   * @param instance the instance to register
   */
  def register(instance: SdlConfigObject): Unit = {
    instances(instance.id) = instance
  }

  /**
   * Retrieve a registered instance.
   *
   * @param objectId the id of the instance.
   * @return the instance registered with this id
   */
  def get[A <: SdlConfigObject](objectId: ConfigObjectId): A = instances(objectId).asInstanceOf[A]

  /**
   * Remove a registered instance from the registry.
   *
   * @param objectId the id of the instance.
   * @return  an option value with the instance that was registered with this id
   *          or `None` if no instance was registered with this id.
   */
  def remove(objectId: ConfigObjectId): Option[SdlConfigObject] = instances.remove(objectId)

  /**
   * Empty the registry.
   *
   * Use this to clean up the registry and avoid memory leaks.
   * Registered instances can not be garbage collected by the JVM.
   */
  def clear(): Unit = instances.clear()

  /**
   * Returns registered Actions
   */
  def getActions: Seq[Action] = instances.values.collect{ case a: Action => a }.toSeq

  /**
   * Returns registered DataObjects
   */
  def getDataObjects: Seq[DataObject] = instances.values.collect{ case d: DataObject => d}.toSeq

  /**
   * Returns registered Connections
   */
  def getConnections: Seq[Connection] = instances.values.collect{ case c: Connection => c}.toSeq
}
