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

import io.smartdatalake.config.SdlConfigObject.{ConfigObjectId, DataObjectId}
import io.smartdatalake.workflow.action.{Action, DataFrameActionImpl}
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanWriteDataFrame, DataObject, ExpectationValidation}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

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
  def register[A <: ConfigObjectId, B <: SdlConfigObject](instancesToAdd: Map[A, B]): Unit = {
    instances ++= instancesToAdd
    _dataObjectIdsToValidateOnRead = None // reset precomputed value
  }

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
    _dataObjectIdsToValidateOnRead = None // reset precomputed value
    instances(instance.id) = instance
  }

  /**
   * Retrieve a registered instance.
   *
   * @param objectId the id of the instance.
   * @return the instance registered with this id
   */
  def get[A <: SdlConfigObject : TypeTag : ClassTag](objectId: ConfigObjectId): A = {
    instances(objectId) match {
      case x: A => x
      case x =>
        val expectedType = typeOf[A].toString.replaceAll(classOf[DataObject].getPackage.getName + ".", "")
        throw TypeMismatchException(s"Cannot cast $objectId to ${typeOf[A]}", x.getClass, expectedType)
    }
  }

  /**
   * Remove a registered instance from the registry.
   *
   * @param objectId the id of the instance.
   * @return  an option value with the instance that was registered with this id
   *          or `None` if no instance was registered with this id.
   */
  def remove(objectId: ConfigObjectId): Option[SdlConfigObject] = {
    _dataObjectIdsToValidateOnRead = None // reset precomputed value
    instances.remove(objectId)
  }

  /**
   * Empty the registry.
   *
   * Use this to clean up the registry and avoid memory leaks.
   * Registered instances can not be garbage collected by the JVM.
   */
  def clear(): Unit = {
    _dataObjectIdsToValidateOnRead = None // reset precomputed value
    instances.clear()
  }

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

  /**
   * Returns Ids of DataObjects for which expectations and constraints should be validated on read, e.g. there is no DataFrame-Action having these DataObjects as output.
   * Value is precomputed to avoid evaluation for every Action.
   */
  def getDataObjectIdsToValidateOnRead: Seq[DataObjectId] = {
    if (_dataObjectIdsToValidateOnRead.isEmpty) {
      // only DataObjects that can Create/Write DataFrames with ExpectationValidation are relevant
      val expectationValidationDataObjects = getDataObjects.collect{case x: CanCreateDataFrame with CanWriteDataFrame with ExpectationValidation => x}
      // get DataObjects that are written by an Action using DataFrames
      val dataFrameActions = getActions.collect{case x: DataFrameActionImpl => x}.flatMap(_.outputs)
      // all DataObjects which are not used as an output should be validated on read
      _dataObjectIdsToValidateOnRead = Some(expectationValidationDataObjects.map(_.id).diff(dataFrameActions.map(_.id)))
    }
    _dataObjectIdsToValidateOnRead.get
  }
  private var _dataObjectIdsToValidateOnRead: Option[Seq[DataObjectId]] = None

  /**
   * Check if this is DataObject should be validated on read
   */
  def shouldValidateDataObjectOnRead(id: DataObjectId): Boolean = getDataObjectIdsToValidateOnRead.contains(id)
}

case class TypeMismatchException(msg: String, currentClass: Class[_], expectedType: String) extends Exception(msg)