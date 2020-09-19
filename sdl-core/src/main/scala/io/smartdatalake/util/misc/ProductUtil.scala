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
package io.smartdatalake.util.misc

import io.smartdatalake.config.SdlConfigObject.ConfigObjectId

private[smartdatalake] object ProductUtil {

  /**
   * Gets the field value for a specified field of a case class instance by field name reflection.
   * Used i.e. for the exporter:
   * We want to export the different attributes of [[DataObject]]s and [[io.smartdatalake.workflow.action.Action]]s
   * without knowing the concrete subclass.
   *
   * @param obj       the object to search extract the field from
   * @param fieldName the field name to search by reflection on the given object
   * @tparam T        type of the field to be extracted
   * @return Some(field value) if the field exists, None otherwise
   */
  def getFieldData[T](obj: Product, fieldName: String): Option[T] = {
    getRawFieldData(obj, fieldName).map(_.asInstanceOf[T])
  }

  /**
   * Same as getFieldData, but helps extracting an optional field type
   */
  def getOptionalFieldData[T](obj: Product, fieldName: String): Option[T] = {
    getRawFieldData(obj, fieldName).flatMap(_.asInstanceOf[Option[T]])
  }

  /**
   * Same as getFieldData, but helps extracting an field which is optional for some objects but for others not
   */
  def getEventuallyOptionalFieldData[T](obj: Product, fieldName: String): Option[T] = {
    getRawFieldData(obj, fieldName).flatMap {
      case x: Option[_] => x.map(_.asInstanceOf[T])
      case x => Some(x.asInstanceOf[T])
    }
  }

  def getIdFromConfigObjectIdOrString(obj: Any) = obj match {
    case id: String => id
    case obj: ConfigObjectId => obj.id
  }

  private def getRawFieldData(obj: Product, fieldName: String): Option[Any] = {
    obj.getClass.getDeclaredFields.find(_.getName == fieldName)
      .map {
        x =>
          x.setAccessible(true)
          x.get(obj)
      }
  }
}