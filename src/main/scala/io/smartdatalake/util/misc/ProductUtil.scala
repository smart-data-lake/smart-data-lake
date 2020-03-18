/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019 ELCA Informatique SA (<https://www.elca.ch>)
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

private[smartdatalake] object ProductUtil {

 /**
  * Gets the field of a class with the given name.
  * Used i.e. for the exporter:
  * We want to export field data for different subtypes of [[DataObject]]s and [[io.smartdatalake.workflow.action.Action]]s
  * but on the superclass, only id and metadata fields are defined.
  *
  * Returns the original field types (no conversion to string i.e) so you need to handle the proper
  * types.
  *
  * @param subtypeOf
  * @param name
  * @return Some if the field exists, None otherwise
  */
 def getFieldData(subtypeOf: Product, name:String) : Option[Any] = {
  subtypeOf.getClass.getDeclaredFields.find(_.getName==name)
    .map {
     x =>
      x.setAccessible(true)
      x.get(subtypeOf)
    }
 }

}
