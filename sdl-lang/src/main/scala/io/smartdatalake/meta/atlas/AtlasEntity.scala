/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.meta.atlas

/**
 * Container Class for definition of an atlas attribute.
 */
case class AtlasEntity(
                          typeName: String,
                          guid: Int,
                          attributes: Map[String, Any],
                          classifications: Seq[Map[String, String]],
                          @transient obj: Any
                          ) extends Serializable

object AtlasEntity {
  private var nguid: Int = 0

  /**
   * New AtlasEntity.
   * The assigned id is a negative number decreasing with each call.
   * Atlas will replace a negative id with a unique id.
   */
  def apply(typeName: String, attributes: Map[String, Any], obj: Any): AtlasEntity = {
    nguid = nguid - 1
    AtlasEntity(typeName, nguid, attributes, Seq(), obj)
  }

}
