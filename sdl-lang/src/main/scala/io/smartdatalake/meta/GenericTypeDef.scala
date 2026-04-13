/*
 * sdl-lang - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.meta

import scala.reflect.runtime.universe.{ClassSymbol, Type}

/**
 * Generic definition of SDL configuration elements
 */
private[smartdatalake] case class GenericTypeDef(
                                                  name: String,
                                                  baseTpe: Option[Type],
                                                  tpe: Type,
                                                  description: Option[String],
                                                  isFinal: Boolean,
                                                  superTypes: Set[Type],
                                                  attributes: Seq[GenericAttributeDef]
                         )

/**
 * Generic definition of attributes of SDL configuration elements
 */
private[smartdatalake] case class GenericAttributeDef(
                         name: String,
                         tpe: Type,
                         description: Option[String],
                         isRequired: Boolean,
                         isDeprecated: Boolean,
                         isOverride: Boolean
                       ) extends Serializable