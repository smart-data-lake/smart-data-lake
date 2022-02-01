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

import io.smartdatalake.meta.{GenericAttributeDef, GenericTypeDef}

/**
 * Definition of an entity type to be exported to Atlas.
 */
case class AtlasTypeDef(
                      name: String,
                      description: String,
                      superTypes: Set[String],
                      attributeDefs: Set[AtlasAttributeDef]
                    )
object AtlasTypeDef {
  def apply(typeDef: GenericTypeDef): AtlasTypeDef = {
    AtlasTypeDef(typeDef.name, typeDef.description.getOrElse(""), typeDef.superTypes.map(_.typeSymbol.name.toString), typeDef.attributes.map(AtlasAttributeDef.apply).toSet)
  }
}

/**
 * Definition of an attribute of an Atlas type.
 */
case class AtlasAttributeDef(
                              name: String,
                              typeName: String,
                              cardinality: String,
                              isOptional: Boolean
                            ) extends Serializable
object AtlasAttributeDef {
  def apply(attrDef: GenericAttributeDef): AtlasAttributeDef = {
    //TODO: check cardinality
    val cardinality = "SINGLE"
    AtlasAttributeDef(attrDef.name, attrDef.tpe.typeSymbol.typeSignature.toString, cardinality, !attrDef.isRequired)
  }
}