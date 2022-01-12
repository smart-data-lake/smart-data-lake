package io.smartdatalake.meta

import scala.reflect.runtime.universe.{ClassSymbol, Type}

/**
 * Generic definition of SDL configuration elements
 */
private[smartdatalake] case class GenericTypeDef(
                           name: String,
                           baseType: Option[BaseType.BaseType],
                           cls: ClassSymbol,
                           description: Option[String],
                           isFinal: Boolean,
                           superTypes: Set[ClassSymbol],
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