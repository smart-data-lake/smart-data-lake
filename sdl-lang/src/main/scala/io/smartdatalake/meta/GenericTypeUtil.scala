package io.smartdatalake.meta

import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.misc.ScaladocUtil.{extractScalaDoc, formatScaladocString, formatScaladocWithTags}
import io.smartdatalake.util.misc.{ReflectionUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, GenericDfsTransformer, ValidationRule}
import io.smartdatalake.workflow.action.script.ParsableScriptDef
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.action.{Action, ActionMetadata}
import io.smartdatalake.workflow.connection.authMode.AuthMode
import io.smartdatalake.workflow.connection.{Connection, ConnectionMetadata}
import io.smartdatalake.workflow.dataobject.{DataObject, DataObjectMetadata, HousekeepingMode, Table}
import org.reflections.Reflections
import scaladoc.Tag

import scala.jdk.CollectionConverters._
import scala.reflect.internal.Symbols
import scala.reflect.runtime.universe.{MethodSymbol, TermSymbol, Type, typeOf}


/**
 * Create generic SDL configuration elements by using reflection.
 */
private[smartdatalake] object GenericTypeUtil extends SmartDataLakeLogger {

  /**
   * Base types for which type definitions are extracted.
   * Note that order is important for creation of json schema: Types used by other types should be placed later in the list.
   */
  def baseTypes: Seq[Type] = Seq(
    typeOf[Connection],
    typeOf[DataObject],
    typeOf[Action],
    typeOf[Table],
    typeOf[DataObjectMetadata],
    typeOf[ActionMetadata],
    typeOf[ConnectionMetadata],
    typeOf[GenericDfTransformer],
    typeOf[GenericDfsTransformer],
    typeOf[ParsableScriptDef],
    typeOf[ExecutionMode],
    typeOf[HousekeepingMode],
    typeOf[AuthMode],
    typeOf[ValidationRule],
    typeOf[SaveModeOptions],
    typeOf[CustomDfTransformerConfig]
  )

  def getReflections = ReflectionUtil.getReflections("io.smartdatalake")

  /**
   * Finds all relevant types according to the config and generates GenericTypeDefs for them.
   * A final GenericTypeDef is marked with isFinal=true and represents an SDL case class used in configuration.
   * The final types include all attributes. Inherited attributes are marked with isOverride=true.
   * GenericTypeDefs include the hierarchy of interfaces implemented by a type. They are listed in superTypes.
   * SuperTypes only include attributes which are defined by the type itself.
   * @return list of generic type definitions
   */
  def typeDefs(reflections: Reflections): Set[GenericTypeDef] = {

    val mirror = scala.reflect.runtime.currentMirror

    val allTypes = baseTypes.flatMap { baseType =>
      val baseCls = getClass.getClassLoader.loadClass(baseType.typeSymbol.fullName)
      val subTypeClss = reflections.getSubTypesOf(baseCls).asScala.toSeq
      subTypeClss.map(cls => (Some(baseType), mirror.classSymbol(cls).toType)) :+ (None, baseType)
    }

    val typeDefsWithCaseClassAttributes = allTypes.map{ case (baseType, tpe) =>
      typeDefForClass(tpe, allTypes.map(_._2), baseType)
    }.toSet

    // propagate attributes from case classes to super type classes where they are defined.
    val typeDefsWithPropagatedAttributes = typeDefsWithCaseClassAttributes
      .map(typeDef => propagateAttributes(typeDef, typeDefsWithCaseClassAttributes))

    // mark overridden attributes with isOverride
    val refinedEntities = typeDefsWithPropagatedAttributes.map{typeDef =>
      if (typeDef.superTypes.nonEmpty) {
        val superTypeDefs = typeDefsWithPropagatedAttributes.filter(superTypeDef => typeDef.superTypes.contains(superTypeDef.tpe))
        val overriddenAttributes = superTypeDefs.flatMap(_.attributes)
        val refinedAttributes = typeDef.attributes.map(a => if(overriddenAttributes.contains(a)) a.copy(isOverride = true) else a)
        val indirectSuperTypes = superTypeDefs.flatMap(_.superTypes)
        val directSuperTypes = typeDef.superTypes.diff(indirectSuperTypes)
        typeDef.copy(attributes = refinedAttributes, superTypes = directSuperTypes)
      } else typeDef
    }

    refinedEntities
  }

  /**
   * Finds attribute definitions that are defined by the given type and used by any of it's children.
   * All defined attributes are returned for leaves (case classes).
   * @param typeDef the type to enrich
   * @param typeDefs all type definitions to search.
   * @return enriched type definition
   */
  def propagateAttributes(typeDef: GenericTypeDef, typeDefs: Set[GenericTypeDef]): GenericTypeDef = {
    if (!typeDef.isFinal) {
      val childTypes = typeDefs.filter(entity => entity.superTypes.contains(typeDef.tpe))
      val candidateAttributes = childTypes.toSeq.flatMap(_.attributes)
        .groupBy(a => (a.name, a.tpe)).map(_._2.maxBy(_.description.isDefined)) // if an attribute exists multiple times, take the first which has a description.
        .toSet
      val methodsWithoutParams = typeDef.tpe.decls.filter(_.isMethod).map(_.asMethod)
        .filter(m => m.paramLists.isEmpty || m.paramLists.map(_.size) == List(0))
        .flatMap(m => extractOptionalResultType(m).map(t => (m.name.toString,t)))
        .toSet
      val propagatedAttributes = candidateAttributes.filter(candidateAttribute => {
        methodsWithoutParams.contains((candidateAttribute.name,candidateAttribute.tpe))
      }).toSeq
      typeDef.copy(attributes = propagatedAttributes)
    } else typeDef
  }

  private def extractOptionalResultType(m: MethodSymbol): Option[Type] = {
    try {
      val tpe = m.typeSignature.resultType
      val t = if (tpe <:< typeOf[scala.Option[_]]) tpe.typeArgs.head else tpe
      Some(t)
    } catch {
      // reflection might cause "illegal cyclic reference involving class InterfaceAudience" with Annotation from InterfaceAudience in hadoop-annotations library.
      case e: Symbols#CyclicReference =>
        logger.warn(s"Could not extractOptionalResultType from ${m.toString}: ${e.getMessage}. This is caused by using reflection when hadoop-annotations being in the classpath.")
        None
    }
  }

  def typeDefForClass(tpe: Type, interestingSuperTypes: Seq[Type] = Seq(), baseType: Option[Type] = None): GenericTypeDef = {
    val parentTypes = if (interestingSuperTypes.nonEmpty)
      tpe.baseClasses.map(_.asType.toType).filter(baseType => interestingSuperTypes.contains(baseType) && baseType != tpe)
    else Seq()
    val name = tpe.typeSymbol.name.toString
    val scaladoc = extractScalaDoc(tpe.typeSymbol.annotations)
    val description = scaladoc.map(formatScaladocWithTags(_, !_.isInstanceOf[Tag.Param]))
    val attributes = if (tpe.typeSymbol.asClass.isCaseClass) attributesForCaseClass(tpe, scaladoc.map(_.textParams.mapValues(formatScaladocString).toMap).getOrElse(Map())) else Seq()
    GenericTypeDef(name, baseType, tpe, description, tpe.typeSymbol.asClass.isCaseClass, parentTypes.toSet, attributes)
  }

  /**
   * Find all attributes for a given case class
   */
  def attributesForCaseClass(tpe: Type, paramDescriptions: Map[String,String]): Seq[GenericAttributeDef] = {
    // get case class constructor parameters
    val params = tpe.decls.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor =>
        // only use first parameter list of case class constructor
        m.paramLists.head
    }.getOrElse(throw new RuntimeException("no primary constructor found. bug?"))
    // get SDLB base classes to search for overrides and scaladoc
    val superTypeMethodsWithoutParameters = tpe.baseClasses
      .filterNot(_ == tpe.typeSymbol)
      .filter(_.fullName.startsWith("io.smartdatalake"))
      .flatMap(_.asType.toType.decls.collect{ case m:MethodSymbol if m.paramLists.isEmpty || m.paramLists.head.isEmpty => m })
    // prepare parameters
    params.map(p => {
      // only java annotations are kept for runtime. SDLB needs to use Java @Deprecated annotation to be able to retrieve this with reflection.
      val isDeprecated = p.annotations.exists(_.tree.tpe =:= typeOf[Deprecated])
      val overriddenMethods = superTypeMethodsWithoutParameters
        .filter(m => p.name == m.name && p.typeSignature.resultType <:< m.typeSignature.resultType)
      val isOverride = overriddenMethods.nonEmpty
      val isOptional = p.typeSignature <:< typeOf[scala.Option[_]]
      val hasDefaultValue = p match {
        case t: TermSymbol => t.isParamWithDefault
        case _ => false
      }
      val tpe = if (isOptional) {
        p.typeSignature.typeArgs.head
      } else p.typeSignature
      val description = paramDescriptions.get(p.name.toString)
        .orElse(overriddenMethods.map(m => extractScalaDoc(m.annotations).map(formatScaladocWithTags(_))).find(_.isDefined).flatten) // use Scaladoc from first overridden method
      GenericAttributeDef(p.name.encodedName.toString, tpe, description, isRequired = !isOptional && !hasDefaultValue, isOverride = isOverride, isDeprecated = isDeprecated)
    })
  }
}
