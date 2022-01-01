package io.smartdatalake.meta

import com.github.takezoe.scaladoc.{Scaladoc => ScaladocAnnotation}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.{Action, ActionMetadata}
import io.smartdatalake.workflow.connection.{Connection, ConnectionMetadata}
import io.smartdatalake.workflow.dataobject.{DataObject, DataObjectMetadata, Table}
import org.reflections.Reflections
import scaladoc.{Markup, Tag}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.{AssignOrNamedArg, ClassSymbol, MethodSymbol, TermSymbol, Type, typeOf}

/**
 * Base types for which type definitions are extracted.
 * Note that order is important for creation of json schema: Types used by other types should be placed later in the list.
 */
object BaseType extends Enumeration {
  type BaseType = Value
  val Connection = Value(classOf[Connection].getName)
  val DataObject = Value(classOf[DataObject].getName)
  val Action = Value(classOf[Action].getName)
  val Table = Value(classOf[Table].getName)
  val DataObjectMeta = Value(classOf[DataObjectMetadata].getName)
  val ActionMeta = Value(classOf[ActionMetadata].getName)
  val ConnectionMeta = Value(classOf[ConnectionMetadata].getName)
}

/**
 * Create generic SDL configuration elements by using reflection.
 */
private[smartdatalake] object GenericTypeUtil extends SmartDataLakeLogger {

  def getReflections = new Reflections("io.smartdatalake")

  /**
   * Finds all relevant types according to the config and generates GenericTypeDefs for them.
   * A final GenericTypeDef is marked with isFinal=true and represents an SDL case class used in configuration.
   * The final types include all attributes. Inherited attributes are marked with isOverride=true.
   * GenericTypeDefs include hte hierarchy of interfaces implemented by a type. They are listed in superTypes.
   * SuperTypes only include attributes which are defined by the type itself.
   * @return list of generic type definitions
   */
  def typeDefs(reflections: Reflections): Set[GenericTypeDef] = {

    val mirror = scala.reflect.runtime.currentMirror

    val allTypes = BaseType.values.toSeq.flatMap{ baseType =>
      val baseCls = getClass.getClassLoader.loadClass(baseType.toString)
      val subTypeClss = reflections.getSubTypesOf(baseCls).asScala.toSeq
      val allClss = subTypeClss :+ baseCls
      allClss.map(cls => (baseType, mirror.classSymbol(cls)))
    }

    val typeDefsWithCaseClassAttributes = allTypes.map{ case (baseType, cls) =>
      typeDefForClass(cls, allTypes.map(_._2), Some(baseType))
    }.toSet

    // propagate attributes from case classes to super type classes where they are defined.
    val typeDefsWithPropagatedAttributes = typeDefsWithCaseClassAttributes
      .map(typeDef => propagateAttributes(typeDef, typeDefsWithCaseClassAttributes))

    // mark overridden attributes with isOverride
    val refinedEntities = typeDefsWithPropagatedAttributes.map{typeDef =>
      if (typeDef.superTypes.nonEmpty) {
        val superTypeDefs = typeDefsWithPropagatedAttributes.filter(superTypeDef => typeDef.superTypes.contains(superTypeDef.cls))
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
      val childTypes = typeDefs.filter(entity => entity.superTypes.contains(typeDef.cls))
      val candidateAttributes = childTypes.toSeq.flatMap(_.attributes)
        .groupBy(a => (a.name, a.tpe)).map(_._2.maxBy(_.description.isDefined)) // if an attribute exists multiple times, take the first which has a description.
        .toSet
      val methodsWithoutParams = typeDef.cls.toType.decls.filter(_.isMethod).map(_.asMethod)
        .filter(m => m.paramLists.isEmpty || m.paramLists.map(_.size) == List(0))
        .map(m => (m.name.toString,extractOptionalType(m.typeSignature.resultType)))
        .toSet
      val propagatedAttributes = candidateAttributes.filter(candidateAttribute => {
        methodsWithoutParams.contains((candidateAttribute.name,candidateAttribute.tpe))
      }).toSeq
      typeDef.copy(attributes = propagatedAttributes)
    } else typeDef
  }

  private def extractOptionalType(tpe: Type) = {
    if (tpe <:< typeOf[scala.Option[_]]) tpe.typeArgs.head
    else tpe
  }

  private def formatScaladocTag(tag: Tag): Option[String] = {
    tag match {
      case x: Tag.Description => Some(s"${formatScaladocMarkup(x.makrup)}")
      case x: Tag.Constructor => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Param => Some(s"${x.getClass.getSimpleName.toUpperCase} ${x.name}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.TypeParam => Some(s"${x.getClass.getSimpleName.toUpperCase} [${x.name}]: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Returns => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Throws => Some(s"${x.getClass.getSimpleName.toUpperCase} ${x.exceptionType}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.See => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Note => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Example => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.UseCase => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Todo => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Deprecated => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Migration => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.OtherTag => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Author => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocString(x.text)}")
      case x: Tag.Version => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocString(x.text)}")
      case x: Tag.Since => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocString(x.text)}")
      case x: Tag.Group => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${x.id}")
      case x: Tag.GroupName => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${x.value}")
      case x: Tag.GroupDescription => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.GroupPriority => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${x.value}")
      case Tag.Documentable => None
      case Tag.InheritDoc => None
    }
  }

  private def formatScaladocString(str: String) = {
    str.replace(raw"\n", System.lineSeparator()).replaceAll(raw"(?m)^\s*\*\s*", "").trim
  }

  private def formatScaladocMarkup(markup: Markup) = {
    formatScaladocString(markup.trimmed.plainString)
  }

  def typeDefForClass(cls: ClassSymbol, interestingSuperTypes: Seq[ClassSymbol] = Seq(), baseType: Option[BaseType.BaseType] = None): GenericTypeDef = {
    val superTypeNames = interestingSuperTypes.map(_.name)
    val parentTypes = if (superTypeNames.nonEmpty)
      cls.baseClasses.filter(baseCls => superTypeNames.contains(baseCls.name) && baseCls != cls).map(_.asClass)
    else Seq()
    val name = cls.fullName.split('.').last
    val rawScaladoc = cls.annotations.find(_.tree.tpe =:= typeOf[ScaladocAnnotation])
      .flatMap(_.tree.children.collectFirst{ case x: AssignOrNamedArg => x.rhs.toString })
    val parsedScaladoc = rawScaladoc.map(d => scaladoc.Scaladoc.fromString(d).right.get)
    val rawDescription = parsedScaladoc.map(d => d.tags.filterNot(_.isInstanceOf[Tag.Param]).flatMap(formatScaladocTag).mkString(System.lineSeparator()))
    val description = rawDescription.map(_.replaceAll(raw"(?m)^\s?\*\s?", "").trim)
    val attributes = if (cls.isCaseClass) attributesForCaseClass(cls, parsedScaladoc.map(_.textParams.mapValues(formatScaladocString)).getOrElse(Map())) else Seq()
    GenericTypeDef(name, baseType, cls, description, cls.isCaseClass, parentTypes.toSet, attributes)
  }

  /**
   * Find all attributes for a given case class
   */
  def attributesForCaseClass(cls: ClassSymbol, paramDescriptions: Map[String,String]): Seq[GenericAttributeDef] = {
    // get case class constructor parameters
    val tpe = cls.toType
    val params = tpe.decls.collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor =>
          // only use first parameter list of case class constructor
          m.paramLists.head
      }.getOrElse(throw new RuntimeException("no primary constructor found. bug?"))
    // prepare parameters
    params.map(p => {
      val isDeprecated = p.annotations.exists(_.tree.tpe =:= typeOf[deprecated])
      val isOverride = p.overrides.nonEmpty
      val isOptional = p.typeSignature <:< typeOf[scala.Option[_]]
      val hasDefaultValue = p match {
        case t: TermSymbol => t.isParamWithDefault
        case _ => false
      }
      val tpe = if (isOptional) {
        p.typeSignature.typeArgs.head
      } else p.typeSignature
      val description = paramDescriptions.get(p.name.toString)
      GenericAttributeDef(p.name.encodedName.toString, tpe, description, isRequired = !isOptional && !hasDefaultValue, isOverride = isOverride, isDeprecated = isDeprecated)
    })
  }

  /**
   * extract case class attributes with values through reflection
   */
  def attributesWithValuesForCaseClass(obj: Any): Seq[(String, Any)] = {
    val cls = mirror.classSymbol(obj.getClass)
    val inst = mirror.reflect(obj)
    val attributes = cls.toType.members.collect { case m: MethodSymbol if m.isCaseAccessor => m }
    attributes.map { m =>
      val key = m.name.toString
      val value = inst.reflectMethod(m).apply()
      (key, value)
    }.toSeq
  }

  private val mirror = scala.reflect.runtime.currentMirror
}
