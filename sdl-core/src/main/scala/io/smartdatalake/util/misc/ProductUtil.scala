/*
 * Smart Data Lake - Build your data lake the smart way.
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
package io.smartdatalake.util.misc

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConfigObjectId

import java.time.format.DateTimeFormatter
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{currentMirror, universe}

object ProductUtil {

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

  def getIdFromConfigObjectIdOrString(obj: Any): String = obj match {
    case id: String => id
    case obj: ConfigObjectId => obj.id
  }

  private[smartdatalake] def getRawFieldData(obj: Product, fieldName: String): Option[Any] = {
    obj.getClass.getDeclaredFields.find(_.getName == fieldName)
      .map {
        x =>
          x.setAccessible(true)
          x.get(obj)
      }
  }


  /**
   * Converts an arbitrary object to a one-line string, which is as easy as possible to read in logs.
   * Case classes and Maps are formatted as key=value list.
   */
  def formatObj(obj: Any, truncateListLimit: Int = 10): String = {
    import scala.jdk.CollectionConverters._

    // recursive function to add an object to the message
    def addObjToBuilder(msg: StringBuilder, inputObj: Any, spacing: Boolean = true): Unit = {
      if (spacing) msg.append(" ")
      inputObj match {
        // handle Options
        case None => msg.append("None")
        case Some(obj) => addObjToBuilder(msg, obj, spacing = false)
        // handle key->value pairs
        case obj: Tuple2[Any, Any] =>
          addObjToBuilder(msg, obj._1, spacing = false)
          msg.append("=")
          addObjToBuilder(msg, obj._2, spacing = false)
        // handle arrays -> convert to Seq (Iterable)
        case obj: Array[Any] => addObjToBuilder(msg, obj.toSeq, spacing = false)
        // handle lists & maps
        case objs: Iterable[Any] =>
          msg.append("[")
          val truncatedObjs = objs.take(truncateListLimit)
          // no spacing for first element
          objs.zip(Seq(false).padTo(truncatedObjs.size, true))
            .foreach { case (elem, spacing) => addObjToBuilder(msg, elem, spacing) }
          if (objs.size>truncateListLimit)msg.append("...]")
          msg.append("]")
        // handle case classes
        case obj: Product =>
          msg.append(s"${obj.productPrefix}=")
          addFields(msg, obj)
        // handle Hocon Config
        case config: Config => addPairs(msg, config.root.unwrapped.asScala.toSeq)
        // Java Enums
        case enum:AnyRef if enum.getClass.isEnum =>
          msg.append(s"${enum.getClass.getSimpleName}=")
          msg.append(enum.toString)
        // BigDecimal needs removal of trailing zeros
        case d: BigDecimal => msg.append(d.underlying.stripTrailingZeros.toPlainString)
        case javaD: java.math.BigDecimal => msg.append(javaD.stripTrailingZeros.toPlainString)
        // java maps from Hocon Config land here... we route them again through addObjToBuilder as scala sequence
        case javaMap: java.util.Map[_, _] => addObjToBuilder(msg, javaMap.asScala.toSeq, spacing = false)
        // date & time
        case d: java.time.LocalDate => msg.append(d.format(DateTimeFormatter.ISO_DATE))
        case dt: java.time.LocalDateTime => msg.append(dt.format(DateTimeFormatter.ISO_DATE_TIME))
        case ts: java.sql.Timestamp => msg.append(ts.toLocalDateTime.format(DateTimeFormatter.ISO_DATE_TIME))
        // strings
        case str: String => msg.append(str)
        // other types are just converted to string
        case x: Any => msg.append(x.toString)
        // null
        case null => msg.append("null")
      }
    }

    // convert Map entries as [k1=v1 k2=v2 ...]
    @inline def addPairs(msg: StringBuilder, pairs: Seq[(String,Any)]): Unit = {
      msg.append("[")
      // first pair should have no spacing
      pairs.zip(Seq(false).padTo(pairs.size, true))
        .foreach { case ((key, value), spacing) => addObjToBuilder(msg, key -> value, spacing) }
      msg.append("]")
    }

    // logs the fields of a case class as key=value
    @inline def addFields(msg: StringBuilder, obj: Product): Unit = {
      // extract product fields as key/value pairs
      val cls = obj.getClass
      val pairs  = cls.getDeclaredFields.filterNot(_.isSynthetic).map{ f =>
        f.setAccessible(true)
        (f.getName,f.get(obj))
      }
      addPairs(msg, pairs)
    }

    // generate string
    val msg = StringBuilder.newBuilder
    addObjToBuilder(msg, obj, spacing = false)
    msg.toString
  }

  /**
   * Given the name of a Product class, return its attribute names.
   */
  def classAccessorNames(className: String): List[String] = {
    val mirror = scala.reflect.runtime.currentMirror
    val tpe: universe.Type = mirror.classSymbol(mirror.classLoader.loadClass(className)).toType
    classAccessorNames(tpe)
  }

  /**
   * Given the name of a Product class, return its attribute names.
   */
  def classAccessorNames[A <: Product : ClassTag](): List[String] = {
    val cls = implicitly[ClassTag[A]].runtimeClass
    val mirror = scala.reflect.runtime.currentMirror
    val tpe: universe.Type = mirror.classSymbol(cls).toType
    classAccessorNames(tpe)
  }

  /**
   * Given the type of a Product class, return its attribute names.
   */
  def classAccessorNames(tpe: universe.Type): List[String] = {
    classAccessors(tpe).map(_.name.toString)
  }

  /**
   * Given the name of a Product class, return its attribute names.
   */
  def classAccessors[A <: Product : ClassTag](): List[MethodSymbol] = {
    val cls = implicitly[ClassTag[A]].runtimeClass
    val mirror = scala.reflect.runtime.currentMirror
    val tpe: universe.Type = mirror.classSymbol(cls).toType
    classAccessors(tpe)
  }

  /**
   * Given the name of a Product class, return its attribute accessor methods.
   */
  def classAccessors(tpe: universe.Type): List[MethodSymbol] = tpe.decls.sorted.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }

  /**
   * Extract case class attributes with values through reflection
   */
  def attributesWithValuesForCaseClass(obj: Any): Seq[(String, Any)] = {
    val clsSym = currentMirror.classSymbol(obj.getClass)
    val inst = currentMirror.reflect(obj)

    val attributes = classAccessors(clsSym.toType)
    attributes.map { m =>
      val key = m.name.toString
      val value = inst.reflectMethod(m).apply()
      (key, value)
    }.toSeq
  }

  /**
   * Dynamically apply copy constructor of a case class, replacing the value of one attribute of the instance.
   * This is useful in class hierarchies to apply the copy constructor from the super class.
   */
  def dynamicCopy[T: ClassTag, V](obj: T, fieldName: String, newValue: V): T = {
    val clsSym = currentMirror.classSymbol(obj.getClass)
    val inst = currentMirror.reflect(obj)
    val copyConstructor = inst.symbol.toType.decls.find(_.name.toString == "copy")
      .getOrElse(throw new IllegalStateException(s"copy constructor method not found in object of type ${obj.getClass.getSimpleName}"))
    val attributes = classAccessors(clsSym.toType)
      .map { m =>
        val key = m.name.toString
        val value = if (key == fieldName) newValue else inst.reflectMethod(m).apply()
        (key, value)
      }.toSeq
    inst.reflectMethod(copyConstructor.asMethod).apply(attributes.map(_._2): _*).asInstanceOf[T]
  }
}