/*
 * sdl-core - Build your data lake the smart way.
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
package io.smartdatalake.util.spark.json

import com.jayway.jsonpath.spi.json.JsonProvider
import com.jayway.jsonpath.spi.mapper.MappingProvider
import com.jayway.jsonpath.{Configuration, TypeRef}
import io.smartdatalake.util.misc.ScalaUtil
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods

import java.io.InputStream
import java.{lang, util}
import scala.jdk.CollectionConverters._

/**
 * Wrapper to use Json4s with JsonPath library.
 *
 * Note Json4s uses immutable objects, but some part of the JsonPath API needs modifying json object.
 * A workaround is using reflection to set `val`s.
 */
class Json4sJsonPathProvider extends JsonProvider {

  override def parse(s: String): JValue = JsonMethods.parse(s)

  override def parse(inputStream: InputStream, s: String): JValue = JsonMethods.parse(inputStream)

  override def toJson(o: AnyRef): String = JsonMethods.compact(o.asInstanceOf[JValue])

  override def createArray(): AnyRef = JArray(List[JValue]())

  override def createMap(): AnyRef = JObject()

  override def isArray(o: Any): Boolean = o.isInstanceOf[JArray]

  override def length(o: Any): Int = o match {
    case x: JArray => x.arr.length
    case x: JObject => x.obj.length
  }

  override def toIterable(o: Any): lang.Iterable[_] = o.asInstanceOf[JArray].arr.asJava

  override def getPropertyKeys(o: Any): util.Collection[String] = o.asInstanceOf[JObject].obj.map(_._1).asJavaCollection

  override def getArrayIndex(o: Any, i: Int): AnyRef = {
    o.asInstanceOf[JArray].apply(i)
  }

  override def getArrayIndex(o: Any, i: Int, unwrap: Boolean): AnyRef = {
    val entry = o.asInstanceOf[JArray].apply(i)
    if (unwrap) entry.values.asInstanceOf[AnyRef]
    else entry
  }

  override def setArrayIndex(o: Any, i: Int, value: Any): Unit = {
    val jArr = o.asInstanceOf[JArray]
    var arr: List[Any] = jArr.arr
    if (arr.length < i + 1) arr = arr ++ List.fill(arr.length - i + 1)(JsonProvider.UNDEFINED)
    arr = arr.updated(i, wrap(value))
    // ATTENTION: mutating scala vals should not be done, but this API is not designed for immutable objects!
    ScalaUtil.mutateVal(jArr, "arr", arr)
  }

  override def getMapValue(o: Any, key: String): AnyRef = o.asInstanceOf[JObject].obj.find(_._1 == key).map(_._2)
    .getOrElse(JsonProvider.UNDEFINED)

  override def setProperty(o: Any, key: Any, value: Any): Unit = {
    val jMap = o.asInstanceOf[JObject]
    var map = jMap.obj
    map = map.filter(_._1 == key.asInstanceOf[String]) :+ JField(key.asInstanceOf[String], wrap(value))
    // ATTENTION: mutating scala vals should not be done, but this API is not designed for immutable objects!
    ScalaUtil.mutateVal(jMap, "obj", map)
  }

  override def removeProperty(o: Any, key: Any): Unit = {
    val jMap = o.asInstanceOf[JObject]
    var map = jMap.obj
    map = map.filter(_._1 == key.asInstanceOf[String])
    // ATTENTION: mutating scala vals should not be done, but this API is not designed for immutable objects!
    ScalaUtil.mutateVal(jMap, "obj", map)
  }

  override def isMap(o: Any): Boolean = o.isInstanceOf[JObject]

  override def unwrap(o: Any): AnyRef = o match {
    case x: JValue => x.values.asInstanceOf[AnyRef]
    case x: AnyRef => x
  }

  def wrap(o: Any): JValue = {
    o match {
      case x: String => JString(x)
      case x: Long => JLong(x)
      case x: Int => JInt(x)
      case x: BigDecimal => JDecimal(x)
      case x: Double => JDouble(x)
      case x: Boolean => JBool(x)
      case x: Map[String, _] =>
        val fields = x.map { case (k, v) => JField(k, wrap(v)) }.toList
        JObject(fields)
      case x: JValue => x
      case x: Seq[JValue] => JArray(x.toList)
      case x: Set[JValue] => JSet(x)
      case None | null => JNothing
    }
  }
}

/**
 * Dummy mapping provider for JsonPath, as we dont need it.
 */
class DummyMappingProvider extends MappingProvider {
  override def map[T](source: Any, targetType: Class[T], configuration: Configuration): T = throw new NotImplementedError()

  override def map[T](source: Any, targetType: TypeRef[T], configuration: Configuration): T = throw new NotImplementedError()
}
