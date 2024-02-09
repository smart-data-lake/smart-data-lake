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

package io.smartdatalake.util.misc

import org.reflections.Reflections

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe._

object ReflectionUtil {

  private val mirror = scala.reflect.runtime.currentMirror
  private val reflectionsCache = mutable.Map[String,Reflections]()

  def getReflections(packageName: String): Reflections = {
    reflectionsCache.getOrElseUpdate(packageName, new Reflections(packageName))
  }

  def getSealedTraitImplClasses[T: TypeTag]: Seq[Class[_]] = {
    val subClasses = typeOf[T].typeSymbol.asClass.knownDirectSubclasses
    subClasses.map(c => mirror.runtimeClass(c.asClass)).toSeq
  }

  def getTraitImplClasses[T: TypeTag](implicit reflections: Reflections): Seq[Class[_]] = {
    val baseCls = mirror.runtimeClass(typeOf[T].typeSymbol.asClass)
    reflections.getSubTypesOf(baseCls).asScala.toSeq
  }

  def classToType(cls: Class[_]): Type = {
    mirror.classSymbol(cls).toType
  }

}
