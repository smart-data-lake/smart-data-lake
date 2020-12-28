/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.config

import java.lang.reflect.InvocationTargetException

import com.typesafe.config.Config
import io.smartdatalake.util.misc.SmartDataLakeLogger

import scala.reflect.runtime.universe._

/**
 * This class manages factory method invocation for instances defined in SDL configurations.
 *
 * @param moduleSymbol  the symbol of the (companion) object on which to invoke the apply method.
 * @param methodSymbol  the symbol of the apply method to invoke.
 * @tparam A the (abstract) type of the case class, e.g. [[io.smartdatalake.workflow.action.Action]].
 */
private[smartdatalake] class FactoryMethod[A](val moduleSymbol: ModuleSymbol, val methodSymbol: MethodSymbol) extends SmartDataLakeLogger {

  /**
   * Invoke this factory method.
   *
   * The supplied config is expected to be the config object containing the constructor parameters.
   *
   * @param config                  the config for this instance.
   * @param instanceRegistry        the instance registry
   * @return                        a new config object instance.
   */
  def invoke(config: Config, instanceRegistry: InstanceRegistry, mirror: Mirror): A = {
    val instanceMirror = mirror.reflect(mirror.reflectModule(moduleSymbol).instance)
    try {
      instanceMirror.reflectMethod(methodSymbol)(config, instanceRegistry).asInstanceOf[A]
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
  }

  override def toString: String = s"${moduleSymbol.name.decodedName}.${methodSymbol.name.decodedName}${
    methodSymbol.paramLists.map {
      list => s"(${list.map {
        param => s"${param.name.decodedName.toString}: ${param.typeSignature.toString}"
      }.mkString(", ")})"
    }.mkString
  }"
}

/**
 * A helper object used to extract apply methods from [[scala.reflect.api.Scopes]] without running
 * into JVM type erasure problems.
 */
private[smartdatalake] object FactoryMethodExtractor extends SmartDataLakeLogger {

  /**
   * Extracts a callable factory method from the provided companion object.
   *
   * @param module          the companion object's symbol.
   * @return                A suitable factory method.
   */
  def extract(module: ModuleSymbol): MethodSymbol = {
    val factoryMethods = module.info.decls.collect {
      case symbol: MethodSymbol if
      symbol.isPublic
        && symbol.returnType =:= symbol.owner.companion.asType.toType // return type is type of provided FQCN.
        && symbol.name.decodedName.toString.equals("fromConfig")
        && (symbol.paramLists.size == 2 && symbol.paramLists.head.size == 1 && symbol.paramLists.last.size == 1
        && symbol.paramLists.head.head.typeSignature =:= typeOf[Config])
        && symbol.paramLists.last.head.typeSignature =:= typeOf[InstanceRegistry]
      => symbol
    }.toSeq

    require(factoryMethods.nonEmpty, s"No suitable factory method found on module '${module.name.decodedName.toString}'.")

    factoryMethods.head
  }
}
