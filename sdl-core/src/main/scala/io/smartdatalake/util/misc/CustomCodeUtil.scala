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
package io.smartdatalake.util.misc

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.action.spark.customlogic.NotFoundError
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDsNTo1Transformer.tolerantGet
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col

import java.io.{FileNotFoundException, InputStream}
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import scala.util.{Failure, Success, Try}

/**
  * Helper functions to work with custom code,
  * either compiled at runtime or from classes in classpath
  */
private[smartdatalake] object CustomCodeUtil {

  private val runtimeMirror = scala.reflect.runtime.currentMirror

  // get Scala Toolbox to compile code at runtime
  private lazy val tb = runtimeMirror.mkToolBox()

  /**
   * Compiling Scala Source Code into Object of Type T
   *
   * @param code scala code to compile
   * @tparam T Type of object returned by code (typically a function)
   * @return object returned by code
   */
  // TODO: currently throws exception when using hadoop filesystem -> assertion failed: no symbol could be loaded from interface org.apache.hadoop.classification.InterfaceAudience$Public in object InterfaceAudience with name Public and classloader scala.reflect.internal.util.AbstractFileClassLoader@6c2be147
  def compileCode[T](code: String): T = {
    // compile and execute
    val compiledCode = Try( tb.eval(tb.parse(code))) match {
      case Success(code) => code
      case Failure(e) => throw new ConfigurationException(s"Error while compiling: "+e.getMessage)
    }
    // cast compiled code to object of expected type and return
    Try(compiledCode.asInstanceOf[T]) match {
      case Success(obj) => obj
      case Failure(e) => throw new ConfigurationException(s"Error while casting compiled code: " +e.getMessage)
    }
  }

  def getClassInstanceByName[T](classname:String): T = {
    val clazz = Environment.classLoader().loadClass(classname)
    require(clazz.getConstructors.exists(con => con.getParameterCount == 0), s"Class $classname needs to have a constructor without parameters!")
    clazz.getConstructor().newInstance().asInstanceOf[T]
  }

  def readResourceFile( filename:String ) : String = {
    val stream : InputStream = Option(ClassLoader.getSystemClassLoader.getResourceAsStream(filename))
      .getOrElse(throw new FileNotFoundException(filename))
    val source = scala.io.Source.fromInputStream(stream)
    val content = source.getLines().mkString(sys.props("line.separator"))
    source.close
    // return value
    content
  }

  /**
   * Get method symbol with given name from class definition
   */
  def getClassMethodsByName(cls: Class[_], methodName: String): scala.Seq[universe.MethodSymbol] = {
    val mirror = scala.reflect.runtime.currentMirror
    val classType = mirror.classSymbol(cls).toType
    classType.members.filter(_.isMethod).filter(_.name.toString == methodName).map(_.asMethod).toSeq
  }

  /**
   * Extract default values for parameters of a method signature.
   * @param instance class instance of object the method belongs to
   * @param method method symbol to read signature from
   * @return a Map with parameter names and their default values.
   */
  def getMethodParameterDefaultValues(instance: AnyRef, method: universe.MethodSymbol): Map[String, Any] = {
    val instanceMirror = runtimeMirror.reflect(instance)
    val classType = instanceMirror.symbol.toType
    method.paramLists.head.zipWithIndex.flatMap {
      case (p,i) =>
        val parameterName = p.name.toString
        // There is a special method for getting the default value for a given parameter.
        // The method name for default values is by convention, see also https://stackoverflow.com/questions/13812172/how-can-i-create-an-instance-of-a-case-class-with-constructor-arguments-with-no
        val defaultMethod = classType.member(universe.TermName(s"${method.name.toString}$$default$$${i+1}"))
        if (defaultMethod != universe.NoSymbol) {
          val defaultMethodMirror = instanceMirror.reflectMethod(defaultMethod.asMethod)
          Some((parameterName, defaultMethodMirror.apply()))
        } else None
    }.toMap
  }

  /**
   * Dynamically call method on class instance.
   */
  def callMethod[R](instance: Any, methodSymbol: universe.MethodSymbol, args: Seq[Any]): R = {
    val instanceMirror = runtimeMirror.reflect(instance)
    instanceMirror.reflectMethod(methodSymbol).apply(args:_*).asInstanceOf[R]
  }

  /**
   * Extract method parameters with default values through reflection.
   * @param instance: class instance for method to inspect. Needed to get parameter default values.
   * @param method: method symbol to inspect
   */
  def analyzeMethodParameters(instance: Option[AnyRef], method: universe.MethodSymbol): Seq[MethodParameterInfo] = {
    val parameters = method.paramLists.head
    val defaultValues = instance.map(i => getMethodParameterDefaultValues(i, method)).getOrElse(Map())
    parameters.map { p =>
      MethodParameterInfo(p.name.toString, p.typeSignature, defaultValues.get(p.name.toString))
    }
  }
}

case class MethodParameterInfo(name: String, tpe: universe.Type, defaultValue: Option[Any]) {
  def toMap: Map[String, Any] = Seq(Some("name" -> name), Some("type" -> tpe.toString), defaultValue.map("default" -> _)).flatten.toMap
}