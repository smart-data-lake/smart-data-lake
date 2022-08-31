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

import java.io.{FileNotFoundException, InputStream}
import scala.tools.reflect.ToolBox
import scala.util.{Failure, Success, Try}

/**
  * Helper functions to work with custom code,
  * either compiled at runtime or from classes in classpath
  */
private[smartdatalake] object CustomCodeUtil {

  // get Scala Toolbox to compile code at runtime
  private lazy val tb = scala.reflect.runtime.currentMirror.mkToolBox()

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
    val clazz = Environment.classLoader.loadClass(classname)
    assert(clazz.getConstructors.exists(con => con.getParameterCount == 0), s"Class $classname needs to have a constructor without parameters!")
    clazz.getDeclaredConstructor().newInstance().asInstanceOf[T]
  }

  def readResourceFile( filename:String ) : String = {
    val stream : InputStream = Option(ClassLoader.getSystemClassLoader.getResourceAsStream(filename))
      .getOrElse(throw new FileNotFoundException(filename))
    val source = scala.io.Source.fromInputStream(stream)
    val content = source.getLines.mkString(sys.props("line.separator"))
    source.close
    // return value
    content
  }
}
