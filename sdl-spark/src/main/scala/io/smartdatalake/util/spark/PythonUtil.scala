/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.util.spark

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.python.PythonHelper
import org.apache.spark.python.PythonHelper.SparkEntryPoint
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

private[smartdatalake] object PythonUtil extends SmartDataLakeLogger {

  /**
   * Execute python code within a given Spark context/session.
   *
   * @param code python code as string.
   *                   The SparkContext is available as "sc" and SparkSession as "session".
   * @param entryPointObj py4j gateway entrypoint java object available in python code as gateway.entry_point.
   *                      This is used to transfer SparkContext to python and can hold additional custom parameters.
   *                      entryPointObj must at least implement trait SparkEntryPoint.
   *
   * The Python interpreter is taken from the Spark configuration `spark.pyspark.driver.python` or
   * `spark.pyspark.python`, otherwise from the environment variables PYSPARK_DRIVER_PYTHON or PYSPARK_PYTHON, and
   * otherwise `python3` is used. `spark.pyspark.python` is set from Environment.pythonPath (environment variable
   * SDL_PYTHON_PATH) when SDLB creates the Spark session.
   */
  def execPythonSparkCode[T<:PythonSparkEntryPoint](entryPointObj: T, code: String): Unit = {
    // Environment.pythonPath can only be applied to Spark sessions created by SDLB
    Environment.pythonPath.foreach { pythonPath =>
      val conf = entryPointObj.session.sparkContext.getConf
      val sparkPython = conf.getOption("spark.pyspark.driver.python").orElse(conf.getOption("spark.pyspark.python"))
      if (!sparkPython.contains(pythonPath)) logger.warn(s"Environment.pythonPath is set to $pythonPath, but the Spark session" +
        s" uses ${sparkPython.map(p => s"$p as Python interpreter").getOrElse("the environment variables PYSPARK_DRIVER_PYTHON or PYSPARK_PYTHON")}." +
        " Environment.pythonPath can only be applied to Spark sessions created by SDLB.")
    }
    PythonHelper.exec(entryPointObj, mainInitCode + sys.props("line.separator") + code)
  }

  // python spark gateway init code
  private val mainInitCode =
    """
      |from pyspark.java_gateway import launch_gateway
      |from pyspark.context import SparkContext
      |from pyspark.conf import SparkConf
      |from pyspark.sql.session import SparkSession
      |from pyspark.sql import DataFrame
      |
      |# Initialize python spark session from java spark context.
      |# The java spark context is set as entrypoint for the py4j gateway.
      |gateway = launch_gateway()
      |entryPoint = gateway.entry_point
      |javaSparkContext = entryPoint.getJavaSparkContext()
      |sparkConf = SparkConf(_jvm=gateway.jvm, _jconf=javaSparkContext.getConf())
      |sc = SparkContext(conf=sparkConf, gateway=gateway, jsc=javaSparkContext)
      |session = SparkSession(sc, entryPoint.session())
      |options = entryPoint.getOptions()
      |print("python spark session initialized (sc, session)")
      |# Unregister python accumulator to avoid "java.net.ConnectException: Connection refused: connect" by PythonAccumulatorV2
      |# This happens as we call python from java and not java from python as it would be normal with pyspark.
      |# Our python server accumulator update server is already closed when the accumulator wants to send its updates to python.
      |# see also initialization in https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/python/pyspark/context.py#L213
      |def ref_scala_object(object_name):
      |  clazz = gateway.jvm.java.lang.Class.forName(object_name+"$")
      |  ff = clazz.getDeclaredField("MODULE$")
      |  return ff.get(None)
      |_accumulatorContext = ref_scala_object("org.apache.spark.util.AccumulatorContext")
      |_accId = sc._javaAccumulator.id()
      |_accumulatorContext.remove(_accId)
      |sc._javaAccumulator = None
      |""".stripMargin

  /** Dedent multiline strings by removing common leading spaces */
  def dedent(code: String): String = {
    val lines = code.stripMargin.linesIterator.toList
    val nonEmptyLines = lines.filter(line => line.trim.nonEmpty)
    val minIndentLength = if (nonEmptyLines.isEmpty) 0 else nonEmptyLines.map(_.segmentLength(c => c == ' ' || c == '\t')).min
    val dedentedLines = lines.map(_.drop(minIndentLength))
    dedentedLines.mkString(System.lineSeparator())
  }

}

class PythonSparkEntryPoint(override val session: SparkSession, options: Map[String,String] = Map()) extends SparkEntryPoint {
  // HashMap is transformed into Python dictionary by py4j
  def getOptions: java.util.HashMap[String,String] = new java.util.HashMap(options.asJava)
}

/**
 * Exception is thrown if the Python transformation can not be executed correctly
 */
private[smartdatalake] class PythonTransformationException(msg: String, throwable: Throwable) extends RuntimeException(msg, throwable)
