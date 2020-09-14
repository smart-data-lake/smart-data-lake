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

import org.apache.spark.python.PythonHelper
import org.apache.spark.python.PythonHelper.SparkEntryPoint
import org.apache.spark.sql.SparkSession

private[smartdatalake] object PythonUtil {

  /**
   * Execute python code within a given Spark context/session.
   *
   * @param code python code as string.
   *                   The SparkContext is available as "sc" and SparkSession as "session".
   * @param entryPointObj py4j gateway entrypoint java object available in python code as gateway.entry_point.
   *                      This is used to transfer SparkContext to python and can hold additional custom parameters.
   *                      entryPointObj must at least implement trait SparkEntryPoint.
   */
  def execPythonTransform[T<:SparkEntryPoint](entryPointObj: T, code: String): Unit = {
    PythonHelper.exec(entryPointObj, mainInitCode + sys.props("line.separator") + code)
  }

  // python spark gateway init code
  val mainInitCode =
    """
      |from pyspark.java_gateway import launch_gateway
      |from pyspark.context import SparkContext
      |from pyspark.conf import SparkConf
      |from pyspark.sql.session import SparkSession
      |from pyspark.sql import SQLContext
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
      |sqlContext = SQLContext(sc, session, entryPoint.getSQLContext())
      |print("python spark session initialized (sc, session, sqlContext)")
      |""".stripMargin
}

class DfTransformerSparkEntryPoint(override val session: SparkSession, options: Map[String,String] = Map()) extends SparkEntryPoint
