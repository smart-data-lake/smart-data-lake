/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action.customlogic

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.{PythonSparkEntryPoint, PythonUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

/**
 * Configuration to register a Python UDF in the spark session of SmartDataLake.
 * Define a python function with type hints i python code and register it in global configuration.
 * The name of the function must match the name you use to declare it in GlobalConf.
 * The Python function can then be used in Spark SQL expressions.
 *
 * @param pythonFile Optional pythonFile to use for python UDF.
 * @param pythonCode Optional pythonCode to use for python UDF.
 * @param options Options are available in your python code as variable options.
 */
case class PythonUDFCreatorConfig(pythonFile: Option[String] = None, pythonCode: Option[String] = None, options: Option[Map[String,String]] = None) {
  require(pythonFile.isDefined || pythonCode.isDefined, "Either pythonFile or pythonCode must be defined for SparkUDFCreatorConfig")

  private val functionCode = {
    implicit val defaultHadoopConf: Configuration = new Configuration()
    pythonFile.map(HdfsUtil.readHadoopFile).orElse(pythonCode.map(_.stripMargin)).get
  }

  private[smartdatalake] def registerUDF(functionName: String, session: SparkSession): Unit = {
    val entryPoint = new PythonSparkEntryPoint(session, options.getOrElse(Map()))
    val registrationCode =
      s"""
         |session.udf.register("$functionName", $functionName)
         |print()
         |print("python udf $functionName registered")
         |""".stripMargin
    PythonUtil.execPythonSparkCode( entryPoint, functionCode + sys.props("line.separator") + registrationCode)
  }
}
