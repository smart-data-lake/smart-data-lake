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
   * @param pythonCode python code as string.
   *                   The SparkContext is available as "sc" and SparkSession as "session".
   * @param entryPointObj py4j gateway entrypoint java object available in python code as gateway.entry_point.
   *                      This is used to transfer SparkContext to python and can hold additional custom parameters.
   *                      entryPointObj must at least implement trait SparkEntryPoint.
   */
  def execPythonTransform[T<:SparkEntryPoint](entryPointObj: T, pythonCode: String): Unit = {

    // load python spark init code
    val initCode = CustomCodeUtil.readResourceFile("pySparkInit.py")

    // exec
    PythonHelper.exec(entryPointObj, initCode + sys.props("line.separator") + pythonCode)
  }

}

class DfTransformerSparkEntryPoint(override val session: SparkSession, options: Map[String,String] = Map()) extends SparkEntryPoint

