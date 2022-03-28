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

package io.smartdatalake.workflow.action.spark.customlogic

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions.Environment
import org.apache.spark.sql.expressions.UserDefinedFunction


/**
 * Configuration to register a UserDefinedFunction in the spark session of SmartDataLake.
 *
 * @param className fully qualified class name of class implementing SparkUDFCreator interface. The class needs a constructor without parameters.
 * @param options Options are passed to SparkUDFCreator apply method.
 */
case class SparkUDFCreatorConfig(className: String, options: Option[Map[String,String]] = None) {
  // instantiate SparkUDFCreator
  private[smartdatalake] val creator: SparkUDFCreator = try {
    val clazz = Environment.classLoader.loadClass(className)
    val constructor = clazz.getConstructor()
    constructor.newInstance().asInstanceOf[SparkUDFCreator]
  } catch {
    case e: NoSuchMethodException => throw ConfigurationException(s"SparkUDFCreatorConfig class $className needs constructor without parameters: ${e.getMessage}", Some("globalConfig.sparkUDFs"), e)
    case e: Exception => throw ConfigurationException(s"Cannot instantiate SparkUDFCreatorConfig class $className: ${e.getMessage}", Some("globalConfig.sparkUDFs"), e)
  }
  private[smartdatalake] def getUDF: UserDefinedFunction = creator.get(options.getOrElse(Map()))
}

/**
 * Interface to create a UserDefinedFunction object to be registered as udf.
 */
trait SparkUDFCreator extends Serializable {

  /**
   * Function that returns a Spark UserDefinedFunction object to be registered as udf.
   * @param options list of options defined in the configuration
   */
  def get(options: Map[String,String]): UserDefinedFunction
}
