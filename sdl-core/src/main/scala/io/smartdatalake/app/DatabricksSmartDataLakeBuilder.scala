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
package io.smartdatalake.app

import io.smartdatalake.config.ConfigurationException

/**
 * Databricks Smart Data Lake Command Line Application.
 *
 * As there is an old version of config-*.jar deployed on Databricks, this special App uses a ChildFirstClassLoader to override it in the classpath.
 */
object DatabricksSmartDataLakeBuilder extends SmartDataLakeBuilder {

  /**
   * Entry-Point of the application.
   *
   * @param args Command-line arguments.
   */
  def main(args: Array[String]): Unit = {
    logger.info(s"Start programm $appType")

    val config = initConfigFromEnvironment.copy (
      overrideJars = Some(Seq("config-1.3.4.jar"))
    )

    parseCommandLineArguments(args, config) match {
      case Some(c) =>

        val jars = c.overrideJars.getOrElse (throw new ConfigurationException(s"override-jars option must be specified for $appType"))

        // get a DefaultSmartDataLakeBuilder instance from ChildFirstClassLoader
        // use reflection to avoid ClassCastException because of different ClassLoaders
        val classLoader = AppUtil.getChildFirstClassLoader(jars)
        val className = classOf[DefaultSmartDataLakeBuilder].getName
        val clazz = classLoader.loadClass(className)
        val smartDataLakeBuilder = clazz.getDeclaredConstructor().newInstance()
        val runMethodName = "parseAndRun"
        val runMethod = clazz.getDeclaredMethods.find (m => m.getName == runMethodName && m.getParameterCount == 2)
        .getOrElse (throw new IllegalStateException(s"'$runMethodName' method not found for class $className") )
        runMethod.invoke(smartDataLakeBuilder, args, Boolean.box (true) )
      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }
}
