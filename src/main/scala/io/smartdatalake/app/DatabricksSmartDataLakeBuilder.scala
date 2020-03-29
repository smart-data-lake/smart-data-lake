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
    logger.info(s"Start programm ${appType}")

    val config = initConfigFromEnvironment.copy (
      overrideJars = Some(Seq("config-1.3.4.jar"))
    )

    val c = parseCommandLineArguments(args, config)

    val jars = c.flatMap(_.overrideJars)
      .getOrElse(throw new ConfigurationException("override-jars option must be specified for DatabricksSmartDataLakeBuilder"))

    // get a DefaultSmartDataLakeBuilder instance from ChildFirstClassLoader
    // use reflection to avoid ClassCastException because of different ClassLoaders
    val classLoader = AppUtil.getChildFirstClassLoader(jars)
    val clazz = classLoader.loadClass(classOf[DefaultSmartDataLakeBuilder].getName)
    val smartDataLakeBuilder = clazz.newInstance
    val runMethod = clazz.getDeclaredMethods.find( m => m.getName == "parseAndRun" && m.getParameterCount == 2)
      .getOrElse( throw new IllegalStateException("'parseAndRun' method not found for class DefaultSmartDataLakeBuilder"))
    runMethod.invoke(smartDataLakeBuilder, args, Boolean.box(true))
  }
}
