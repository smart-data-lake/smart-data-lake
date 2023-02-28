/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.lab

import io.smartdatalake.config.{ConfigToolbox, ConfigurationException, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.dataobject.{CanCreateSparkDataFrame, DataObject}
import scopt.OptionParser

import java.nio.file.{Files, Paths, StandardOpenOption}


case class LabCatalogGeneratorConfig(configPaths: Seq[String] = null, srcDirectory: String = null, packageName: String = "io.smartdatalake.generated", className: String = "DataObjectCatalog")

object LabCatalogGenerator extends SmartDataLakeLogger {
  import scala.reflect.runtime.universe._

  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType

  protected val parser: OptionParser[LabCatalogGeneratorConfig] = new OptionParser[LabCatalogGeneratorConfig](appType) {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('c', "config")
      .required()
      .action((value, c) => c.copy(configPaths = value.split(',')))
      .text("One or multiple configuration files or directories containing configuration files for SDLB, separated by comma.")
    opt[String]('s', "srcDirectory")
      .required()
      .action((value, c) => c.copy(srcDirectory = value))
      .text("Source directory where the scala file should be created. Must not include package path.")
    opt[String]('p', "packageName")
      .optional()
      .action((value, c) => c.copy(packageName = value))
      .text("Package name of scala class to create. Default: io.smartdatalake.generated")
    opt[String]('c', "className")
      .optional()
      .action((value, c) => c.copy(className = value))
      .text("Class name of scala class to create. Default: DataObjectCatalog")
    help("help").text("Display the help text.")
  }

  /**
   * Takes as input a SDL Config and exports it as one json document, everything resolved.
   * Additionally a separate file with the mapping of first class config objects to source code origin is created.
   */
  def main(args: Array[String]): Unit = {
    val config = LabCatalogGeneratorConfig()
    // Parse all command line arguments
    parser.parse(args, config) match {
      case Some(config) =>
        generateDataObjectCatalog(config)
      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  def generateDataObjectCatalog(config: LabCatalogGeneratorConfig) = {
    // parse config
    val (registry, _) = ConfigToolbox.loadAndParseConfig(config.configPaths)

    // write scala file
    createDataObjectCatalogScalaFile(config.srcDirectory, config.packageName, config.className, registry)
  }

  def createDataObjectCatalogScalaFile(srcDir: String, packageName:String, className: String, registry: InstanceRegistry): Unit = {
    val filename = s"$srcDir/${packageName.split('.').mkString("/")}/$className.scala"
    val classDef = generateDataObjectCatalogClass(packageName, className, registry)

    logger.info(s"Writing generated DataObjectCatalog java source code to file ${filename}")
    val path = Paths.get(filename)
    Files.createDirectories(path.getParent)
    Files.write(path, classDef.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  def generateDataObjectCatalogClass(packageName:String, className: String, registry: InstanceRegistry): String = {
    val entries = registry.getDataObjects.flatMap {
      case x: DataObject with CanCreateSparkDataFrame =>
        Some(s"""lazy val ${DataFrameUtil.strToLowerCamelCase(x.id.id)} = LabSparkDataObjectWrapper(registry.get[${x.getClass.getName}](DataObjectId("${x.id.id}")), context)""")
      case x =>
        logger.info(s"No catalog entry created for ${x.id} of type ${x.getClass.getSimpleName}, as it does not implement CanCreateSparkDataFrame")
        None
    }
    s"""
    |package $packageName
    |import io.smartdatalake.config.InstanceRegistry
    |import io.smartdatalake.config.SdlConfigObject.DataObjectId
    |import io.smartdatalake.workflow.ActionPipelineContext
    |import io.smartdatalake.lab.LabSparkDataObjectWrapper
    |case class $className(registry: InstanceRegistry, context: ActionPipelineContext) {
    |${entries.map("  "+_).mkString("\n")}
    |}
    """.stripMargin
  }
}