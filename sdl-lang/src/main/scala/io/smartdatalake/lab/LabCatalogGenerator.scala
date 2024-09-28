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
import io.smartdatalake.workflow.action.{Action, CustomDataFrameAction, DataFrameOneToOneActionImpl}
import io.smartdatalake.workflow.dataobject.{CanCreateSparkDataFrame, DataObject}
import org.apache.commons.io.FileUtils
import scopt.OptionParser

import java.io.File
import java.nio.file.{Files, Paths, StandardOpenOption}

/**
 * Configuration for the LabCatalogGenerator
 *
 * @param configPaths SDLB configuration paths/files
 * @param srcDirectory directory where generated sources will be written
 * @param packageName package name for generated classes, default is io.smartdatalake.generated
 * @param dataObjectCatalogClassName class name for DataObject catalog
 * @param actionCatalogClassName class name for Action catalog
 * @param additionalSrcPath optional path of additional sources to be copied into srcDirectory for 2nd compilation step
 */
case class LabCatalogGeneratorConfig(configPaths: Seq[String] = null, srcDirectory: String = null, packageName: String = "io.smartdatalake.generated", dataObjectCatalogClassName: String = "DataObjectCatalog", actionCatalogClassName: String = "ActionCatalog", additionalSrcPath: Option[String] = None)

/**
 * Command line interface to generate a scala files that serve as catalog for SmartDataLakeBuilderLab.
 * For now a catalog for DataObjects is created, but could be extended to Actions in the future.
 *
 * The compilation of the scala file has to be added in the build process of the SDLB application as a second compilation phase
 * because it needs to parse the configuration, incl. potential transformers defined.
 * In Maven this can be done by defining the following additional plugins and adding `sdl-lang` as additional project dependency:
 * ```
 *      <profile>
 *          <id>generate-catalog</id>
 *          <build>
 *              <plugins>
 *                  <!-- generate catalog scala code. -->
 *                  <plugin>
 *                      <groupId>org.codehaus.mojo</groupId>
 *                      <artifactId>exec-maven-plugin</artifactId>
 *                      <version>3.1.0</version>
 *                      <executions>
 *                          <execution>
 *                              <id>generate-catalog</id>
 *                              <phase>prepare-package</phase>
 *                              <goals><goal>java</goal></goals>
 *                              <configuration>
 *                                  <mainClass>io.smartdatalake.lab.LabCatalogGenerator</mainClass>
 *                                  <arguments>
 *                                      <argument>--config</argument><argument>./config,./envConfig/dev.conf</argument>
 *                                      <argument>--srcDirectory</argument><argument>./src/main/scala-generated</argument>
 *                                      <argument>--packageName</argument><argument>io.smartdatalake.generated</argument>
 *                                  </arguments>
 *                                  <classpathScope>compile</classpathScope>
 *                              </configuration>
 *                          </execution>
 *                      </executions>
 *                  </plugin>
 *                  <!-- Compiles generated Scala sources. -->
 *                  <plugin>
 *                      <groupId>net.alchim31.maven</groupId>
 *                      <artifactId>scala-maven-plugin</artifactId>
 *                      <executions>
 *                          <!-- add additional execution to compile generated catalog (see id generate-catalog) -->
 *                          <execution>
 *                              <id>compile-catalog</id>
 *                              <phase>prepare-package</phase>
 *                              <goals>
 *                                  <goal>compile</goal>
 *                              </goals>
 *                              <configuration>
 *                                  <sourceDir>./src/main/scala-generated</sourceDir>
 *                              </configuration>
 *                          </execution>
 *                      </executions>
 *                  </plugin>
 *              </plugins>
 *          </build>
 *      </profile>
 * ```
 */
object LabCatalogGenerator extends SmartDataLakeLogger {

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
    opt[String]('d', "dataObjectCatalogClassName")
      .optional()
      .action((value, c) => c.copy(dataObjectCatalogClassName = value))
      .text("Class name of scala class to create. Default: DataObjectCatalog")
    opt[String]('a', "actionCatalogClassName")
      .optional()
      .action((value, c) => c.copy(actionCatalogClassName = value))
      .text("Class name of scala class to create. Default: ActionCatalog")
    opt[String]('s', "additionalSrcPath")
      .optional()
      .action((value, c) => c.copy(additionalSrcPath = Some(value)))
      .text("Optional path of additional sources to be copied into srcDirectory for 2nd compilation step")
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
        generateCatalogs(config)
      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  def generateCatalogs(config: LabCatalogGeneratorConfig): Unit = {
    // parse config
    val (registry, _) = ConfigToolbox.loadAndParseConfig(config.configPaths)

    // write scala files
    val dataObjectCatalogClassDef = generateDataObjectCatalogClass(config.packageName, config.dataObjectCatalogClassName, registry)
    createCatalogScalaFile(config.srcDirectory, config.packageName, config.dataObjectCatalogClassName, dataObjectCatalogClassDef)
    val actionCatalogClassDef = generateActionCatalogClass(config.packageName, config.actionCatalogClassName, registry)
    createCatalogScalaFile(config.srcDirectory, config.packageName, config.actionCatalogClassName, actionCatalogClassDef)

    // copy additional sources
    config.additionalSrcPath.foreach{ path =>
      FileUtils.copyDirectory(new File(path), new File(config.srcDirectory))
    }
  }

  def createCatalogScalaFile(srcDir: String, packageName:String, className: String, classDef: String): Unit = {
    val filename = s"$srcDir/${packageName.split('.').mkString("/")}/$className.scala"

    logger.info(s"Writing generated $className java source code to file ${filename}")
    val path = Paths.get(filename)
    Files.createDirectories(path.getParent)
    Files.write(path, classDef.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  def generateDataObjectCatalogClass(packageName:String, className: String, registry: InstanceRegistry): String = {
    val entries = registry.getDataObjects.sortBy(_.id.id).flatMap {
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


  def generateActionCatalogClass(packageName:String, className: String, registry: InstanceRegistry): String = {
    val entries = registry.getActions.sortBy(_.id.id).flatMap {
      case x: Action with CustomDataFrameAction =>
        Some(s"""lazy val ${DataFrameUtil.strToLowerCamelCase(x.id.id)} = LabSparkDfsActionWrapper(registry.get[${x.getClass.getName}](ActionId("${x.id.id}")), context)""")
      case x: Action with DataFrameOneToOneActionImpl =>
        Some(s"""lazy val ${DataFrameUtil.strToLowerCamelCase(x.id.id)} = LabSparkDfActionWrapper(registry.get[${x.getClass.getName}](ActionId("${x.id.id}")), context)""")
      case x =>
        logger.info(s"No catalog entry created for ${x.id} of type ${x.getClass.getSimpleName}, as it does not implement DataFrameActionImpl")
        None
    }
    s"""
       |package $packageName
       |import io.smartdatalake.config.InstanceRegistry
       |import io.smartdatalake.config.SdlConfigObject.ActionId
       |import io.smartdatalake.workflow.ActionPipelineContext
       |import io.smartdatalake.lab.LabSparkDfsActionWrapper
       |import io.smartdatalake.lab.LabSparkDfActionWrapper
       |case class $className(registry: InstanceRegistry, context: ActionPipelineContext) {
       |${entries.map("  "+_).mkString("\n")}
       |}
    """.stripMargin
  }
}