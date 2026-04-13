/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.meta.jsonschema

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.json4s.jackson.JsonMethods.pretty
import scopt.OptionParser

import java.nio.file.{Files, Paths, StandardOpenOption}

/**
 * Configuration for exporting SDL configuration schema as json schema
 */
case class JsonSchemaExporterConfig(
                                     filename: String = null,
                                     version: Option[String] = None
                                   )

/**
 * Main class to export SDL configuration schema as json schema
 */
object JsonSchemaExporter extends SmartDataLakeLogger {

  // read version from package jar-manifest (not defined if project is executed in IntellJ)
  val appVersion: String = Option(getClass.getPackage.getImplementationVersion).getOrElse("develop")
  val appType: String = getClass.getSimpleName.replaceAll("\\$$","") // remove $ from object name and use it as appType

  /**
   * The Parser defines how to extract the options from the command line args.
   */
  private val parser: OptionParser[JsonSchemaExporterConfig] = new OptionParser[JsonSchemaExporterConfig]("JsonSchemaExporter") {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('f', "filename")
      .required()
      .action((v, c) => c.copy(filename = v))
      .text("Filename to write json schema into")
    opt[String]('v', "version")
      .action((v, c) => c.copy(version = Some(v)))
      .text("SDL Version to write to json file")
    help("help").text("Display the help text.")
  }

  def main(args: Array[String]): Unit = {
    logger.info(s"Starting Json Schema Exporter")

    val config = JsonSchemaExporterConfig()

    // Parse command line
    parser.parse(args, config) match {

      case Some(config) =>

        // create schema
        val jsonRootDef = JsonSchemaUtil.createSdlSchema(config.version.getOrElse(appVersion))
        val jsonRoot = jsonRootDef.toJson
        val jsonRootString = pretty(jsonRoot)

        // write file
        logger.info(s"Writing schema to file ${config.filename}")
        Files.write(Paths.get(config.filename), jsonRootString.getBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING )

      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }
}
