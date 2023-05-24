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

package io.smartdatalake.app

import io.smartdatalake.app.BuildVersionInfo.buildVersionInfoFilename
import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.misc.{SmartDataLakeLogger, WithResource}
import scopt.OptionParser

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

/**
 * Configuration for writing SDLB version info file
 */
case class VersionInfoWriterConfig(
                                     outputDir: String = null,
                                     version: String = null
                                   )

/**
 * Main class to writing SDLB version info file
 */
object VersionInfoWriter extends SmartDataLakeLogger {

  /**
   * The Parser defines how to extract the options from the command line args.
   */
  private val parser: OptionParser[VersionInfoWriterConfig] = new OptionParser[VersionInfoWriterConfig]("VersionInfoWriter") {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('o', "outputDir")
      .required()
      .action((v, c) => c.copy(outputDir = v))
      .text("Directory to write version info file into")
    opt[String]('v', "version")
      .action((v, c) => c.copy(version = v))
      .text("SDLB Version to write to json file")
    help("help").text("Display the help text.")
  }

  def main(args: Array[String]): Unit = {
    logger.info(s"Starting Version Info Writer")

    val config = VersionInfoWriterConfig()

    // Parse command line
    parser.parse(args, config) match {
      case Some(config) => BuildVersionInfo(config.version).writeBuildVersionInfo(config.outputDir)
      case None => logAndThrowException(s"Error parsing command line parameters", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }
}

case class BuildVersionInfo(version: String, user: String, date: LocalDateTime) extends SmartDataLakeLogger {

  /**
   * Write the SDLB version info properties file to the given output directory.
   * This is called during maven build.
   */
  def writeBuildVersionInfo(outputDir: String): Unit = {
    val versionInfoFile = s"$outputDir/$buildVersionInfoFilename"
    logger.info(s"writing $versionInfoFile")
    val props = new Properties()
    props.setProperty("version", version)
    props.setProperty("user", user)
    props.setProperty("date", date.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
    WithResource.exec(Files.newOutputStream(Paths.get(versionInfoFile), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
      os => props.store(os, "")
    }
  }

  override def toString: String = {
    s"version=$version user=$user date=$date"
  }
}

object BuildVersionInfo extends SmartDataLakeLogger {

  /**
   * Create new build version informations with given version
   */
  def apply(version: String): BuildVersionInfo = {
    BuildVersionInfo(version, sys.env.get("USERNAME").orElse(sys.env.get("USER")).getOrElse("unknown"), LocalDateTime.now)
  }

  /**
   * Read the SDLB version info properties from the corresponding classpath resource.
   * @return: version, user, date
   */
  def readBuildVersionInfo: Option[BuildVersionInfo] = {
    val resourceStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(buildVersionInfoFilename)
    if (resourceStream == null) {
      logger.warn(s"Could not find resource $buildVersionInfoFilename")
      None
    } else {
      WithResource.exec(resourceStream) {
        stream =>
          val props = new Properties()
          props.load(stream)
          Some(BuildVersionInfo(props.getProperty("version"), props.getProperty("user"), LocalDateTime.parse(props.getProperty("date"), DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
      }
    }
  }

  private val buildVersionInfoFilename = "sdlb-version-info.properties"
}