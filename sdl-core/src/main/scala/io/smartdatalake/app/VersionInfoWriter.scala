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

import io.smartdatalake.app.BuildVersionInfo.getFilename
import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import scopt.OptionParser

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.language.postfixOps
import scala.util.Using

/**
 * Configuration for writing SDLB version info file
 */
case class VersionInfoWriterConfig(
                                     outputDir: String = null,
                                     version: String = null,
                                     app: Boolean = false
                                   )

/**
 * Main class to write SDLB or App version info file.
 * This is intended to be executed during build phase with maven.
 *
 * Version information can then be read during runtime using
 * {{{BuildVersionInfo.readBuildVersionInfo(app=true)}}}
 *
 * Example maven configuration:
 * {{{
 * <!-- generate app-version-info.properties file -->
 * <plugin>
 *     <groupId>org.codehaus.mojo</groupId>
 *     <artifactId>exec-maven-plugin</artifactId>
 *     <executions>
 *         <execution>
 *             <id>generate-version-info</id>
 *             <phase>prepare-package</phase>
 *             <goals>
 *                 <!-- use exec instead of java, so it runs in a separate jvm from mvn -->
 *                 <goal>exec</goal>
 *             </goals>
 *             <configuration>
 *                 <executable>java</executable>
 *                 <longClasspath>true</longClasspath>
 *                 <arguments>
 *                     <argument>-Dlog4j.configurationFile=log4j2.yml</argument>
 *                     <argument>-classpath</argument>
 *                     <classpath />
 *                     <argument>io.smartdatalake.custom.VersionInfoWriter</argument>
 *                     <argument>--outputDir</argument>
 *                     <argument>${project.build.outputDirectory}</argument>
 *                     <argument>--version</argument>
 *                     <argument>${project.version}</argument>
 *                     <argument>--app</argument>
 *                     <argument>true</argument>
 *                 </arguments>
 *             <classpathScope>compile</classpathScope>
 *             </configuration>
 *         </execution>
 *     </executions>
 * </plugin>
 * }}}
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
      .text("Version to write to json file")
    opt[String]('a', "app")
      .action((v, c) => c.copy(app = v.toBoolean))
      .text("If true, create app-version-info.properties, otherwise sdlb-version-info.properties")
    help("help").text("Display the help text.")
  }

  def main(args: Array[String]): Unit = {
    logger.info(s"Starting Version Info Writer")

    val config = VersionInfoWriterConfig()

    // Parse command line
    parser.parse(args, config) match {
      case Some(config) => BuildVersionInfo(config.version).writeBuildVersionInfo(config.outputDir, config.app)
      case None => logAndThrowException(s"Error parsing command line parameters", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }
}

case class BuildVersionInfo(version: String, user: String, date: LocalDateTime, revision: String) extends SmartDataLakeLogger {

  /**
   * Write the SDLB version info properties file to the given output directory.
   * This is called during maven build.
   */
  def writeBuildVersionInfo(outputDir: String, app: Boolean): Unit = {
    val versionInfoFile = s"$outputDir/${getFilename(app)}"
    logger.info(s"writing $versionInfoFile")
    val props = new Properties()
    props.setProperty("version", version)
    props.setProperty("user", user)
    props.setProperty("date", date.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
    props.setProperty("revision", revision)
    Using.resource(Files.newOutputStream(Paths.get(versionInfoFile), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
      os => props.store(os, "")
    }
  }

  def entries(): Seq[(String,Any)] = {
    Seq("version" -> version, "user" -> user, "date" -> date, "revision" -> revision)
  }

  override def toString: String = {
    s"version=$version user=$user date=$date revision=$revision"
  }
}

object BuildVersionInfo extends SmartDataLakeLogger {

  /**
   * Create new build version informations with given version
   */
  def apply(version: String): BuildVersionInfo = {
    BuildVersionInfo(version, getUser, LocalDateTime.now, getRevision)
  }

  /**
   * Read the version info properties from the corresponding classpath resource.
   */
  private def readBuildVersionInfo(app: Boolean = false): Option[BuildVersionInfo] = {
    val filename = getFilename(app)
    val resourceStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(filename)
    if (resourceStream == null) {
      logger.warn(s"Could not find resource $filename")
      None
    } else {
      Using.resource(resourceStream) {
        stream =>
          val props = new Properties()
          props.load(stream)
          Some(BuildVersionInfo(props.getProperty("version"), props.getProperty("user"), LocalDateTime.parse(props.getProperty("date"), DateTimeFormatter.ISO_LOCAL_DATE_TIME), props.getProperty("revision")))
      }
    }
  }
  lazy val sdlbVersionInfo: Option[BuildVersionInfo] = readBuildVersionInfo(app=false)
  lazy val appVersionInfo: Option[BuildVersionInfo] = readBuildVersionInfo(app=true)

  def getRevision: String = {
    import sys.process._
    try {
      s"git rev-parse --verify --short=$gitRevisionLength HEAD" !!
    } catch {
      case e: Exception =>
        logger.warn(s"Could not get Git revision number: ${e.getClass.getSimpleName} ${e.getMessage}")
        "unknown"
    }
  }

  def getUser: String = {
    sys.env.get("USERNAME").orElse(sys.env.get("USER")).getOrElse("unknown")
  }

  def getFilename(app: Boolean): String = if (app) appVersionInfoFilename else sdlbVersionInfoFilename

  private val sdlbVersionInfoFilename = "sdlb-version-info.properties"
  private val appVersionInfoFilename = "app-version-info.properties"
  private val gitRevisionLength = 10
}