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

package io.smartdatalake.meta.state

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionDAGRunState.checkStateFormatVersionAndMigrate
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.jackson.{JsonMethods, prettyJson}
import scopt.OptionParser

case class StateMigratorConfig(statePath: Option[String] = None)

object StateMigrator extends SmartDataLakeLogger {

  val appType: String = getClass.getSimpleName.replaceAll("\\$$", "") // remove $ from object name and use it as appType

  protected val parser: OptionParser[StateMigratorConfig] = new OptionParser[StateMigratorConfig](appType) {
    override def showUsageOnError: Option[Boolean] = Some(true)
    opt[String]('s', "state-path")
      .required()
      .action((value, c) => c.copy(statePath = Some(value)))
      .text("Path with run state files to migrate.")
    help("help").text("Display the help text.")
  }

  /**
   * Migrates state files in a given folder to the latest SDLB state format version.
   */
  def main(args: Array[String]): Unit = {
    val config = StateMigratorConfig()
    // Parse all command line arguments
    parser.parse(args, config) match {
      case Some(config) =>
         migrateStateFiles(new Path(config.statePath.get))
      case None =>
        logAndThrowException(s"Aborting ${appType} after error", new ConfigurationException("Couldn't set command line parameters correctly."))
    }
  }

  def migrateStateFiles(statePath: Path): Unit = {
    val defaultHadoopConf: Configuration = new Configuration()
      implicit val filesystem: FileSystem = statePath.getFileSystem(defaultHadoopConf)
      logger.info(s"Searching state files in $statePath")
      RemoteIteratorWrapper(filesystem.listStatusIterator(statePath))
      .foreach {
        // recurse into subdirectories
        case p if p.isDirectory =>
          migrateStateFiles(p.getPath)
        // migrate all json files
        case p if p.getPath.getName.endsWith(".json") =>
          migrateStateFile(p.getPath)
        // ignore the rest
        case x => logger.info(s"ignoring file ${x.getPath}")
      }
  }

  def migrateStateFile(file: Path)(implicit filesystem: FileSystem): Unit = {
    val jsonStr = HdfsUtil.readHadoopFile(file)
    val jValue = JsonMethods.parse(jsonStr)
    val migratedJValue = checkStateFormatVersionAndMigrate(jValue)
    if (migratedJValue.nonEmpty) {
      HdfsUtil.writeHadoopFile(file, prettyJson(migratedJValue.get))
    }
  }
}
