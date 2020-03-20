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
package io.smartdatalake.config

import java.io.InputStreamReader

import com.typesafe.config.{Config, ConfigFactory}
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object ConfigLoader extends SmartDataLakeLogger {

  final val configFileExtensions: Set[String] = Set("conf", "json", "properties")

  /**
   * Load the configuration from classpath using the default behavior of typesafe Config.
   *
   * The order of loading is:
   *
   * 1. system properties
   * 2. all application.conf files on the classpath
   * 3. all application.json files on the classpath
   * 4. all application.properties files on the classpath
   * 5. all reference.conf files on the classpath.
   *
   * Configuration values take precedence in that order.
   *
   * @see [[https://github.com/lightbend/config#standard-behavior]] for more details.
   *
   * @return the parsed, combined, and resolved configuration.
   */
  def loadConfigFromClasspath: Config = ConfigFactory.load()

  /**
   * Load the configuration from the file system location `configLocation`.
   *
   * If `configLocation` is a directory, it is traversed in breadth-first search (BFS) order provided by HDFS file system.
   * Only file names ending in '.conf', '.json', or '.properties' are processed.
   * All processed config files are merged and files encountered later overwrite settings in files processed earlier.
   *
   * The order of loading is:
   *
   * 1. system properties
   * 2. all application.conf files in BFS order
   * 3. all application.json files in BFS order
   * 4. all application.properties files in BFS order
   *
   * Configuration values take precedence in that order.
   *
   * The file extension of any encountered file forces a corresponding config syntax:
   *
   * - '.conf' forces HOCON syntax
   * - '.json' forces JSON syntax
   * - '.properties' forces Java properties syntax
   *
   * @see [[com.typesafe.config.ConfigSyntax]]
   * @param configLocation      a configuration file or directory containing configuration files.
   * @return                    a resolved [[Config]] merged from all found configuration files.
   */
  def loadConfigFromFilesystem(configLocation: String): Config = {
    val hadoopPath = HdfsUtil.addHadoopDefaultSchemaAuthority(new Path(configLocation))
    logger.info(s"Loading configuration from filesystem location: ${hadoopPath.toUri}.")
    implicit val fileSystem: FileSystem = HdfsUtil.getHadoopFs(hadoopPath)

    require(fileSystem.exists(hadoopPath), s"$hadoopPath is not a valid location.")

    val configFileIndex = filesInBfsOrderByExtension(hadoopPath)
    logger.debug(s"Configuration file index:\n${configFileIndex.map(e => s"\t${e._1} -> ${e._2.mkString(", ")}").mkString("\n")}")

    val confFiles = configFileIndex("conf")
    val jsonFiles = configFileIndex("json")
    val propertyFiles = configFileIndex("properties")
    if (confFiles.isEmpty && jsonFiles.isEmpty && propertyFiles.isEmpty) {
      logger.warn(s"$hadoopPath does not contain valid configuration files. " +
        s"Ensure the configuration files have one of the following extensions: ${configFileExtensions.map(ext => s".$ext").mkString(", ")}")
    }
    //read file extensions in the same order as typesafe config
    //system properties take precedence
    val config = ConfigFactory.systemProperties()

    mergeWithHdfsFiles(propertyFiles,
      mergeWithHdfsFiles(jsonFiles,
        mergeWithHdfsFiles(confFiles, config)
      )
    ).resolve()
  }

  /**
   * Parse a [[Config]] using HDFS [[FileSystem]] API.
   *
   * A configuration is parsed from a supplied list of configuration locations such that configurations at the end of the
   * list overwrite configurations at the beginning of the list.
   *
   * The parsed configs are then merged with the supplied `config` and settings in `config` take precendence.
   *
   * @param configFilePaths   a list of [[Path]]s corresponding to the HDFS locations of  configuration files.
   * @param config            a config with which to merge the configs from `configFilePaths`.
   * @param fs                the configured filesystem handle
   * @return                  a merged config.
   */
  private def mergeWithHdfsFiles(configFilePaths: Seq[Path], config: Config)(implicit fs: FileSystem): Config = {
    if (configFilePaths.nonEmpty) {
      config.withFallback(configFilePaths.map { path =>
        val reader = new InputStreamReader(fs.open(path))
        try {
          val parsedConfig = ConfigFactory.parseReader(reader)
          if (parsedConfig.isEmpty) {
            logger.warn(s"Config parsed from ${path.toString} is empty!")
          }
          parsedConfig
        } catch {
          case exception: Throwable =>
            logger.error(s"Failed to parse config from ${path.toString}", exception)
            throw exception
        } finally {
          reader.close()
        }
      }.reduceRight((c1, c2) => c2.withFallback(c1)))
    } else {
      config
    }
  }
  
  /**
   * Collect readable files with valid config file extensions from HDFS in BFS order indexed by file extension.
   *
   * This is an internal method to create a utility data structure.
   *
   * @param rootPath      root [[Path]] pointing to a configuration file or a directory from where the traversal starts.
   * @param fs            a configured HDFS [[FileSystem]] handle.
   * @return              a map with BFS ordered file lists for each file extension in [[ConfigLoader.configFileExtensions]].
   */
  private def filesInBfsOrderByExtension(rootPath: Path)(implicit fs: FileSystem): Map[String, Seq[Path]] = {
    val traversalQueue = mutable.Queue[Path](rootPath)
    val readableFileIndex = configFileExtensions.map((_, new mutable.MutableList[Path]())).toMap

    while (traversalQueue.nonEmpty) {
      val nextFile = traversalQueue.dequeue()
      if (fs.isDirectory(nextFile)) {
        Try(fs.listLocatedStatus(nextFile)) match {
          case Failure(exception) =>
            logger.warn(s"Failed to list directory content of ${nextFile.toString}.", exception)
          case Success(children) =>
            while (children.hasNext) {
              traversalQueue += children.next().getPath
              logger.trace(s"Found '${traversalQueue.last.getName}' in directory $nextFile.")
            }
        }
      } else if (fs.isFile(nextFile) && hasPermission(nextFile, FsAction.READ)) {
        val fileExtension = nextFile.getName.split('.').last
        if (configFileExtensions.contains(fileExtension) && !nextFile.getName.equals("log4j.properties")) {
          logger.trace(s"'$nextFile' is a configuration file.")
          readableFileIndex(fileExtension) += nextFile
        }
      }
    }
    readableFileIndex
  }

  /**
   * Check if the current user has permission to execute `action` on HDFS path `path`.
   *
   * @param path        a HDFS path
   * @param action      an action like [[FsAction.READ]]
   * @param fs          a configure HDFS [[FileSystem]] handle.
   * @return            `true` if the current user has permission for `action` on `path`. `false` otherwise.
   */
  private def hasPermission(path: Path, action: FsAction)(implicit fs: FileSystem): Boolean = {
    try {
      if (isWindowsOS) true // Workaround: checking permissions on windows doesn't work with Hadoop (version 2.6)
      else {
        fs.access(path, action)
        true
      }
    } catch {
      case t: Throwable =>
        logger.warn(s"Cannot access $path: ${t.getClass.getSimpleName}: ${t.getMessage}")
        false
    }
  }

  private def isWindowsOS = {
    sys.env.get("OS").exists(_.toLowerCase.startsWith("windows"))
  }
}
