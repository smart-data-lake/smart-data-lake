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

import com.typesafe.config.{Config, ConfigFactory}
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}

import java.io.InputStreamReader
import scala.collection.AbstractIterator
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
   * Load the configuration from the file system locations `configLocations`.
   * Entries must be valid hadoop URIs or a special URI with scheme "cp" which is treated as classpath entry.
   *
   * If `configLocation` is a directory, it is traversed in breadth-first search (BFS) order provided by hadoop file system.
   * Only file names ending in '.conf', '.json', or '.properties' are processed.
   * If multiple entries are given, all entries must be on the same file system.
   * All processed config files are merged and files encountered later overwrite settings in files processed earlier.
   *
   * The order of loading is:
   *
   * 1. system properties
   * 2. all '.conf' files in BFS order
   * 3. all '.json' files in BFS order
   * 4. all '.properties' files in BFS order
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
   * @param configLocations     configuration files or directories containing configuration files.
   * @param hadoopConf          Hadoop configuration to initialize filesystem.
   *                            Note that maybe additional hadoop/spark configurations could not yet been loaded from the configuration files.
   *                            In that case the default configuration is used.
   * @return                    a resolved [[Config]] merged from all found configuration files.
   */
  def loadConfigFromFilesystem(configLocations: Seq[String], hadoopConf: Configuration): Config = try {
    val hadoopPaths = configLocations.map( l => HdfsUtil.addHadoopDefaultSchemaAuthority(new Path(l)))
    logger.info(s"Loading configuration from filesystem locations: ${hadoopPaths.map(_.toUri).mkString(", ")}.")
    val hadoopConf: Configuration = new Configuration() // note that we could not yet load additional hadoop/spark configurations set in the configuration files

    // Search locations for config files
    val configFiles = hadoopPaths.flatMap(
      location => if (ClasspathConfigFile.canHandleScheme(location)) Seq(ClasspathConfigFile(location))
      else getFilesInBfsOrder(location)(location.getFileSystem(hadoopConf))
    )
    if (configFiles.isEmpty) throw ConfigurationException(s"No configuration files found in ${hadoopPaths.mkString(", ")}. " +
      s"Ensure the configuration files have one of the following extensions: ${configFileExtensions.map(ext => s".$ext").mkString(", ")}")

    //read file extensions in the same order as typesafe config
    val sortedConfigFiles = configFiles.filter(_.extension=="properties") ++ configFiles.filter(_.extension=="json") ++ configFiles.filter(_.extension=="conf")
    logger.debug(s"Configuration files to parse:\n${sortedConfigFiles.mkString("\n")}")

    // parse config files
    val sortedConfigs = sortedConfigFiles.map(file => (file, parseConfig(file))).reverse

    // check for duplicate first class object definitions (connections, data objects, actions)
    if (Environment.enableCheckConfigDuplicates) {
      val objectIdLocationMap =
        sortedConfigs.flatMap { case (file, config) => ConfigParser.getActionsEntries(config).map(objName => (ActionId(objName), file)) } ++
          sortedConfigs.flatMap { case (file, config) => ConfigParser.getDataObjectsEntries(config).map(objName => (DataObjectId(objName), file)) } ++
          sortedConfigs.flatMap { case (file, config) => ConfigParser.getConnectionEntries(config).map(objName => (ConnectionId(objName), file)) }
      val duplicates = objectIdLocationMap.groupBy(_._1)
        .filter(_._2.size > 1)
        .mapValues(_.map(_._2))
      if (duplicates.nonEmpty) {
        val duplicatesStr = duplicates.map { case (id, files) => s"$id=${files.mkString(";")}" }.mkString(" ")
        throw ConfigurationException(s"Configuration parsing failed because of configuration objects defined in multiple locations: $duplicatesStr")
      }
    }

    //system properties take precedence
    val systemPropConfig = ConfigFactory.systemProperties()
    mergeConfigs(systemPropConfig +: sortedConfigs.map(_._2)).resolve()
  } catch {
    // catch if hadoop libraries are missing and output debug informations
    case ex:UnsatisfiedLinkError =>
      logger.error(s"There seems to be a problem loading hadoop binaries: ${ex.getClass.getSimpleName}: ${ex.getMessage}. Make sure directory of hadoop libary is listed in path environment variable (libraryPath=${System.getProperty("java.library.path")})")
      // retry loading hadoop library on our own to catch better error message (e.g. 64/32bit problems)
      try {
        System.loadLibrary("hadoop")
        logger.info("Wow, loading hadoop native library succeeded when doing on our own, strange there is an error when loaded by hadoop itself.")
      } catch {
        case ex:UnsatisfiedLinkError => logger.error(s"retry loading hadoop library on our own ${ex.getClass.getSimpleName}: ${ex.getMessage}")
      }
      throw ex // throw original exception
  }

  /**
   * Merge configurations such that configurations earlier in the list overwrite configurations at the end of the list.
   *
   * @param configs a list of [[Config]]s sorted according to their priority
   * @return        a merged [[Config]].
   */
  private def mergeConfigs(configs: Seq[Config]): Config = {
    configs.reduceLeft((c1, c2) => c1.withFallback(c2))
  }

  /**
   * Parse a configuration file as Hocon [[Config]]
   * @param file: a ConfigFile to parse
   * @return parsed [[Config]] object
   */
  private def parseConfig(file: ConfigFile): Config = {
    val reader = file.getReader
    try {
      val parsedConfig = ConfigFactory.parseReader(reader)
      if (parsedConfig.isEmpty) {
        logger.warn(s"Config parsed from ${file.toString} is empty!")
      }
      parsedConfig
    } catch {
      case exception: Throwable =>
        throw ConfigurationException(s"Failed to parse config from ${file.toString}", None, exception)
    } finally {
      reader.close()
    }
  }

  /**
   * Collect Hadoop files with valid config file extensions in BFS order.
   * Note that all filenames containing "log4j" are ignored.
   *
   * This is an internal method to create a utility data structure.
   * BFS is implemented by recursion.
   *
   * @param path [[Path]] pointing to a configuration file or a directory from where the traversal should start.
   * @return     a BFS ordered config file list.
   */
  private def getFilesInBfsOrder(path: Path)(implicit fs: FileSystem): Seq[ConfigFile] = {
    if (fs.isDirectory(path)) {
      Try(RemoteIteratorWrapper(fs.listStatusIterator(path))) match {
        case Failure(exception) =>
          logger.warn(s"Failed to list directory content of ${path.toString}.", exception)
          Seq()
        case Success(children) =>
          // sort files before directories for BFS
          val (directories, files) = children
            .filterNot(_.getPath.getName.startsWith(".")) // ignore hidden entries
            .partition(_.isDirectory)
          (files ++ directories).flatMap(p => getFilesInBfsOrder(p.getPath)).toSeq
      }
    } else if (fs.isFile(path) && ConfigFile.canHandleExtension(path)) {
      if (path.getName.contains("log4j")) {
        logger.debug(s"Ignoring log4j configuration file '$path'.")
        Seq()
      } else {
        Seq(HadoopConfigFile(path))
      }
    } else {
      logger.debug(s"Ignoring file '$path'.")
      Seq()
    }
  }

  // Helper classes to handle different location types
  private case class HadoopConfigFile(override val path: Path)(implicit val fs: FileSystem) extends ConfigFile {
    override def getReader = new InputStreamReader(fs.open(path))
  }
  private case class ClasspathConfigFile(override val path: Path) extends ConfigFile {
    override def getReader: InputStreamReader = {
      val resource = path.toUri.getPath
      val inputStream = Option(getClass.getResourceAsStream(resource))
        .getOrElse(throw ConfigurationException(s"Could not find resource $resource in classpath"))
      new InputStreamReader(inputStream)
    }
  }
  private object ClasspathConfigFile {
    def canHandleScheme(path: Path): Boolean = path.toUri.getScheme == "cp"
  }
  private trait ConfigFile {
    def path: Path
    lazy val extension: String = ConfigFile.getExtension(path)
    if (!ConfigFile.canHandleExtension(extension)) throw ConfigurationException(s"Cannot parse file with unknown extension $path. Allowed extensions are ${configFileExtensions.mkString(", ")}")
    def getReader: InputStreamReader
  }
  private object ConfigFile {
    def canHandleExtension(extension: String): Boolean = configFileExtensions.contains(extension)
    def canHandleExtension(path: Path): Boolean = canHandleExtension(getExtension(path))
    private def getExtension(path: Path): String = path.getName.split('.').last
  }
}
