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

package io.smartdatalake.util.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * A factory for Hadoop FileSystems.
 */
trait FileSystemFactory {
  def getFileSystem(path: Path, hadoopConf: Configuration): FileSystem
}

/**
 * Default Hadoop FileSystem factory creates the standard filesystem according to a given path and hadoop configuration.
 */
class DefaultFileSystemFactory extends FileSystemFactory {
  def getFileSystem(path: Path, hadoopConf: Configuration): FileSystem = {
    path.getFileSystem(hadoopConf)
  }
}
