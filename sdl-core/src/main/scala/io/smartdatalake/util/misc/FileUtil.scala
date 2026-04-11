/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.misc

import io.smartdatalake.util.hdfs.HdfsUtil.{addHadoopDefaultSchemaAuthority, getHadoopFsWithConf, readHadoopFile}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.File
import scala.io.Source
import scala.util.Using

object FileUtil
{
  def readFile(file: File): String = {
    Using(Source.fromFile(file)) {
      _.getLines().mkString(System.lineSeparator)
    }.get
  }

  def readFromPath(inputPath: Path)(implicit hadoopConfiguration: Configuration): String = {
    val path = addHadoopDefaultSchemaAuthority(inputPath)
    if (ResourceUtil.canHandleScheme(path)) ResourceUtil.readResourceAsString(path)
    else {
      val filesystem = getHadoopFsWithConf(path)
      readHadoopFile(path)(filesystem)
    }
  }
}
