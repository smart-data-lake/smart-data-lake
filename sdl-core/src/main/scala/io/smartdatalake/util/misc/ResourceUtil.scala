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

package io.smartdatalake.util.misc

import io.smartdatalake.config.ConfigurationException
import org.apache.hadoop.fs.Path

import java.io.InputStream

object ResourceUtil {

  /**
   * Validates if the a provided path has the schema 'cp'
   *
   * @param path [[Path]] pointing to a directory or file
   * @return tru if Uri scheme is 'cp' and false if not
   */
  def canHandleScheme(path: Path): Boolean = path.toUri.getScheme == "cp"

  /**
   * Creates InputStream out of provided file path
   *
   * @param path [[Path]] pointing to a directory or file
   * @return [[InputStream]] from the provided path
   */
  def readResource(path: Path): InputStream ={
    assert(canHandleScheme(path), "The provided path does not have the schema 'cp'.")
    val resource = path.toUri.getPath
    val inputStream = Option(getClass.getResourceAsStream(resource))
      .getOrElse(throw ConfigurationException(s"Could not find resource $resource in classpath"))
    inputStream
  }

}
