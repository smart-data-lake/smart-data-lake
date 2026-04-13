/*
 * sdl-core - Build your data lake the smart way.
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

import io.smartdatalake.config.ConfigurationException
import org.apache.hadoop.fs.Path

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.stream.Collectors

object ResourceUtil {

  /**
   * Validates if the a provided path has the schema 'cp'
   *
   * @param path [[Path]] pointing to a directory or file
   * @return tru if Uri scheme is 'cp' and false if not
   */
  def canHandleScheme(path: Path): Boolean = path.toUri.getScheme == "cp"

  def canHandleScheme(path: String): Boolean = canHandleScheme(new Path(path))

  /**
   * Creates InputStream for given resource file
   *
   * @param path Path to resource file, prefixed with cp:
   *             Note that reading resources needs an absolute path.
   */
  def readResource(path: Path): InputStream ={
    assert(canHandleScheme(path), "The provided path does not have the schema 'cp'.")
    val resource = path.toUri.getPath
    val inputStream = Option(getClass.getResourceAsStream(resource))
      .getOrElse(throw ConfigurationException(s"Could not find resource $resource in classpath"))
    inputStream
  }

  /**
   * Reads given resource file into a String
   *
   * @param path Path to resource file, prefixed with cp:
   *             Note that reading resources needs an absolute path.
   */
  def readResourceAsString(path: Path): String = {
    val inputStream = readResource(path)
    new BufferedReader(
      new InputStreamReader(inputStream, StandardCharsets.UTF_8)
    ).lines().collect(Collectors.joining())
  }

}
