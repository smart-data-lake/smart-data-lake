/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.secrets

import io.smartdatalake.config.ConfigurationException

import scala.util.Using

/**
 * Read a secret from a property file, where the filename is provided directly in the configuration entry.
 * Use format FILE#<filename>;<secretName> to read secretName from file with filename.
 * The file must be a valid property file, e.g. every line needs to be in format "<secretName>=<secretValue>".
 */
object GenericFileSecretProvider extends SecretProvider {
  override def getSecret(name: String): String = {
    assert(name.split(";").length == 2, s"Expected semicolon separated filename and variable, got $name")
    val Array(file, secretKey) = name.split(";")
    FileSecretProvider.getSecretFromFile(file, secretKey)
  }
}

/**
 * Define a secret provider for a specific file.
 * This is to avoid putting the file name into every configuration entry as with GenericFileSecretProvider.
 * The file must be a valid property file, e.g. every line needs to be in format "<secretName>=<secretValue>".
 * @param file filename of property file to read secrets from
 */
class FileSecretProvider(file: String) extends SecretProvider {
  def this(options: Map[String, String]) = {
    this(
      options.getOrElse("file", throw new ConfigurationException(s"Cannot create FileSecretProvider, option 'file' missing."))
    )
  }
  override def getSecret(name: String): String = FileSecretProvider.getSecretFromFile(file, name)
}
object FileSecretProvider {
  def getSecretFromFile(file: String, name: String): String = {
    val props = Using.resource(scala.io.Source.fromFile(file))(_.getLines().toSeq)
    val namePrefix = name + "="
    props.find(_.startsWith(namePrefix))
      .map(_.stripPrefix(namePrefix))
      .getOrElse(throw new ConfigurationException(s"Variable $name in file $file not found"))
  }
}