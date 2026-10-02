/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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
package io.smartdatalake.config

import scala.annotation.StaticAnnotation

/**
 * Marks a configuration attribute holding options which are passed to an underlying library, e.g. `csvOptions`
 * passed to the Spark CSV data source.
 *
 * The json schema export adds a link to the documentation of the library to the description of the attribute,
 * and lists the option names known by the `optionsProvider` as properties, so that the schema viewer and IDEs
 * can show them. Other option names are still accepted.
 *
 * Arguments must be given as positional string literals, as they are read from the annotation tree by reflection.
 *
 * @param docUrl          URL of the documentation of the options of the underlying library.
 * @param optionsProvider Optional fully qualified name of a Scala object which knows the option names, either a
 *                        [[LibraryOptionsProvider]] or a Spark `org.apache.spark.sql.catalyst.DataSourceOptions`
 *                        object, e.g. `org.apache.spark.sql.catalyst.csv.CSVOptions`.
 */
final class LibraryOptions(docUrl: String, optionsProvider: String) extends StaticAnnotation {
  def this(docUrl: String) = this(docUrl, "")
}

/**
 * A Scala object implementing this trait can be used as `optionsProvider` of a [[LibraryOptions]] annotation,
 * if the underlying library does not provide a list of its option names itself.
 */
trait LibraryOptionsProvider {
  def libraryOptionNames: Set[String]
}
