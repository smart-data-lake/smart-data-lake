/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.dataobject

/**
 * Table attributes
 *
 * @param db optional override of db defined by connection
 * @param name table name
 * @param query optional select query
 * @param primaryKey optional sequence of primary key columns
 * @param options
 */
case class Table(
                  db: Option[String],
                  name: String,
                  query: Option[String] = None,
                  primaryKey: Option[Seq[String]] = None,
                  options: Option[Map[String,String]] = None
                ) {
  override def toString: String = s"""$fullName${primaryKey.map(pks => "("+pks.mkString(",")+")").getOrElse("")}"""

  def overrideDb(dbParam: Option[String]): Table = if (db.isEmpty) this.copy(db=dbParam) else this

  def fullName: String = db.map(_ + ".").getOrElse("") + name
}
