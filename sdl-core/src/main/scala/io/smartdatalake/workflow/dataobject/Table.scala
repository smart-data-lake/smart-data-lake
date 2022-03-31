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
package io.smartdatalake.workflow.dataobject

/**
 * Table attributes
 *
 * @param db          database-schema to be used for this table. If there exists a connection for the DataObject,
 *                    the contents of this field will be overwritten.
 *                    Called db for backwards-compatibility because for hive tables, db and connection mean the same thing.
 * @param name        table name
 * @param query       optional select query
 * @param primaryKey  optional sequence of primary key columns
 * @param foreignKeys optional sequence of foreign key definitions.
 *                    This is used as metadata for a data catalog.
 * @param options
 */
case class Table(
                  db: Option[String],
                  name: String,
                  query: Option[String] = None,
                  primaryKey: Option[Seq[String]] = None,
                  foreignKeys: Option[Seq[ForeignKey]] = None,
                  options: Option[Map[String,String]] = None
                ) {
  override def toString: String = s"""$fullName${primaryKey.map(pks => "("+pks.mkString(",")+")").getOrElse("")}"""

  def overrideDb(dbParam: Option[String]): Table = if (db.isEmpty) this.copy(db=dbParam) else this

  def fullName: String = db.map(_ + ".").getOrElse("") + name
}

/**
 * Foreign key definition
 *
 * @param db target database, if not defined it is assumed to be the same as the table owning the foreign key
 * @param table referenced target table name
 * @param columns mapping of source column(s) to referenced target table column(s)
 * @param name optional name for foreign key, e.g to depict it's role
 */
case class ForeignKey(
                       db: Option[String],
                       table: String,
                       columns: Map[String,String],
                       name: Option[String]
                     )