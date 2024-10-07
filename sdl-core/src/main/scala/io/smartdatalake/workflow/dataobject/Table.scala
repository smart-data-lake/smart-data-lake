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

import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Table attributes
 *
 * @param catalog     Optional catalog to be used for this table. If null default catalog is used.
 *                    If there exists a connection with catalog value for the DataObject and this field is not defined, it will be set to the connections catalog value.
 * @param db database-schema to be used for this table.
 *           If there exists a connection for the DataObject and this field is not defined, it will be set to the connections database value .
 *           Called db for backwards-compatibility because for hive tables, db and schema mean the same thing.
 * @param name        table name
 * @param query       optional select query
 * @param commentOnTable  An optional comment to add to the table after writing a DataFrame to it. As of now, this is only possible for Delta Lake and Snowflake tables.
 * @param commentsOnColumns Optional sequence of comments to add to the columns of the table as metadata. They must be written as follows:
 *                          commentsOnColumns = {
 *                            colname1 = "Comment or description for column colname1"
 *                            colname2 = "Comment or descrpition for column colname2"
 *                          }
 * @param primaryKey  optional sequence of primary key columns
 * @param createAndReplacePrimaryKey Parameter to define if the primary key should be created and updated
 *                                   according to the SDLB configuration (=TRUE), or if they are configured just
 *                                   for information purposes (=FALSE). It defaults to false. For the creation / replacement to work,
 *                                   at least one primary Key column must be defined.
 *                                   As of now, this feature is only available for JdbcTableDataObject. Using it in other DataObjects
 *                                   will have no effect.
 * @param primaryKeyConstraintName  This parameter is used in case that createAndReplaceParameterPrimaryKey is set to TRUE.
 *                                  In case a constraint name is not given, the default value sdlb_"tableName" will be used
 *                                  when updating the primary key.
 * @param foreignKeys optional sequence of foreign key definitions.
 *                    This is used as metadata for a data catalog.
 * Each foreign key in the .conf files is an object with the following properties: 
 * {db: string, table: string , name: string map: Map[String]}, whereas a Map[String] is simply 
 * a further object of the type {<local_column_name>:string, <external_column_name>:string}. For example: 
 *   foreignKeys = [
 *       {
 *         db = "OPTIONAL_DB_name" 
 *         table = "table_id" 
 *         columns = { 
 *           "local_column_name": "external_column_name" 
 *           } 
 *         name = "OPTIONAL_key_name" 
 *       } 
 *     ]
 */
case class Table(
                  db: Option[String],
                  name: String,
                  query: Option[String] = None,
                  primaryKey: Option[Seq[String]] = None,
                  createAndReplacePrimaryKey: Boolean = false,
                  primaryKeyConstraintName: Option[String] = None,
                  foreignKeys: Option[Seq[ForeignKey]] = None,
                  commentOnTable: Option[String],
                  commentsOnColumns: Option[Map[String, String]],
                  catalog: Option[String] = None
                ) {
  override def toString: String = s"""$fullName${primaryKey.map(pks => "("+pks.mkString(",")+")").getOrElse("")}"""

  def overrideCatalogAndDb(catalogParam: Option[String], dbParam: Option[String]): Table = {
    this.copy(catalog = catalog.orElse(catalogParam), db = db.orElse(dbParam))
  }

  def fullName: String = nameParts.mkString(".")

  def getDbName: String = nameParts.init.mkString(".")

  def nameParts: Seq[String] = Seq(catalog, db, Some(name)).flatten

  private[smartdatalake] def tableIdentifier: TableIdentifier = {
    TableIdentifier(name, db, catalog)
  }
}

/**
 * Foreign key definition.
 *
 * @param db target database, if not defined it is assumed to be the same as the table owning the foreign key
 * @param table referenced target table name
 * @param columns mapping of source column(s) to referenced target table column(s). The map is given
 * as a list of objects with the following syntax: {"local_column_name" : "external_column_name"}
 * @param name optional name for foreign key, e.g to depict it's role.
 * 
 * 
 * Foreign keys in .conf files are to be defined like the following example 
 * (here two foreign key objects): 
 *   foreignKeys = [
 *       {
 *         db = "OPTIONAL_DB_name"
 *         table = "table_id"
 *         columns = {
 *           "local_column_name": "external_column_name"
 *           }
 *         name = "OPTIONAL_key_name"
 *       },
 *       {
 *         table = "another_table_id"
 *         columns = {
 *           "another_local_column_name": "another_external_column_name"
 *         }
 *         name = "another_OPTIONAL_key_name"
 *       }
 *     ]
 */
case class ForeignKey(
                       db: Option[String],
                       table: String,
                       columns: Map[String,String],
                       name: Option[String]
                     )

  /**
   *
   * @param colName Name of the column
   * @param comment Comment of the column as metadata.
   */
