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
package io.smartdatalake.workflow.dataobject.generic

import io.smartdatalake.config.SdlConfigObject.DataObjectId

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
 * @param primaryKey  optional sequence of primary key columns
 * @param createAndReplacePrimaryKey Parameter to define if the primary key should be created and updated
 *                                   according to the SDLB configuration (=TRUE), or if they are configured just
 *                                   for information purposes (=FALSE). It defaults to false. For the creation / replacement to work,
 *                                   at least one primary Key column must be defined.
 *                                   Primary keys are created at deployment time by CatalogSchemaUpdater,
 *                                   see [[io.smartdatalake.workflow.dataobject.generic.CanHandleConstraints]]
 *                                   for the DataObjects supporting it. Using it in other DataObjects will have no effect.
 *                                   It also makes the primary key columns not null when creating or evolving
 *                                   the table schema, see [[io.smartdatalake.workflow.dataobject.generic.CanHandleTableSchema]].
 * @param primaryKeyConstraintName  This parameter is used in case that createAndReplacePrimaryKey is set to TRUE.
 *                                  In case a constraint name is not given, the default value sdlb_"tableName"_pk will be used
 *                                  when updating the primary key.
 * @param foreignKeys optional sequence of foreign key definitions.
 *                    This is used as metadata for a data catalog, and to create foreign key
 *                    constraints if `createAndReplaceForeignKeys` is set to true.
 * @param createAndReplaceForeignKeys Parameter to define if the foreign keys should be created and updated
 *                                    according to the SDLB configuration (=TRUE), or if they are configured just
 *                                    for information purposes (=FALSE). It defaults to false.
 *                                    Foreign keys are created at deployment time by CatalogSchemaUpdater,
 *                                    see [[io.smartdatalake.workflow.dataobject.generic.CanHandleForeignKeys]].
 *                                    Note that the referenced table must exist and have a primary key on the
 *                                    referenced columns.
 * Each foreign key in the .conf files is an object with the following properties:
 * {dataObjectId: string, columns: Map[String], name: string}, whereas a Map[String] is simply
 * a further object of the type {<local_column_name>:string, <referenced_column_name>:string}. For example:
 *   foreignKeys = [
 *       {
 *         dataObjectId = "referenced_data_object_id"
 *         columns = {
 *           "local_column_name": "referenced_column_name"
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
                  createAndReplaceForeignKeys: Boolean = false,
                  catalog: Option[String] = None
                ) {
  override def toString: String = s"""$fullName${primaryKey.map(pks => "("+pks.mkString(",")+")").getOrElse("")}"""

  def overrideCatalogAndDb(catalogParam: Option[String], dbParam: Option[String]): Table = {
    this.copy(catalog = catalog.orElse(catalogParam), db = db.orElse(dbParam))
  }

  def fullName: String = nameParts.mkString(".")

  def getDbName: String = nameParts.init.mkString(".")

  def nameParts: Seq[String] = Seq(catalog, db, Some(name)).flatten

}

/**
 * Foreign key definition.
 *
 * The referenced table is not given by name, but by the id of the DataObject defining it. Its catalog,
 * database and table name are looked up in the configuration when the foreign key is applied,
 * see [[CanHandleForeignKeys]].
 *
 * @param dataObjectId id of the DataObject referenced by this foreign key. It must be a table DataObject.
 *                     If its table does not define a catalog or database, the ones of the table owning the
 *                     foreign key are used.
 * @param columns mapping of source column(s) to referenced target table column(s). The map is given
 * as a list of objects with the following syntax: {"local_column_name" : "referenced_column_name"}
 * @param name optional name for foreign key, e.g. to depict its role.
 *
 *
 * Foreign keys in .conf files are to be defined like the following example
 * (here two foreign key objects):
 *   foreignKeys = [
 *       {
 *         dataObjectId = "referenced_data_object_id"
 *         columns = {
 *           "local_column_name": "referenced_column_name"
 *           }
 *         name = "OPTIONAL_key_name"
 *       },
 *       {
 *         dataObjectId = "another_referenced_data_object_id"
 *         columns = {
 *           "another_local_column_name": "another_referenced_column_name"
 *         }
 *         name = "another_OPTIONAL_key_name"
 *       }
 *     ]
 */
case class ForeignKey(
                       dataObjectId: DataObjectId,
                       columns: Map[String,String],
                       name: Option[String] = None
                     )
