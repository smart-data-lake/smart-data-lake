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
package io.smartdatalake.util.spark.dataset

import org.apache.spark.sql.Column

/**
 * CommentedColumn is part of return type of DsComment.transformCommentCols
 * That method allows to not only transform columns
 * but also to add comments to the obtained columns.
 *
 * If a DataFrame is persisted as table (e.g. hive, Databricks) the column
 * comments are saved in the corresponding MetaStore and thus visible
 * and Databrick Catalog and DB Tools like Dbeaver.
 *
 * @param colname    name of column
 * @param definition expression of column
 * @param comment    a short description
 */
case class CommentedColumn(colname: String, definition: Column, comment: String = "") {
  val defName: (Column, String) = (definition, colname)
  val nameComment: (String, String) = (colname, comment)
}

