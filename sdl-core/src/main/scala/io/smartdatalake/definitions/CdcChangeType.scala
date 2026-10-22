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
package io.smartdatalake.definitions

/**
 * Values of the change-data-capture (CDC) change type column, see [[Environment.cdcChangeTypeColumnName]].
 *
 * The values follow the change data feed of Delta Lake, with `read` added for events which stem from an initial
 * snapshot of the source table instead of a real change, e.g. produced by DebeziumCdcDataObject.
 */
object CdcChangeType {

  /**
   * A record has been created in the source system.
   */
  val insert = "insert"

  /**
   * The value of a record before it has been updated in the source system.
   * Only available if the source system delivers the previous value of an update.
   */
  val updatePreimage = "update_preimage"

  /**
   * The value of a record after it has been updated in the source system.
   */
  val updatePostimage = "update_postimage"

  /**
   * A record has been deleted in the source system.
   */
  val delete = "delete"

  /**
   * A record read from an initial snapshot of the source table, e.g. no real change.
   * This is an SDLB specific extension of Delta Lakes change data feed.
   */
  val read = "read"

  /**
   * Change types which describe the current value of a record, e.g. which should be kept when consolidating
   * the change events of one primary key.
   */
  val currentValueTypes: Seq[String] = Seq(insert, updatePostimage, read, delete)
}
