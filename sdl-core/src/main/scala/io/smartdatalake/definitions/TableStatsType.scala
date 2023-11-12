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

package io.smartdatalake.definitions

object TableStatsType extends Enumeration {
  type TableStatsType = Value
  val SizeInBytes = Value("sizeInBytes")
  val SizeInBytesCurrent = Value("sizeInBytesCurrent")
  val TableSizeInBytes = Value("tableSizeInBytes")
  val CreatedAt = Value("createdAt")
  val LastModifiedAt = Value("lastModifiedAt")
  val LastAnalyzedAt = Value("lastAnalyzedAt")
  val LastAnalyzedColumnsAt = Value("lastAnalyzedColumnsAt")
  val LastCommitMsg = Value("lastCommitMsg")
  val Location = Value("location")
  val Info = Value("info")
  val NumFiles = Value("numFiles")
  val NumDataFilesCurrent = Value("numDataFilesCurrent") // for table formats with history (Delta, Iceberg...)
  val NumRows = Value("numRows")
  val NumPartitions = Value("numPartitions")
  val MinPartition = Value("minPartition")
  val MaxPartition = Value("maxPartition")
  val OldestSnapshotTs = Value("oldestSnapshotTs")
  val Branches = Value("branches")
  val Columns = Value("columns")
}