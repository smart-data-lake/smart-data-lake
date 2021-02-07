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
package io.smartdatalake.definitions

import org.apache.spark.sql.SaveMode
import scala.language.implicitConversions

/**
 * SDL supports more SaveModes than Spark, that's why there is an own definition of SDLSaveMode.
 */
object SDLSaveMode extends Enumeration {
  type SDLSaveMode = Value

  // Standard Spark SaveMode's
  /**
   * @see [[SaveMode]]
   */
  val Overwrite: Value = Value("Overwrite")
  /**
   * @see [[SaveMode]]
   */
  val Append: Value = Value("Append")
  /**
   * @see [[SaveMode]]
   */
  val ErrorIfExists: Value = Value("Error")
  /**
   * @see [[SaveMode]]
   */
  val Ignore: Value = Value("Ignore")

  /**
   * This is like SDLSaveMode.Overwrite but doesnt delete the directory of the DataObject and its partition, but only the files
   * inside. Then it uses Sparks append mode to add the new files.
   * Like that ACLs set on the base directory are preserved.
   *
   * Implementation: This save mode will delete all files inside the base directory, but not the directory itself.
   * If no partition values are present when writing to a partitioned data object, all files in all partitions are
   * deleted, but not the partition directories itself. This is different to Sparks dynamic partitioning, which only deletes
   * partitions where data is present in the DataFrame to be written (enabled by default in SDL).
   * To stop if no partition values are present, configure executionMode.type = FailIfNoPartitionValuesMode on the Action.
   */
  val OverwritePreserveDirectories: Value = Value("OverwritePreserveDirectories")

  /**
   * This is like SDLSaveMode.Overwrite but processed partitions are manually deleted instead of using dynamic partitioning mode.
   * Then it uses Sparks append mode to add the new partitions.
   * This helps if there are performance problems when using dynamic partitioning mode with hive tables and many partitions.
   *
   * Implementation: This save mode will delete processed partition directories manually.
   * If no partition values are present when writing to a partitioned data object, all all partitions are deleted. This is
   * different to Sparks dynamic partitioning, which only deletes partitions where data is present in the DataFrame to
   * be written (enabled by default in SDL).
   * To stop if no partition values are present, configure executionMode.type = FailIfNoPartitionValuesMode on the Action.
   */
  val OverwriteOptimized: Value = Value("OverwriteOptimized")


  /* add implicit methods to enumeration, e.g. asSparkSaveMode */
  class SDLSaveModeValue(mode: Value) {
    /**
     * Mapping to Spark SaveMode
     * This is one-to-one except custom modes as OverwritePreserveDirectories
     */
    def asSparkSaveMode: SaveMode = mode match {
      case Overwrite => SaveMode.Overwrite
      case Append => SaveMode.Append
      case ErrorIfExists => SaveMode.ErrorIfExists
      case Ignore => SaveMode.Ignore
      case OverwritePreserveDirectories => SaveMode.Append // Append with spark, but delete files before with hadoop
      case OverwriteOptimized => SaveMode.Append // Append with spark, but delete partitions before with hadoop
    }
  }
  implicit def value2SparkSaveMode(mode: Value): SDLSaveModeValue = new SDLSaveModeValue(mode)

}