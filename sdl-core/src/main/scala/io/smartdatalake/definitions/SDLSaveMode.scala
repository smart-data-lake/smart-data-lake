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

import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrameWriterV2, Row, SaveMode}

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
   * Spark only optimization.
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
   * Spark only optimization.
   * This is like SDLSaveMode.Overwrite but processed partitions are manually deleted instead of using dynamic partitioning mode.
   * Then it uses Sparks append mode to add the new partitions.
   * This helps if there are performance problems when using dynamic partitioning mode with hive tables and many partitions.
   *
   * Implementation: This save mode will delete processed partition directories manually.
   * If no partition values are present when writing to a partitioned data object, all partitions are deleted. This is
   * different to Sparks dynamic partitioning, which only deletes partitions where data is present in the DataFrame to
   * be written (enabled by default in SDL).
   * To stop if no partition values are present, configure executionMode.type = FailIfNoPartitionValuesMode on the Action.
   */
  val OverwriteOptimized: Value = Value("OverwriteOptimized")

  /**
   * Merge new data with existing data by insert new records and update (or delete) existing records.
   * DataObjects need primary key defined to check if a record is new.
   * To delete existing records add column '_deleted' to DataFrame and set its value to 'true' for the records which should be deleted.
   *
   * Note that only few DataObjects are able to merge new data, e.g. DeltaLakeTableDataObject and JdbcTableDataObject
   */
  val Merge: Value = Value("Merge")

  private[smartdatalake] def execV2(saveMode: SDLSaveMode.Value, writer: DataFrameWriterV2[Row], partitionValues: Seq[PartitionValues], partitionOverwriteModeDynamic: Boolean = false): Unit = {
    implicit val helper: SparkSubFeed.type = SparkSubFeed
    saveMode match {
      case SDLSaveMode.Append => writer.append()
      case SDLSaveMode.Overwrite | SDLSaveMode.OverwriteOptimized if partitionValues.nonEmpty => writer.overwrite(expr(partitionValues.map(_.getFilterExpr).reduce(_ or _).exprSql))
      case SDLSaveMode.Overwrite | SDLSaveMode.OverwriteOptimized if partitionValues.isEmpty && partitionOverwriteModeDynamic => writer.overwritePartitions()
      case SDLSaveMode.Overwrite | SDLSaveMode.OverwriteOptimized if partitionValues.isEmpty => writer.replace()
    }
  }
}

/**
 * Override and control detailed behaviour of saveMode, especially SaveMode.Merge for now.
 */
sealed trait SaveModeOptions {
  private[smartdatalake] def saveMode: SDLSaveMode

  private[smartdatalake] def convertToTargetSchema[D <: GenericDataFrame](df: D): D = df
}

/**
 * This class can be used to override save mode without further special parameters.
 */
case class SaveModeGenericOptions(override val saveMode: SDLSaveMode) extends SaveModeOptions

/**
 * Options to control detailed behaviour of SaveMode.Merge.
 * In Spark expressions use table alias 'existing' to reference columns of the existing table data, and table alias 'new' to reference columns of new data set.
 * @param deleteCondition A condition to control if matched records are deleted. If no condition is given, *no* records are delete.
 * @param updateCondition A condition to control if matched records are updated. If no condition is given all matched records are updated (default).
 *                        Note that delete is applied before update. Records selected for deletion are automatically excluded from the updates.
 * @param updateColumns List of column names to update in update clause. If empty all columns (except primary keys) are updated (default)
 * @param insertCondition A condition to control if unmatched records are inserted. If no condition is given all unmatched records are inserted (default).
 * @param insertColumnsToIgnore List of column names to ignore in insert clause. If empty all columns are inserted (default).
 * @param insertValuesOverride Optional Map of column name and value expression to override value on insert. Value expressions have to be a sql expression string, e.g. true or 'abc'.
 * @param additionalMergePredicate To optimize performance for SDLSaveMode.Merge it might be interesting to limit the records read from the existing table data, e.g. merge operation might use only the last 7 days.
 */
case class SaveModeMergeOptions(deleteCondition: Option[String] = None,
                                updateCondition: Option[String] = None,
                                updateColumns: Seq[String] = Seq(),
                                updateExistingCondition: Option[String] = None,
                                insertCondition: Option[String] = None,
                                insertColumnsToIgnore: Seq[String] = Seq(),
                                insertValuesOverride: Map[String, String] = Map(),
                                additionalMergePredicate: Option[String] = None
                               ) extends SaveModeOptions {
  override private[smartdatalake] val saveMode = SDLSaveMode.Merge
  private[smartdatalake] val deleteConditionExpr = deleteCondition.map(expr)
  private[smartdatalake] val updateConditionExpr = updateCondition.map(expr)
  private[smartdatalake] val updateExistingConditionExpr = updateExistingCondition.map(expr)
  private[smartdatalake] val insertConditionExpr = insertCondition.map(expr)
  private[smartdatalake] val insertValuesOverrideExpr = insertValuesOverride.mapValues(expr)
  private[smartdatalake] val additionalMergePredicateExpr = additionalMergePredicate.map(expr)
  private[smartdatalake] val updateColumnsOpt = if (updateColumns.nonEmpty) Some(updateColumns) else None

  override private[smartdatalake] def convertToTargetSchema[D <: GenericDataFrame](df: D): D = insertColumnsToIgnore.foldLeft(df) {
    case (df, col) => df.drop(col).asInstanceOf[D]
  }
}
object SaveModeMergeOptions {
  def fromSaveModeOptions(saveModeOptions: SaveModeOptions): SaveModeMergeOptions = saveModeOptions match {
    case m: SaveModeMergeOptions => m
    case m: SaveModeGenericOptions if (m.saveMode == SDLSaveMode.Merge) => SaveModeMergeOptions()
    case m => throw new IllegalStateException(s"Cannot convert ${m.getClass.getSimpleName} $m to SaveModeMergeOptions")
  }
}