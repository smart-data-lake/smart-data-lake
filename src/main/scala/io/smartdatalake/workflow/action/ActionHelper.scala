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
package io.smartdatalake.workflow.action

import java.sql.Timestamp
import java.time.LocalDateTime

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.ActionObjectId
import io.smartdatalake.definitions.{ExecutionMode, PartitionDiffMode, SparkIncrementalMode, SparkStreamingOnceMode}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanHandlePartitions, DataObject}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object ActionHelper extends SmartDataLakeLogger {

  /**
   * Removes all columns from a [[DataFrame]] except those specified in whitelist.
   *
   * @param df [[DataFrame]] to be filtered
   * @param columnWhitelist columns to keep
   * @return [[DataFrame]] with all columns removed except those specified in whitelist
   */
  def filterWhitelist(columnWhitelist: Seq[String])(df: DataFrame): DataFrame = {
    df.select(df.columns.filter(colName => columnWhitelist.contains(colName.toLowerCase)).map(col) :_*)
  }

  /**
   * Remove all columns in blacklist from a [[DataFrame]].
   *
   * @param df [[DataFrame]] to be filtered
   * @param columnBlacklist columns to remove
   * @return [[DataFrame]] with all columns in blacklist removed
   */
  def filterBlacklist(columnBlacklist: Seq[String])(df: DataFrame): DataFrame = {
    df.select(df.columns.filter(colName => !columnBlacklist.contains(colName.toLowerCase)).map(col) :_*)
  }

  /**
   * create util literal column from [[LocalDateTime ]]
   */
  def ts1(t: java.time.LocalDateTime): Column = lit(t.toString).cast(TimestampType)


  def dropDuplicates(pks: Seq[String])(df: DataFrame): DataFrame = {
    df.dropDuplicates(pks)
  }

  /**
   * Check plausibility of latest timestamp of a [[DataFrame]] vs. a given timestamp.
   * Throws exception if not successful.
   *
   * @param timestamp to compare with
   * @param df [[DataFrame]] to compare with
   * @param tstmpColName the timestamp column of the dataframe
   */
  def checkDataFrameNotNewerThan(timestamp: LocalDateTime, df: DataFrame, tstmpColName: String)(implicit session: SparkSession): Unit = {
    import session.implicits._

    logger.info("starting checkDataFrameNotNewerThan")
    session.sparkContext.setJobDescription("checkDataFrameNotNewerThan")
    val existingLatestCaptured = df.agg(max(col(tstmpColName))).as[Timestamp].collect.find(_ != null)
    if (existingLatestCaptured.isDefined) {
      if (timestamp.compareTo(existingLatestCaptured.get.toLocalDateTime) < 0) {
        throw new TimeOrderLogicException(
          s"""
             | When using historize, the timestamp of the current load mustn't be older
             | than the timestamp of any existing records in the reporting table.
             | Timestamp current load: $timestamp
             | Highest existing timestamp: ${existingLatestCaptured.get}
          """.
            stripMargin)
      }
    }
  }

  /**
   * search common inits between to partition column definitions
   */
  def searchCommonInits(partitions1: Seq[String], partitions2: Seq[String]): Seq[Seq[String]] = {
    partitions1.inits.toSeq.intersect(partitions2.inits.toSeq)
      .filter(_.nonEmpty)
  }

  /**
   * search greatest common init between to partition column definitions
   */
  def searchGreatestCommonInit(partitions1: Seq[String], partitions2: Seq[String]): Option[Seq[String]] = {
    val commonInits = searchCommonInits(partitions1, partitions2)
    if (commonInits.nonEmpty) Some(commonInits.maxBy(_.size))
    else None
  }

  /**
   * Apply execution mode to partition values
   */
  def applyExecutionMode(executionMode: ExecutionMode, actionId: ActionObjectId, input: DataObject, output: DataObject, phase: ExecutionPhase)(implicit session: SparkSession): Option[(Seq[PartitionValues], Option[Column])] = {
    import session.implicits._

    executionMode match {
      case mode:PartitionDiffMode =>
        (input,output) match {
          case (partitionInput: CanHandlePartitions, partitionOutput: CanHandlePartitions)  =>
            if (partitionInput.partitions.nonEmpty) {
              if (partitionOutput.partitions.nonEmpty) {
                // prepare common partition columns
                val commonInits = searchCommonInits(partitionInput.partitions, partitionOutput.partitions)
                require(commonInits.nonEmpty, s"$actionId has set initExecutionMode = 'partitionDiff' but no common init was found in partition columns for $input and $output")
                val commonPartitions = if (mode.partitionColNb.isDefined) {
                  commonInits.find(_.size==mode.partitionColNb.get).getOrElse(throw ConfigurationException(s"$actionId has set initExecutionMode = 'partitionDiff' but no common init with ${mode.partitionColNb.get} was found in partition columns of $input and $output from $commonInits!"))
                } else {
                  commonInits.maxBy(_.size)
                }
                // calculate missing partition values
                val partitionValuesToBeProcessed = partitionInput.listPartitions.map(_.filterKeys(commonPartitions)).toSet
                  .diff(partitionOutput.listPartitions.map(_.filterKeys(commonPartitions)).toSet).toSeq
                // stop processing if no new data
                if (partitionValuesToBeProcessed.isEmpty) throw NoDataToProcessWarning(actionId.id, s"($actionId) No partitions to process found for ${input.id}")
                // sort and limit number of partitions processed
                val ordering = PartitionValues.getOrdering(commonPartitions)
                val selectedPartitionValues = mode.nbOfPartitionValuesPerRun match {
                  case Some(n) => partitionValuesToBeProcessed.sorted(ordering).take(n)
                  case None => partitionValuesToBeProcessed.sorted(ordering)
                }
                logger.info(s"($actionId) $PartitionDiffMode selected partition values ${selectedPartitionValues.mkString(", ")} to process")
                //return
                Some((selectedPartitionValues, None))
              } else throw ConfigurationException(s"$actionId has set initExecutionMode = $PartitionDiffMode but $output has no partition columns defined!")
            } else throw ConfigurationException(s"$actionId has set initExecutionMode = $PartitionDiffMode but $input has no partition columns defined!")
          case (_: CanHandlePartitions, _) => throw ConfigurationException(s"$actionId has set initExecutionMode = $PartitionDiffMode but $output does not support partitions!")
          case (_, _) => throw ConfigurationException(s"$actionId has set initExecutionMode = $PartitionDiffMode but $input does not support partitions!")
        }

      case mode:SparkIncrementalMode =>
        (input,output) match {
          case (sparkInput: CanCreateDataFrame, sparkOutput: CanCreateDataFrame) =>
            val dfInput = sparkOutput.getDataFrame()
            val inputColType = dfInput.schema(mode.compareCol).dataType
            require(SparkIncrementalMode.allowedDataTypes.contains(inputColType), s"($actionId) Type of compare column ${mode.compareCol} must be one of ${SparkIncrementalMode.allowedDataTypes.mkString(", ")} in ${sparkInput.id}")
            val dfOutput = sparkOutput.getDataFrame()
            val outputColType = dfOutput.schema(mode.compareCol).dataType
            require(SparkIncrementalMode.allowedDataTypes.contains(outputColType), s"($actionId) Type of compare column ${mode.compareCol} must be one of ${SparkIncrementalMode.allowedDataTypes.mkString(", ")} in ${sparkOutput.id}")
            require(inputColType == outputColType, s"($actionId) Type of compare column ${mode.compareCol} is different between ${sparkInput.id} ($inputColType) and ${sparkOutput.id} ($outputColType)")
            // get latest values
            val outputLatestValue = dfInput.agg(max(col(mode.compareCol)).cast(StringType)).as[String].head
            val inputLatestValue = dfOutput.agg(max(col(mode.compareCol)).cast(StringType)).as[String].head
            // stop processing if no new data
            if (outputLatestValue == inputLatestValue) throw NoDataToProcessWarning(actionId.id, s"($actionId) No increment to process found for ${input.id} and column ${mode.compareCol}")
            // prepare filter
            val selectedData = col(mode.compareCol) > lit(outputLatestValue).cast(inputColType)
            Some((Seq(), Some(selectedData)))
          case _ => throw ConfigurationException(s"$actionId has set executionMode = $SparkIncrementalMode but $input or $output does not support creating Spark DataFrames!")
        }

      case _ => None
    }
  }

  /**
   * Replace all special characters in a String with underscore
   * Used to get valid temp view names
   * @param str
   * @return
   */
  def replaceSpecialCharactersWithUnderscore(str: String) : String = {
    val invalidCharacters = "[^a-zA-Z0-9_]".r
    invalidCharacters.replaceAllIn(str, "_")
  }

}

