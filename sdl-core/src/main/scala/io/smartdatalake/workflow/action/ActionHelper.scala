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
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.definitions.{Environment, ExecutionModeResult}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.{ActionPipelineContext, FileSubFeed, InitSubFeed, SubFeed}
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanHandlePartitions, DataObject}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SparkSession}

private[smartdatalake] object ActionHelper extends SmartDataLakeLogger {

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

  def getOptionalDataFrame(sparkInput: CanCreateDataFrame, partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession, context: ActionPipelineContext) : Option[DataFrame] = try {
    Some(sparkInput.getDataFrame(partitionValues))
  } catch {
    case e: IllegalArgumentException if e.getMessage.contains("DataObject schema is undefined") => None
    case e: AnalysisException if e.getMessage.contains("Table or view not found") => None
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

  def getMainDataObjectCandidates[T <: DataObject](mainId: Option[DataObjectId], dataObjects: Seq[T], dataObjectIdsToIgnoreFilter: Seq[DataObjectId], inputOutput: String, mainNeeded: Boolean, actionId: ActionId): Seq[T] = {
    if (mainId.isDefined) {
      // if mainInput is defined -> return only that DataObject
      Seq(dataObjects.find(_.id == mainId.get).getOrElse(throw ConfigurationException(s"($actionId) main${inputOutput}Id ${mainId.get} not found in ${inputOutput}s")))
    } else {
      // prioritize DataObjects by number of partition columns
      dataObjects.sortBy {
        case x: CanHandlePartitions if !dataObjectIdsToIgnoreFilter.contains(x.id) => x.partitions.size
        case _ => 0
      }.reverse
    }
  }

  /**
   * Updates the partition values of a SubFeed to the partition columns of the given input data object:
   * - remove not existing columns from the partition values
   */
  def updateInputPartitionValues[T <: SubFeed](dataObject: DataObject, subFeed: T)(implicit session: SparkSession, context: ActionPipelineContext): T = {
    dataObject match {
      case partitionedDO: CanHandlePartitions =>
        // remove superfluous partitionValues
        subFeed.updatePartitionValues(partitionedDO.partitions, newPartitionValues = Some(subFeed.partitionValues)).asInstanceOf[T]
      case _ =>
        subFeed.clearPartitionValues().asInstanceOf[T]
    }
  }

  /**
   * Updates the partition values of a SubFeed to the partition columns of the given output data object:
   * - transform partition values
   * - add run_id_partition value if needed
   * - removing not existing columns from the partition values.
   */
  def updateOutputPartitionValues[T <: SubFeed](dataObject: DataObject, subFeed: T, partitionValuesTransform: Option[Seq[PartitionValues] => Map[PartitionValues,PartitionValues]] = None)(implicit session: SparkSession, context: ActionPipelineContext): T =
    dataObject match {
      case partitionedDO: CanHandlePartitions =>
        // transform partition values
        val newPartitionValues = partitionValuesTransform.map(fn => fn(subFeed.partitionValues).values.toSeq.distinct)
          .getOrElse(subFeed.partitionValues)
        // remove superfluous partitionValues
        subFeed.updatePartitionValues(partitionedDO.partitions, breakLineageOnChange = false, newPartitionValues = Some(newPartitionValues)).asInstanceOf[T]
      case _ =>
        subFeed.clearPartitionValues(breakLineageOnChange = false).asInstanceOf[T]
    }

  def addRunIdPartitionIfNeeded[T <: SubFeed](dataObject: DataObject, subFeed: T)(implicit session: SparkSession, context: ActionPipelineContext): T = {
    dataObject match {
      case partitionedDO: CanHandlePartitions =>
        if (partitionedDO.partitions.contains(Environment.runIdPartitionColumnName)) {
          val newPartitionValues = if (subFeed.partitionValues.nonEmpty) subFeed.partitionValues.map(_.addKey(Environment.runIdPartitionColumnName, context.runId.toString))
          else Seq(PartitionValues(Map(Environment.runIdPartitionColumnName -> context.runId.toString)))
          subFeed.updatePartitionValues(partitionedDO.partitions, breakLineageOnChange = false, newPartitionValues = Some(newPartitionValues)).asInstanceOf[T]
        } else subFeed
      case _ => subFeed
    }
  }

  def getHandleExecutionModeExceptionPartialFunction(outputs: Seq[DataObject]): PartialFunction[Throwable, Option[ExecutionModeResult]] = {
    // return empty output subfeeds if "no data"
    case ex: NoDataToProcessWarning =>
      // This exception is changed to a NoDataToProcessDontStopWarning but subFeeds have isSkipped set to true
      // The following action's executionCondition will stop by default if there is a skipped input subFeed. The executionCondition can be set to "true" to get stopIfNoData=false behaviour.
      val outputSubFeeds = outputs.map(output => InitSubFeed(dataObjectId = output.id, partitionValues = Seq(), isSkipped = true))
      // throw NoDataToProcessDontStopWarning with fake results added. The DAG will pass the fake results to further actions.
      throw NoDataToProcessDontStopWarning(ex.actionId, ex.msg, results = Some(outputSubFeeds))
    case ex: NoDataToProcessDontStopWarning =>
      // in this case subFeed isSkipped is set to false to be backward compatible with executionMode stopIfNoData=false
      // This can be removed once executionMode stopIfNoData is removed.
      val outputSubFeeds = outputs.map(output => InitSubFeed(dataObjectId = output.id, partitionValues = Seq()))
      // rethrow exception with fake results added. The DAG will pass the fake results to further actions.
      throw ex.copy(results = Some(outputSubFeeds))
  }
}

