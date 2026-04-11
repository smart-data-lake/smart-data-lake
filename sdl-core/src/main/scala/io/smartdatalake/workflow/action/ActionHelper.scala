/*
 * Smart Data Lake - Build your data lake the smart way.
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
package io.smartdatalake.workflow.action

import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{ConfigurationException, InstanceRegistry, TypeMismatchException}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.CanCreateDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, InitSubFeed, SubFeed}
import org.apache.spark.sql.AnalysisException

import java.sql.Timestamp
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.TypeTag

/**
 * Collection of helper functions for Actions
 */
object ActionHelper extends SmartDataLakeLogger {

  /**
   * Check plausibility of latest timestamp of a [[GenericDataFrame]] vs. a given timestamp. Throws
   * exception if not successful.
   *
   * @param timestamp
   *   to compare with
   * @param df
   *   [[GenericDataFrame]] to compare with
   * @param tstmpColName
   *   the timestamp column of the dataframe
   */
  def checkDataFrameNotNewerThan(timestamp: Timestamp, df: GenericDataFrame, tstmpColName: String): Unit = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._
    logger.info("starting checkDataFrameNotNewerThan")
    val existingLatestCaptured = df.agg(Seq(max(col(tstmpColName)))).collect.headOption
      .map(_.getAs[Timestamp](0)).filter(_ != null)
    if (existingLatestCaptured.isDefined) {
      if (timestamp.compareTo(existingLatestCaptured.get) < 0) {
        throw new TimeOrderLogicException(
          s"""
             | When using historize, the timestamp of the current load mustn't be older
             | than the timestamp of any existing records in the reporting table.
             | Timestamp current load: $timestamp
             | Highest existing timestamp: ${existingLatestCaptured.get}
          """.
            stripMargin
        )
      }
    }
  }

  /**
   * search common inits between to partition column definitions
   */
  def searchCommonInits(partitions1: Seq[String], partitions2: Seq[String]): Seq[Seq[String]] =
    partitions1.inits.toSeq.intersect(partitions2.inits.toSeq)
      .filter(_.nonEmpty)

  /**
   * search greatest common init between to partition column definitions
   */
  def searchGreatestCommonInit(partitions1: Seq[String], partitions2: Seq[String]): Option[Seq[String]] = {
    val commonInits = searchCommonInits(partitions1, partitions2)
    if (commonInits.nonEmpty) Some(commonInits.maxBy(_.size))
    else None
  }

  def getOptionalDataFrame(input: CanCreateDataFrame, partitionValues: Seq[PartitionValues], subFeedType: Type)(implicit
      context: ActionPipelineContext
  ): Option[GenericDataFrame] = try
    Some(input.getDataFrame(partitionValues, subFeedType))
  catch {
    case e: IllegalArgumentException if e.getMessage.contains("DataObject schema is undefined") => None
    case e
        if e.getClass.getSimpleName == "AnalysisException" && e.getMessage.contains("[TABLE_OR_VIEW_NOT_FOUND]") ||
          e.getMessage().contains("[UNABLE_TO_INFER_SCHEMA]") || e.getMessage().contains("[DELTA_MISSING_DELTA_TABLE]") => None
    case _: NoDataToProcessWarning => None
  }

  /**
   * Replace all special characters in a String with underscore Used to get valid temp view names
   */
  def replaceSpecialCharactersWithUnderscore(str: String): String = {
    val invalidCharacters = "[^a-zA-Z0-9_]".r
    invalidCharacters.replaceAllIn(str, "_")
  }

  /**
   * Create a valid temporary view name for SQL transformation. Apart from replacing special
   * characters, a postfix is added to make the name unique in case the input name is also an
   * existing table.
   *
   * @param inputName
   *   name of the input the temporary view should be created for
   */
  def createTemporaryViewName(inputName: String): String =
    replaceSpecialCharactersWithUnderscore(inputName) + TEMP_VIEW_POSTFIX

  def replaceLegacyViewName(sql: String, inputViewName: String): String =
    sql.replaceAll("\\s" + inputViewName.stripSuffix(ActionHelper.TEMP_VIEW_POSTFIX) + "(\\s|\\.|$)", s" $inputViewName" + "$1")

  def createSkippedSubFeed(output: DataObject): SubFeed =
    InitSubFeed(dataObjectId = output.id, partitionValues = Seq(), isSkipped = true)

  /**
   * Create results for skipped actions, e.g. InitSubFeeds with isSkipped = true
   */
  def createSkippedSubFeeds(outputs: Seq[DataObject]): Seq[SubFeed] =
    outputs.map(output => createSkippedSubFeed(output))

  val TEMP_VIEW_POSTFIX = "_sdltemp"
}
