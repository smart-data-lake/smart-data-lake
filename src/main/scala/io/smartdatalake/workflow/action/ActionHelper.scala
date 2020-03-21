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
import io.smartdatalake.definitions.{ExecutionMode, PartitionDiffMode}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.dataobject.{CanCreateDataFrame, CanHandlePartitions, DataObject, TableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
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
    * Apply filter to a [[DataFrame]].
    *
    * @param df [[DataFrame]] to be filtered
    * @param filterClauseExpr Spark expression to apply to the dataframe
    * @return [[DataFrame]] with all rows filter given expression above
    */
  def filterRows(filterClauseExpr: Column)(df: DataFrame): DataFrame = {
    df.where(filterClauseExpr)
  }

  /**
   * create util literal column from [[LocalDateTime ]]
   */
  def ts1(t: java.time.LocalDateTime): Column = lit(t.toString).cast(TimestampType)

  /**
   * transform sequence of subfeeds
   */
  def transformSubfeeds(subFeeds: Seq[SparkSubFeed], transformer: DataFrame => DataFrame): Seq[SparkSubFeed] = {
    subFeeds.map( subFeed => subFeed.copy( dataFrame = Some(transformer(subFeed.dataFrame.get))))
  }

  def dropDuplicates(pks: Seq[String])(df: DataFrame): DataFrame = {
    df.dropDuplicates(pks)
  }

  /**
   * applies multiple transformations to a sequence of subfeeds
   */
  def multiTransformSubfeeds( subFeeds: Seq[SparkSubFeed], transformers: Seq[DataFrame => DataFrame]): Seq[SparkSubFeed] = {
    transformers.foldLeft( subFeeds ){
      case (subFeed, transform) => transformSubfeeds( subFeed, transform )
    }
  }

  /**
   * applies multiple transformations to a sequence of subfeeds
   */
  def multiTransformSubfeed( subFeed: SparkSubFeed, transformers: Seq[DataFrame => DataFrame]): SparkSubFeed = {
    transformers.foldLeft( subFeed ){
      case (subFeed, transform) => subFeed.copy( dataFrame = Some(transform(subFeed.dataFrame.get )))
    }
  }

  /**
   * applies the transformers
   */
  def applyCustomTransformation(inputSubFeed: SparkSubFeed, transformer: Option[CustomDfTransformerConfig])(implicit session: SparkSession): SparkSubFeed = transformer.map {
    transformer =>
      val transformedDf = transformer.transform(inputSubFeed.dataFrame.get, inputSubFeed.dataObjectId)
      inputSubFeed.copy(dataFrame = Some(transformedDf))
  }.getOrElse( inputSubFeed )

  /**
   * applies columnBlackList and columnWhitelist
   */
  def applyBlackWhitelists(subFeed: SparkSubFeed, columnBlacklist: Option[Seq[String]], columnWhitelist: Option[Seq[String]]): SparkSubFeed = {
    val blackWhiteDfTransforms: Seq[DataFrame => DataFrame] = Seq(
      columnBlacklist.map(l => filterBlacklist(l) _),
      columnWhitelist.map(l => filterWhitelist(l) _)
    ).flatten
    multiTransformSubfeed(subFeed, blackWhiteDfTransforms)
  }

  /**
    * applies filterClauseExpr
    */
  def applyFilter(subFeed: SparkSubFeed, filterClauseExpr: Option[Column]): SparkSubFeed = {
    val filterDfTransform: Seq[DataFrame => DataFrame] = Seq(
      filterClauseExpr.map(l => ActionHelper.filterRows(l) _)
    ).flatten
    ActionHelper.multiTransformSubfeed(subFeed, filterDfTransform)
  }

  /**
   * applies type casting decimal -> integral/float
   */
  def applyCastDecimal2IntegralFloat(subFeed: SparkSubFeed): SparkSubFeed = ActionHelper.multiTransformSubfeed(subFeed, Seq(_.castAllDecimal2IntegralFloat))

  /**
   * applies an optional additional transformation
   */
  def applyAdditional(subFeed: SparkSubFeed,
                      additional: (SparkSubFeed,Option[DataFrame],Seq[String],LocalDateTime) => SparkSubFeed,
                      output: TableDataObject)(implicit session: SparkSession,
                                               context: ActionPipelineContext): SparkSubFeed = {
    logger.info(s"Starting applyAdditional context=$context")
    val timestamp = context.referenceTimestamp.getOrElse(LocalDateTime.now)
    val table = output.table
    val pks = table.primaryKey
      .getOrElse( throw new ConfigurationException(s"There is no <primary-keys> defined for table ${table.name}."))
    val existingDf = if (output.isTableExisting) {
      Some(output.getDataFrame())
    } else None
    additional(subFeed, existingDf, pks, timestamp)
  }

  /**
   * applies all the transformations above
   */
  def applyTransformations(inputSubFeed: SparkSubFeed,
                           transformer: Option[CustomDfTransformerConfig],
                           columnBlacklist: Option[Seq[String]],
                           columnWhitelist: Option[Seq[String]],
                           standardizeDatatypes: Boolean,
                           output: DataObject,
                           additional: Option[(SparkSubFeed,Option[DataFrame],Seq[String],LocalDateTime) => SparkSubFeed],
                           filterClauseExpr: Option[Column] = None)(
                            implicit session: SparkSession,
                            context: ActionPipelineContext): SparkSubFeed = {

    var transformedSubFeed : SparkSubFeed = applyBlackWhitelists(applyCustomTransformation(inputSubFeed, transformer)(session),
      columnBlacklist: Option[Seq[String]],
      columnWhitelist: Option[Seq[String]]
    )

    if (filterClauseExpr.isDefined) transformedSubFeed = applyFilter(inputSubFeed, filterClauseExpr)

    if (standardizeDatatypes) transformedSubFeed = applyCastDecimal2IntegralFloat(transformedSubFeed) // currently we cast decimals away only but later we may add further type casts
    if (additional.isDefined && output.isInstanceOf[TableDataObject]) {
      transformedSubFeed = applyAdditional(transformedSubFeed, additional.get, output.asInstanceOf[TableDataObject])(session,context)
    }
    // return
    transformedSubFeed
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
   * Updates the partition values of a SubFeed to the partition columns of an output, removing not existing columns from the partition values.
   * Further the transformed DataFrame is validated to have the output's partition columns included.
   *
   * @param output output DataObject
   * @param subFeed SubFeed with transformed DataFrame
   * @return SubFeed with updated partition values.
   */
  def validateAndUpdateSubFeedPartitionValues(output: DataObject, subFeed: SparkSubFeed ): SparkSubFeed = {
    output match {
      case partitionedDO: CanHandlePartitions =>
        // validate output partition columns exist in DataFrame
        validateDataFrameContainsCols(subFeed.dataFrame.get, partitionedDO.partitions, s"for ${output.id}")
        // adapt subfeed
        subFeed.updatePartitionValues(partitionedDO.partitions)
      case _ => subFeed.clearPartitionValues()
    }
  }

  /**
   * Validate that DataFrame contains a given list of columns, throwing an exception otherwise.
   *
   * @param df DataFrame to validate
   * @param columns Columns that must exist in DataFrame
   * @param debugName name to mention in exception
   */
  def validateDataFrameContainsCols(df: DataFrame, columns: Seq[String], debugName: String): Unit = {
    val missingColumns = columns.diff(df.columns)
    assert(missingColumns.isEmpty, s"DataFrame $debugName doesn't include columns $missingColumns")
  }

  /**
   * Filter DataFrame with given partition values
   *
   * @param df DataFrame to filter
   * @param partitionValues partition values to use as filter condition
   * @return filtered DataFrame
   */
  def filterDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues]): DataFrame = {
    val partitionValuesColumn = partitionValues.flatMap(_.keys).distinct
    if (partitionValues.isEmpty) df
    else if (partitionValuesColumn.size == 1) {
      // filter with Sql "isin" expression if only one column
      val filterExpr = col(partitionValuesColumn.head).isin(partitionValues.flatMap(_.elements.values):_*)
      df.where(filterExpr)
    } else {
      // filter with and/or expression if multiple partition columns
      val filterExpr = partitionValues.map(_.getSparkExpr).reduce( (a,b) => a or b)
      df.where(filterExpr)
    }
  }

  /**
   * Enriches SparkSubFeed with DataFrame if not existing
   *
   * @param input input data object.
   * @param subFeed input SubFeed.
   */
  def enrichSubFeedDataFrame(input: DataObject with CanCreateDataFrame, subFeed: SparkSubFeed)(implicit session: SparkSession): SparkSubFeed = {
    if (subFeed.dataFrame.isEmpty) {
      assert(input.id == subFeed.dataObjectId, s"DataObject.Id ${input.id} doesnt match SubFeed.DataObjectId ${subFeed.dataObjectId} ")
      logger.info(s"Getting DataFrame for DataObject ${input.id} filtered by partition values ${subFeed.partitionValues}")
      val df = input.getDataFrame().colNamesLowercase // convert to lower case by default
      val filteredDf = ActionHelper.filterDataFrame(df, subFeed.partitionValues)
      subFeed.copy(dataFrame = Some(filteredDf))
    } else subFeed
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
  def applyExecutionMode(executionMode: Option[ExecutionMode], actionId: ActionObjectId, input: DataObject, output: DataObject, partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Seq[PartitionValues] = {
    executionMode match {
      case Some(mode:PartitionDiffMode) =>
        (input,output) match {
          case (partitionInput: CanHandlePartitions, partitionOutput: CanHandlePartitions)  =>
            if (partitionInput.partitions.nonEmpty) {
              if (partitionOutput.partitions.nonEmpty) {
                // prepare common partition columns
                val commonInits = searchCommonInits(partitionInput.partitions, partitionOutput.partitions)
                require(commonInits.nonEmpty, throw ConfigurationException(s"$actionId has set initExecutionMode = 'partitionDiff' but no common init was found in partition columns for $input and $output"))
                val commonPartitions = if (mode.partitionColNb.isDefined) {
                  commonInits.find(_.size==mode.partitionColNb.get).getOrElse(throw ConfigurationException(s"$actionId has set initExecutionMode = 'partitionDiff' but no common init with ${mode.partitionColNb.get} was found in partition columns of $input and $output from $commonInits!"))
                } else {
                  commonInits.maxBy(_.size)
                }
                // calculate missing partition values
                val partitionValuesToBeProcessed = partitionInput.listPartitions.map(_.filterKeys(commonPartitions)).toSet
                  .diff(partitionOutput.listPartitions.map(_.filterKeys(commonPartitions)).toSet)
                partitionValuesToBeProcessed.toSeq
              } else throw ConfigurationException(s"$actionId has set initExecutionMode = 'partitionDiff' but $output has no partition columns defined!")
            } else throw ConfigurationException(s"$actionId has set initExecutionMode = 'partitionDiff' but $input has no partition columns defined!")
          case (_: CanHandlePartitions, _) => throw ConfigurationException(s"$actionId has set initExecutionMode = 'partitionDiff' but $output does not support partitions!")
          case (_, _) => throw ConfigurationException(s"$actionId has set initExecutionMode = 'partitionDiff' but $input does not support partitions!")
        }
      case _ => partitionValues
    }
  }


  //  /**
  //   * Checks a historized hive table to verify that there is only one active record for any given primary key
  //   *
  //   * @param pks Bestandteile des (Composite) Primary Key einer Tabelle
  //   * @param historizedDf Historisierter [[DataFrame]]
  //   * @throws PrimaryKeyConstraintViolationException when the primary key is not unique.
  //   */
  //  @throws[PrimaryKeyConstraintViolationException]
  //  private def checkPrimaryKeyConstraint(pks: Seq[String], historizedDf: DataFrame): Unit = {
  //    val cols = pks.map(pk => col(pk)) ++ List(col(s"${TechnicalTableColumn.delimited}"))
  //    val surrogateTimestamp = HiveConventions.getHistorizationSurrogateTimestamp
  //
  //    val res = historizedDf.select(cols: _*)
  //      .where(s"${TechnicalTableColumn.delimited} = '${Timestamp.valueOf(surrogateTimestamp)}'")
  //      .groupBy(cols: _*).agg(count("*") as "countActiveKeys")
  //      .where("countActiveKeys > 1")
  //
  //    if (res.count > 0) {
  //      val msg =
  //        s"""
  //           |Primary Keys mit > 1 aktiven Datensätzen:
  //           |${res.collect.mkString("\n")}
  //                """.stripMargin
  //      throw new PrimaryKeyConstraintViolationException(msg)
  //    }
  //  }
}

