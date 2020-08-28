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

import java.time.LocalDateTime

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions.{ExecutionMode, FailIfNoPartitionValuesMode, PartitionDiffMode, SparkIncrementalMode, SparkStreamingOnceMode}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.DataFrameUtil
import io.smartdatalake.util.misc.DataFrameUtil.DfSDL
import io.smartdatalake.util.streaming.DummyStreamProvider
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.action.ActionHelper.{filterBlacklist, filterWhitelist}
import io.smartdatalake.workflow.action.customlogic.CustomDfTransformerConfig
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase, SparkSubFeed, SubFeed}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

private[smartdatalake] abstract class SparkAction extends Action {

  /**
   * Stop propagating input DataFrame through action and instead get a new DataFrame from DataObject.
   * This can help to save memory and performance if the input DataFrame includes many transformations from previous Actions.
   * The new DataFrame will be initialized according to the SubFeed's partitionValues.
   */
  def breakDataFrameLineage: Boolean

  /**
   * Force persisting DataFrame on Disk.
   * This helps to reduce memory needed for caching the DataFrame content and can serve as a recovery point in case an task get's lost.
   */
  def persist: Boolean

  /**
   * execution mode for this action.
   */
  def executionMode: Option[ExecutionMode]

  override def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.prepare
    executionMode.foreach(_.prepare(id))
  }

  /**
   * Enriches SparkSubFeed with DataFrame if not existing
   *
   * @param input input data object.
   * @param subFeed input SubFeed.
   */
  def enrichSubFeedDataFrame(input: DataObject with CanCreateDataFrame, subFeed: SparkSubFeed, phase: ExecutionPhase)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    assert(input.id == subFeed.dataObjectId, s"($id) DataObject.Id ${input.id} doesnt match SubFeed.DataObjectId ${subFeed.dataObjectId} ")
    executionMode match {
      case Some(m: SparkStreamingOnceMode) if !context.simulation =>
        if (subFeed.dataFrame.isEmpty || phase==ExecutionPhase.Exec) { // in exec phase we always needs a fresh streaming DataFrame
          // recreate DataFrame from DataObject
          assert(input.isInstanceOf[CanCreateStreamingDataFrame], s"($id) DataObject ${input.id} doesn't implement CanCreateStreamingDataFrame. Can not create StreamingDataFrame for executionMode=SparkStreamingOnceMode")
          logger.info(s"getting streaming DataFrame for ${input.id}")
          val df = input.asInstanceOf[CanCreateStreamingDataFrame].getStreamingDataFrame(m.inputOptions, subFeed.dataFrame.map(_.schema))
            .colNamesLowercase // convert to lower case by default
          subFeed.copy(dataFrame = Some(df), partitionValues = Seq()) // remove partition values for streaming mode
        } else if (subFeed.isStreaming.contains(false)) {
          // convert to dummy streaming DataFrame
          val emptyStreamingDataFrame = subFeed.dataFrame.map(df => DummyStreamProvider.getDummyDf(df.schema))
          subFeed.copy(dataFrame = emptyStreamingDataFrame, partitionValues = Seq()) // remove partition values for streaming mode
        } else subFeed
      case _ =>
        if (phase==ExecutionPhase.Exec && (subFeed.dataFrame.isEmpty || subFeed.isDummy || subFeed.isStreaming.contains(true))) {
          // validate partition values existing for input
          input match {
            case partitionedInput: DataObject with CanHandlePartitions if subFeed.partitionValues.nonEmpty && (context.phase==ExecutionPhase.Exec || subFeed.isDAGStart) =>
              val expectedPartitions = partitionedInput.filterExpectedPartitionValues(subFeed.partitionValues)
              val missingPartitionValues = PartitionValues.checkExpectedPartitionValues(partitionedInput.listPartitions, expectedPartitions)
              assert(missingPartitionValues.isEmpty, s"($id) partitions $missingPartitionValues missing for ${input.id}")
            case _ => Unit
          }
          // recreate DataFrame from DataObject
          logger.info(s"($id) getting DataFrame for ${input.id}" + (if (subFeed.partitionValues.nonEmpty) s" filtered by partition values ${subFeed.partitionValues.mkString(" ")}" else ""))
          val df = input.getDataFrame(subFeed.partitionValues)
            .colNamesLowercase // convert to lower case by default
          val filteredDf = filterDataFrame(df, subFeed.partitionValues, subFeed.getFilterCol)
          subFeed.copy(dataFrame = Some(filteredDf))
        } else if (subFeed.dataFrame.isEmpty) {
          // create a dummy dataframe if possible as we are not in exec phase
          val schema = input match {
            case sparkFileInput: SparkFileDataObject => sparkFileInput.readSchema(false)
            case userDefInput: UserDefinedSchema => userDefInput.schema
            case _ => None
          }
          val df = schema.map( s => DataFrameUtil.getEmptyDataFrame(s))
            .getOrElse(input.getDataFrame(subFeed.partitionValues))
            .colNamesLowercase // convert to lower case by default
          val filteredDf = filterDataFrame(df, subFeed.partitionValues, subFeed.getFilterCol)
          subFeed.copy(dataFrame = Some(filteredDf))
        } else if (subFeed.isStreaming.contains(true)) {
          // convert to empty normal DataFrame
          val emptyNormalDataFrame = subFeed.dataFrame.map(df => DataFrameUtil.getEmptyDataFrame(df.schema))
          subFeed.copy(dataFrame = emptyNormalDataFrame)
        } else subFeed
    }
  }

  /**
   * writes subfeed to output respecting given execution mode
   * @return true if no data was transfered, otherwise false
   */
  def writeSubFeed(subFeed: SparkSubFeed, output: DataObject with CanWriteDataFrame)(implicit session: SparkSession): Boolean = {
    executionMode match {
      case Some(m: SparkStreamingOnceMode) =>
        // Write in streaming mode - use spark streaming with Trigger.Once and awaitTermination
        assert(subFeed.dataFrame.get.isStreaming, s"($id) ExecutionMode ${m.getClass} needs streaming DataFrame in SubFeed")
        val streamingQuery = output.writeStreamingDataFrame(subFeed.dataFrame.get, Trigger.Once, m.outputOptions, m.checkpointLocation, s"$id writing ${output.id}", m.outputMode)
        streamingQuery.awaitTermination
        val noData = streamingQuery.lastProgress.numInputRows == 0
        if (noData) logger.info(s"($id) no data to process for ${output.id} in streaming mode")
        // return
        noData
      case None | Some(_: PartitionDiffMode) | Some(_: SparkIncrementalMode) | Some(_: FailIfNoPartitionValuesMode) =>
        // Write in batch mode
        assert(!subFeed.dataFrame.get.isStreaming, s"($id) Input from ${subFeed.dataObjectId} is a streaming DataFrame, but executionMode!=${SparkStreamingOnceMode.getClass.getSimpleName}")
        output.writeDataFrame(subFeed.dataFrame.get, subFeed.partitionValues)
        // return noData
        false
      case x => throw new IllegalStateException( s"($id) ExecutionMode $x is not supported")
    }
  }

  /**
   * transform sequence of subfeeds
   */
  def transformSubfeeds(subFeeds: Seq[SparkSubFeed], transformer: DataFrame => DataFrame): Seq[SparkSubFeed] = {
    subFeeds.map( subFeed => subFeed.copy( dataFrame = Some(transformer(subFeed.dataFrame.get))))
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
    val filterDfTransform = filterClauseExpr.map( expr => (df: DataFrame) => df.where(expr))
    multiTransformSubfeed(subFeed, filterDfTransform.toSeq)
  }

  /**
   * applies type casting decimal -> integral/float
   */
  def applyCastDecimal2IntegralFloat(subFeed: SparkSubFeed): SparkSubFeed = multiTransformSubfeed(subFeed, Seq(_.castAllDecimal2IntegralFloat))

  /**
   * applies an optional additional transformation
   */
  def applyAdditional(subFeed: SparkSubFeed,
                      additional: (SparkSubFeed,Option[DataFrame],Seq[String],LocalDateTime) => SparkSubFeed,
                      output: TableDataObject)(implicit session: SparkSession,
                                               context: ActionPipelineContext): SparkSubFeed = {
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
   * Updates the partition values of a SubFeed to the partition columns of an output, removing not existing columns from the partition values.
   * Further the transformed DataFrame is validated to have the output's partition columns included.
   *
   * @param output output DataObject
   * @param subFeed SubFeed with transformed DataFrame
   * @return SubFeed with updated partition values.
   */
  def validateAndUpdateSubFeedPartitionValues(output: DataObject, subFeed: SparkSubFeed )(implicit session: SparkSession): SparkSubFeed = {
    val updatedSubFeed = output match {
      case partitionedDO: CanHandlePartitions =>
        // validate output partition columns exist in DataFrame
        validateDataFrameContainsCols(subFeed.dataFrame.get, partitionedDO.partitions, s"for ${output.id}")
        // adapt subfeed
        subFeed.updatePartitionValues(partitionedDO.partitions)
      case _ => subFeed.clearPartitionValues()
    }
    updatedSubFeed.clearDAGStart()
  }

  def updateSubFeedAfterWrite(subFeed: SparkSubFeed)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    subFeed.clearFilter // clear filter must be applied after write, because it includes removing the DataFrame
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
   * @param genericFilter filter expression to apply
   * @return filtered DataFrame
   */
  def filterDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], genericFilter: Option[Column]): DataFrame = {
    // apply partition filter
    val partitionValuesColumn = partitionValues.flatMap(_.keys).distinct
    val dfPartitionFiltered = if (partitionValues.isEmpty) df
    else if (partitionValuesColumn.size == 1) {
      // filter with Sql "isin" expression if only one column
      val filterExpr = col(partitionValuesColumn.head).isin(partitionValues.flatMap(_.elements.values):_*)
      df.where(filterExpr)
    } else {
      // filter with and/or expression if multiple partition columns
      val filterExpr = partitionValues.map(_.getSparkExpr).reduce( (a,b) => a or b)
      df.where(filterExpr)
    }
    // apply generic filter
    if (genericFilter.isDefined) dfPartitionFiltered.where(genericFilter.get)
    else dfPartitionFiltered
  }

  /**
   * Applies changes to a SubFeed from a previous action in order to be used as input for this actions transformation.
   */
  def prepareInputSubFeed(subFeed: SparkSubFeed, input: DataObject with CanCreateDataFrame)(implicit session: SparkSession, context: ActionPipelineContext): SparkSubFeed = {
    // persist if requested
    var preparedSubFeed = if (persist) subFeed.persist else subFeed
    // create dummy DataFrame if read schema is different from write schema on this DataObject
    val writeSchema = preparedSubFeed.dataFrame.map(_.schema)
    val readSchema = preparedSubFeed.dataFrame.map(df => input.createReadSchema(df.schema))
    val schemaChanges = writeSchema != readSchema
    preparedSubFeed = if (schemaChanges) preparedSubFeed.convertToDummy(readSchema.get) else preparedSubFeed
    // TODO ZKU: adapt partition values to input's partition cols!
    // break lineage if requested or if it's a streaming DataFrame or if a filter expression is set
    preparedSubFeed = if (breakDataFrameLineage || preparedSubFeed.isStreaming.contains(true) || preparedSubFeed.filter.isDefined) preparedSubFeed.breakLineage else preparedSubFeed
    // return
    preparedSubFeed
  }

}
