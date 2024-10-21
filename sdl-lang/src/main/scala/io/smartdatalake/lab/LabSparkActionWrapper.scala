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

package io.smartdatalake.lab

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, ExcludeFromSchemaExport, FromConfigFactory}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.ProductUtil
import io.smartdatalake.workflow.action.executionMode.ExecutionModeResult
import io.smartdatalake.workflow.action.generic.transformer._
import io.smartdatalake.workflow.action.spark.customlogic.{CustomDfTransformer, CustomDfsTransformer}
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfTransformer
import io.smartdatalake.workflow.action.{CustomDataFrameAction, DataFrameActionImpl, DataFrameOneToOneActionImpl}
import io.smartdatalake.workflow.dataframe.spark.{SparkColumn, SparkDataFrame}
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, InitSubFeed}
import org.apache.spark.sql.{Column, DataFrame}

case class LabSparkDfsActionWrapper[A <: CustomDataFrameAction](action: A, context: ActionPipelineContext) extends LabSparkActionWrapper[A, GenericDfsTransformerDef, CustomDfsTransformer](action, context) {
  override private[smartdatalake] def transform(inputSubFeeds: Seq[DataFrameSubFeed], outputSubFeeds: Seq[DataFrameSubFeed], selectedTransformerIndexes: Option[Seq[Int]], additionalTransformers: Seq[GenericDfsTransformerDef], replacedTransformers: Map[Int, GenericDfsTransformerDef], additionalTransformerOptions: Map[Int, Map[String, String]]): Map[String, GenericDataFrame] = {
    val mainPartitionValues = action.getMainPartitionValues(inputSubFeeds)(context)
    val transformers = action.getTransformers(context)
    val selectedTransformerIndexesPrep = selectedTransformerIndexes.map(_.filter(_ < transformers.size)).getOrElse(transformers.indices).toSet
    val transformersToApply = transformers.zipWithIndex
      .filter {case (t,idx) => selectedTransformerIndexesPrep.contains(idx)}
      .map { case (t, idx) =>
        replacedTransformers.getOrElse(idx, t) match {
          case t: OptionsGenericDfsTransformer if additionalTransformerOptions.isDefinedAt(idx) =>
            val configuredOptions = ProductUtil.getFieldData[Map[String, String]](t.asInstanceOf[Product], "options").getOrElse(Map())
            ProductUtil.dynamicCopy(t, "options", configuredOptions ++ additionalTransformerOptions(idx))
          case t if additionalTransformerOptions.isDefinedAt(idx) =>
            throw new IllegalStateException(s"additionalTransformerOptions defined for idx $idx but transformer of type ${t.getClass.getSimpleName} can not handle options")
          case t => t
        }
      } ++ additionalTransformers
    println(s"Transformers applied: ${transformersToApply.map(_.name).mkString(", ")}")
    // apply transformers
    action.applyTransformers(transformersToApply, mainPartitionValues, inputSubFeeds)(context)
  }
  override private[smartdatalake] def createLabTransformer(customTransformer: CustomDfsTransformer): GenericDfsTransformerDef = {
    LabSparkDfsTransformer(customTransformer = customTransformer)
  }
}

case class LabSparkDfActionWrapper[A <: DataFrameOneToOneActionImpl](action: A, context: ActionPipelineContext) extends LabSparkActionWrapper[A, GenericDfTransformerDef, CustomDfTransformer](action, context) {
  override private[smartdatalake] def transform(inputSubFeeds: Seq[DataFrameSubFeed], outputSubFeeds: Seq[DataFrameSubFeed], selectedTransformerIndexes: Option[Seq[Int]], additionalTransformers: Seq[GenericDfTransformerDef], replacedTransformers: Map[Int, GenericDfTransformerDef], additionalTransformerOptions: Map[Int, Map[String, String]]): Map[String, GenericDataFrame] = {
    val transformers = action.getTransformers(context)
    val selectedTransformerIndexesPrep = selectedTransformerIndexes.map(_.filter(_ < transformers.size)).getOrElse(transformers.indices).toSet
    val transformersToApply = transformers.zipWithIndex
      .filter {case (t,idx) => selectedTransformerIndexesPrep.contains(idx)}
      .map { case (t, idx) =>
        replacedTransformers.getOrElse(idx, t) match {
          case t: OptionsGenericDfTransformer if additionalTransformerOptions.isDefinedAt(idx) =>
            val configuredOptions = ProductUtil.getFieldData[Map[String, String]](t.asInstanceOf[Product], "options").getOrElse(Map())
            ProductUtil.dynamicCopy(t, "options", configuredOptions ++ additionalTransformerOptions(idx))
          case t if additionalTransformerOptions.isDefinedAt(idx) =>
            throw new IllegalStateException(s"additionalTransformerOptions defined for idx $idx but transformer of type ${t.getClass.getSimpleName} can not handle options")
          case t => t
        }
      } ++ additionalTransformers
    println(s"Transformers applied: ${transformersToApply.map(_.name).mkString(", ")}")
    val outputSubFeed = action.applyTransformers(transformersToApply, inputSubFeeds.head, outputSubFeeds.head)(context)
    Map(outputSubFeed.dataObjectId.id -> outputSubFeed.dataFrame.get)
  }
  override private[smartdatalake] def createLabTransformer(customTransformer: CustomDfTransformer): GenericDfTransformerDef = {
    LabSparkDfTransformer(customTransformer = customTransformer)
  }
}

/**
 * A wrapper around a Spark Action simplifying debugging and extending transformation logic.
 * @tparam A: Action type
 * @tparam T: Transformer type used by this Action type
 * @tparam S: Scala Customer Transformer type used for this Action type.
 */
abstract class LabSparkActionWrapper[A <: DataFrameActionImpl, T <: Transformer, S](action: A, context: ActionPipelineContext) {

  /**
   * Get DataFrames used in Action.
   * Parameters can be used to control if input or transformed DataFrames are returned.
   *
   * @param partitionValues optional partition values used to filter input DataFrames.
   * @param filters optional yMap of column name and filter expressions, used to filter DataFrames created that contain the column name in their schema.
   * @param selectedTransformerIndexes index of transformers configured for this Action that should be applied.
   *                                   None -> all transformers are applied (Default).
   * @param postProcessOutputSubFeeds if post processing of SubFeeds should be applied. Default = false.
   *                                  PostProcessing will filter DataFrames created by Transformations to configured outputs, and let outputs post-process SubFeeds.
   * @param additionalTransformers transformers to apply after existing transformers of the Action.
   *                               Note that these transformers are appended after existing transformers are selected by using selectedTransformerIndexes.
   * @param replacedTransformers         transformers to apply at the index of another configured transformers of the Action.
   *                                     The index of the transformer is starting from 0.
   * @param additionalTransformerOptions additional transformer options to add to transformer at given index.
   *                                     The index of the transformer is starting from 0.
   * @return a Map of ids and DataFrames.
   */
  def getDataFrames(partitionValues: Seq[PartitionValues] = Seq(), filters: Map[String, Column] = Map(), selectedTransformerIndexes: Option[Seq[Int]] = None, postProcessOutputSubFeeds: Boolean = false, additionalTransformers: Seq[T] = Seq(), replacedTransformers: Map[Int, T] = Map(), additionalTransformerOptions: Map[Int, Map[String, String]] = Map()): Map[String, DataFrame] = {
    getGenericDataFrames(partitionValues, filters.mapValues(SparkColumn).toMap, selectedTransformerIndexes, postProcessOutputSubFeeds, additionalTransformers, replacedTransformers, additionalTransformerOptions)
      .collect{case (id, df: SparkDataFrame) => (id,df.inner)}
  }

  def simulateExecutionMode(partitionValues: Seq[PartitionValues] = Seq()): Option[ExecutionModeResult] = {
    action.executionMode match {
      case Some(executionMode) =>
        implicit val implicitContext: ActionPipelineContext = context
        // prepare
        val emptyInputSubFeeds = (action.inputs ++ action.recursiveInputs).map(i => InitSubFeed(i.id, partitionValues, isSkipped = false, metrics = None))
        val mainInput = action.getMainInput(emptyInputSubFeeds)
        val mainSubFeed = emptyInputSubFeeds.find(_.dataObjectId == mainInput.id).get
        val inputSubFeeds = emptyInputSubFeeds.map { subFeed =>
          val partitionValues = if (mainSubFeed.partitionValues.nonEmpty) Some(mainSubFeed.partitionValues) else None
          action.updateInputPartitionValues(action.inputMap(subFeed.dataObjectId), action.subFeedConverter().fromSubFeed(subFeed), partitionValues)
        }
        // apply execution mode
        val mainInputSubFeed = inputSubFeeds.find(_.dataObjectId == mainInput.id).get
        executionMode.apply(action.id, mainInput, action.mainOutput, mainInputSubFeed, action.transformPartitionValues)
      case None => throw NotSupportedException(action.id, "has no ExecutionMode defined")
    }
  }

  /**
   * Use a Builder to configure and get DataFrames from this Action.
   */
  def buildDataFrames: GetDataFrameBuilder = GetDataFrameBuilder()

  case class GetDataFrameBuilder private (
                                           override val partitionValues: Seq[PartitionValues] = Seq(),
                                           override val filters: Map[String,Column] = Map(),
                                           selectedTransformerIndexes: Option[Seq[Int]] = None,
                                           postProcessOutputSubFeeds: Boolean = false,
                                           additionalTransformers: Seq[T] = Seq(),
                                           replacedTransformers: Map[Int,T] = Map(),
                                           additionalTransformerOptions: Map[Int, Map[String, String]] = Map()
                                         ) extends DataFrameBaseBuilder[GetDataFrameBuilder] {

    /**
     * Get input DataFrames only, do not apply transformers.
     */
    def withoutTransformers: GetDataFrameBuilder = copy(selectedTransformerIndexes = Some(Seq()))

    /**
     * From the transformers configured for this action, choose how many that should be applied.
     * Note that the order of the transformers can not be changed.
     * Default is to apply all transformers.
     */
    def withLimitedTransformerNb(limit: Int): GetDataFrameBuilder = copy(selectedTransformerIndexes = Some(0 until limit))

    /**
     * Enable SubFeed postprocessing. Default is to not apply SubFeed postprocessing.
     * PostProcessing will filter DataFrames created by Transformations to configured outputs, and let outputs post-process SubFeeds.
     */
    def withPostProcessOutputSubFeeds: GetDataFrameBuilder = copy(postProcessOutputSubFeeds = true)

    /**
     * Replace transformer at configured at given index of this action.
     *
     * @param idx : index of the transformer, starting from 0
     */
    def withReplacedTransformer(idx: Int, transformer: T): GetDataFrameBuilder = copy(replacedTransformers = replacedTransformers + (idx -> transformer))

    /**
     * Replace transformer at configured at given index of this action with custom transformer.
     *
     * @param idx : index of the transformer, starting from 0
     */
    def withReplacedTransformer(idx: Int, transformer: S): GetDataFrameBuilder = copy(replacedTransformers = replacedTransformers + (idx -> createLabTransformer(transformer)))

    /**
     * Add additional transformer to be applied after transformers of this action.
     */
    def withAdditionalTransformer(transformer: T): GetDataFrameBuilder = copy(additionalTransformers = additionalTransformers :+ transformer)

    /**
     * Add additional custom transformer to be applied after transformers of this action.
     */
    def withAdditionalTransformer(transformer: S): GetDataFrameBuilder = copy(additionalTransformers = additionalTransformers :+ createLabTransformer(transformer))

    /**
     * Add additional transformer options to transformer at given index.
     *
     * @param idx : index of the transformer, starting from 0
     */
    def withAdditionalTransformerOptions(idx: Int, options: Map[String, String]): GetDataFrameBuilder = copy(additionalTransformerOptions = additionalTransformerOptions + (idx -> options))

    /**
     * Get DataFrames using selected options.
     */
    def get: Map[String,DataFrame] = {
      val dfs = getDataFrames(partitionValues, filters, selectedTransformerIndexes, postProcessOutputSubFeeds, additionalTransformers, replacedTransformers, additionalTransformerOptions)
      println(s"DataFrames built: ${dfs.keys.mkString(", ")}")
      dfs
    }

    override protected def setPartitionValues(partitionValues: Seq[PartitionValues]): GetDataFrameBuilder = copy(partitionValues = partitionValues)

    override protected def setFilters(filters: Map[String, Column]): GetDataFrameBuilder = copy(filters = filters)
  }

  private[smartdatalake] def getGenericDataFrames(partitionValues: Seq[PartitionValues] = Seq(), filters: Map[String, GenericColumn] = Map(), selectedTransformerIndexes: Option[Seq[Int]] = None, postProcessOutputSubFeeds: Boolean = false, additionalTransformers: Seq[T] = Seq(), replacedTransformers: Map[Int, T] = Map(), additionalTransformerOptions: Map[Int, Map[String, String]] = Map()): Map[String, GenericDataFrame] = {
    assert(!selectedTransformerIndexes.exists(_.isEmpty) || !postProcessOutputSubFeeds, "selectedTransformerIndexes=empty and postProcessOutputSubFeeds=true can not be set together")

    // prepare
    val emptyInputSubFeeds = (action.inputs ++ action.recursiveInputs).map(i => InitSubFeed(i.id, partitionValues, isSkipped = false, metrics = None))
    var (inputSubFeeds, outputSubFeeds) = action.prepareInputSubFeeds(emptyInputSubFeeds)(context)

    // filter
    inputSubFeeds = filters.foldLeft(inputSubFeeds){
      case (subFeeds, (column, filterExpr)) =>
        subFeeds.map(f => if (f.schema.toSeq.flatMap(_.columns).contains(column)) f.withDataFrame(f.dataFrame.map(_.filter(filterExpr))) else f)
    }

    // transform
    val outputDataFrames = transform(inputSubFeeds, outputSubFeeds, selectedTransformerIndexes, additionalTransformers, replacedTransformers, additionalTransformerOptions)

    // check and adapt output SubFeeds
    if (postProcessOutputSubFeeds) {
      outputSubFeeds = outputSubFeeds.map { subFeed=>
        val df = outputDataFrames.getOrElse(subFeed.dataObjectId.id, throw ConfigurationException(s"(${action.id}) No result found for output ${subFeed.dataObjectId}. Available results are ${outputDataFrames.keys.mkString(", ")}."))
        subFeed.withDataFrame(Some(df))
      }
      outputSubFeeds = action.postprocessOutputSubFeeds(outputSubFeeds, inputSubFeeds)(context)
      outputSubFeeds.map(s => (s.dataObjectId.id, s.dataFrame.get)).toMap
    } else {
      outputDataFrames
    }
  }

  /**
   * To override by subclasses to handle their specific transformer classes.
   */
  private[smartdatalake] def transform(inputSubFeeds: Seq[DataFrameSubFeed], outputSubFeeds: Seq[DataFrameSubFeed], selectedTransformerIndexes: Option[Seq[Int]], additionalTransformers: Seq[T], replacedTransformers: Map[Int, T], additionalTransformerOptions: Map[Int, Map[String, String]]): Map[String, GenericDataFrame]

  /**
   * To override by subclasses to create create a transformer from a custom scala transformer class.
   */
  private[smartdatalake] def createLabTransformer(customTransformer: S): T
}

private case class LabSparkDfsTransformer(override val name: String = "labScalaSparkTransform", customTransformer: CustomDfsTransformer, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsSparkDfsTransformer with ExcludeFromSchemaExport {
  override val description: Option[String] = None
  override def transformSparkWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String, DataFrame], options: Map[String, String])(implicit context: ActionPipelineContext): Map[String, DataFrame] = {
    customTransformer.transform(context.sparkSession, options, dfs)
  }
  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] = {
    customTransformer.transformPartitionValues(options, partitionValues)
  }
  override def factory: FromConfigFactory[GenericDfsTransformer] = throw new NotImplementedError()
}

private case class LabSparkDfTransformer(override val name: String = "labScalaSparkTransform", customTransformer: CustomDfTransformer, options: Map[String, String] = Map(), runtimeOptions: Map[String, String] = Map()) extends OptionsSparkDfTransformer with ExcludeFromSchemaExport {
  override val description: Option[String] = None
  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId, options: Map[String, String])(implicit context: ActionPipelineContext): DataFrame = {
    customTransformer.transform(context.sparkSession, options, df, dataObjectId.id)
  }
  override def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues,PartitionValues]] = {
    customTransformer.transformPartitionValues(options, partitionValues)
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = ScalaClassSparkDfTransformer
}