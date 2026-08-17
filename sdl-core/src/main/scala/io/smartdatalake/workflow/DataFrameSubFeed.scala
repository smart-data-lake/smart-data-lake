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
package io.smartdatalake.workflow

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{ProductUtil, ReflectionUtil, ScalaUtil}
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, SchemaValidation, UserDefinedSchema}
import io.smartdatalake.workflow.dataobject.DataObject
import org.reflections.Reflections

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.Type
import scala.reflect.runtime.universe.TypeTag

/**
 * A SubFeed that holds a DataFrame
 */
trait DataFrameSubFeed extends SubFeed {
  @transient
  def tpe: Type // concrete type of this DataFrameSubFeed
  implicit lazy val companion: DataFrameSubFeedCompanion = DataFrameSubFeed.getCompanion(tpe)
  /**
   * DataFrame transported by this SubFeed.
   * It is only defined if the producing Action cached it (cacheOutput=true), otherwise the consuming Action
   * reads a fresh DataFrame from the DataObject.
   */
  def dataFrame: Option[GenericDataFrame]
  def observation: Option[DataFrameObservation]

  /**
   * Materialize the DataFrame of this SubFeed, so that reading it again does not recompute it.
   *
   * Note that the returned SubFeed must be used: engines materializing eagerly (e.g. Snowpark `cacheResult`)
   * return a *new* DataFrame and leave the receiver unchanged.
   */
  def cache: DataFrameSubFeed = withDataFrame(dataFrame.map(_.cache))

  /**
   * Release a DataFrame materialized by [[cache]]. Not all engines support this, see [[DataFrameSubFeedCompanion.canUncacheDataFrame]].
   */
  def uncache: DataFrameSubFeed = withDataFrame(dataFrame.map(_.uncache))

  /**
   * Schema kept when this SubFeed transports no DataFrame, e.g. after breakLineage or when crossing
   * an engine boundary. Do not read this directly, use [[schema]] or [[schemaOpt]] instead.
   * Note: implementations must declare this @transient, so it is not written to the run state.
   */
  private[smartdatalake] def keptSchema: Option[GenericSchema]

  /**
   * Schema of the data transported by this SubFeed, if it is already known.
   * It is only unknown for SubFeeds at the start of the DAG which did not yet enter an Action.
   */
  def schemaOpt: Option[GenericSchema] = dataFrame.map(_.schema).orElse(keptSchema)

  /**
   * Schema of the data transported by this SubFeed.
   * This is always defined once the SubFeed has been enriched with a DataFrame by its Action,
   * see DataFrameActionImpl.enrichSubFeedDataFrame. Use [[schemaOpt]] before that, e.g. for SubFeeds
   * at the start of the DAG which do not yet know their schema.
   */
  def schema: GenericSchema = schemaOpt.getOrElse(
    throw new IllegalStateException(s"($dataObjectId) schema of this SubFeed is not yet initialized")
  )

  /**
   * Column-bound filters transported by this SubFeed, at most one per column.
   * They are applied when the DataFrame is (re)created, and only if their column exists, see [[applyFilter]].
   */
  def filters: Seq[ColumnFilter]

  def hasFilters: Boolean = filters.nonEmpty

  /**
   * Filters to keep when merging two SubFeeds for the same DataObject.
   * Only filters present in both SubFeeds are kept: dropping a filter widens the data read, whereas keeping a filter
   * which is not valid for the other SubFeed would lose data.
   */
  def unionFilters(other: SubFeed): Seq[ColumnFilter] = other match {
    case dataFrameSubFeed: DataFrameSubFeed => this.filters.intersect(dataFrameSubFeed.filters)
    case _ => Seq()
  }

  /** true if this SubFeed holds a streaming DataFrame */
  def isStreamingDataFrame: Boolean = dataFrame.exists(_.isStreaming)

  def clearFilters(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    // if filters are removed, normally also the DataFrame must be removed so that the next action get's a fresh unfiltered DataFrame with all data of this DataObject
    val cleared = ProductUtil.dynamicCopy(ProductUtil.dynamicCopy(this, "filters", Seq.empty[ColumnFilter]), "observation", None)
    if (breakLineageOnChange && hasFilters) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearFilters")
      cleared.breakLineage
    } else cleared
  }

  override def breakLineage(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    // The DataFrame is dropped in order to truncate the engines logical plan. The schema is kept, so that subsequent
    // Actions can still validate the lineage of Actions and DataObjects in init phase. A DataFrame is recreated on
    // demand, see DataFrameActionImpl.enrichSubFeedDataFrame.
    // Exception: a simulation run passes the data through in memory without writing it, so it can not be reread from
    // the DataObject and the DataFrame must be kept.
    if (dataFrame.isDefined && !context.simulation) withDataFrame(None) else this
  }

  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    if (breakLineageOnChange && partitionValues.nonEmpty) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearPartitionValues")
      withPartitionValues(Seq()).breakLineage
    } else withPartitionValues(Seq())
  }

  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    withPartitionValues(updatedPartitionValues)
  }
  /**
   * Set the DataFrame of this SubFeed. The schema follows the DataFrame if it is defined,
   * otherwise the currently known schema is kept.
   */
  def withDataFrame(dataFrame: Option[GenericDataFrame]): DataFrameSubFeed

  /**
   * Drop the DataFrame of this SubFeed and transport only the given schema.
   */
  def withSchema(schema: Option[GenericSchema]): DataFrameSubFeed

  def withObservation(observation: Option[DataFrameObservation]): DataFrameSubFeed = {
    ProductUtil.dynamicCopy(this, "observation", observation)
  }

  def withPartitionValues(partitionValues: Seq[PartitionValues]): DataFrameSubFeed = {
    ProductUtil.dynamicCopy(this, "partitionValues", partitionValues)
  }

  /**
   * Set the filters of this SubFeed, replacing existing ones. This does not apply them, see [[applyFilter]].
   */
  def withFilters(filters: Seq[ColumnFilter]): DataFrameSubFeed = {
    ProductUtil.dynamicCopy(this, "filters", filters)
  }

  /**
   * Add filters to this SubFeed. A filter for a column which already has one replaces it, see [[ColumnFilter.merge]].
   */
  def addFilters(added: Seq[ColumnFilter]): DataFrameSubFeed = {
    withFilters(ColumnFilter.merge(filters, added, s"($dataObjectId)"))
  }

  /**
   * Restrict the filters of this SubFeed to the columns existing in the given schema, analogous to
   * [[updatePartitionValues]]. Filters for non-existing columns are dropped, as they do not apply to the
   * corresponding DataObject.
   */
  def updateFilters(schema: GenericSchema): DataFrameSubFeed = {
    val updatedFilters = ColumnFilter.filterExistingColumns(filters, schema)
    val droppedFilters = filters.diff(updatedFilters)
    if (droppedFilters.nonEmpty) logger.debug(s"($dataObjectId) filters ${ColumnFilter.describe(droppedFilters)}" +
      s" are dropped because their column does not exist")
    withFilters(updatedFilters)
  }

  def withFilters(partitionValues: Seq[PartitionValues], filters: Seq[ColumnFilter]): DataFrameSubFeed = {
    withPartitionValues(partitionValues).withFilters(filters)
      .applyFilter
  }
  def applyFilter: DataFrameSubFeed = {
    // apply partition filter
    val partitionValuesColumn = partitionValues.flatMap(_.keys).distinct
    val dfPartitionFiltered = if (partitionValues.isEmpty) dataFrame
    else if (partitionValuesColumn.size == 1) {
      // filter with Sql "isin" expression if only one column
      val filterExpr = companion.col(partitionValuesColumn.head).isin(partitionValues.flatMap(_.elements.values):_*)
      dataFrame.map(_.filter(filterExpr))
    } else {
      // filter with and/or expression if multiple partition columns
      val filterExpr = PartitionValues.createFilterExpr(partitionValues)
      dataFrame.map(_.filter(filterExpr))
    }
    // apply column filters. A filter is only applied if its column exists in the DataFrame, so that an ExecutionMode
    // can push a filter to all inputs of an Action.
    // Note that df.schema is used deliberately and not schemaOpt: it is the schema the expression is evaluated
    // against, and filtering only ever happens if a DataFrame is present.
    val dfResult = dfPartitionFiltered.map { df =>
      ColumnFilter.filterExistingColumns(filters, df.schema)
        .foldLeft(df)((dfAcc, f) => dfAcc.filter(companion.expr(f.expression)))
    }
    // return updated SubFeed
    withDataFrame(dfResult)
  }

  def transform(transformer: GenericDataFrame => GenericDataFrame): DataFrameSubFeed = withDataFrame(dataFrame.map(transformer))

  def movePartitionColumnsLast(partitions: Seq[String]): DataFrameSubFeed = {
    withDataFrame(dataFrame.map(x => x.movePartitionColsLast(partitions)))
  }
}

trait DataFrameSubFeedCompanion extends SubFeedConverter[DataFrameSubFeed] with DataFrameFunctions {
  protected def subFeedType: universe.Type

  /**
   * true if this engine can materialize a DataFrame, e.g. [[GenericDataFrame.cache]] is not a no-op.
   */
  def canCacheDataFrame: Boolean = false

  /**
   * true if [[GenericDataFrame.cache]] releases the materialized data again.
   * Note that Snowpark creates a temporary table which can only be released by closing the session.
   */
  def canUncacheDataFrame: Boolean = false

  /**
   * Get the read schema of a DataObject from its configuration, without accessing the DataObject itself.
   * Returns None if no schema is declared anywhere, in which case the DataObject has to be asked - see [[getDataObjectSchema]].
   * If SubFeed subtypes have DataObjects with other methods to create a schema, they can override this method.
   */
  def getDeclaredDataObjectSchema(dataObject: DataObject with CanCreateDataFrame)(implicit context: ActionPipelineContext): Option[GenericSchema] = {
    val schema = dataObject match {
      case input: UserDefinedSchema if input.schema.isDefined => input.schema
      case input: SchemaValidation if input.schemaMin.isDefined => input.schemaMin
      case _ if context.globalConfig.dataObjectsSchemaSource.isDefined && !context.isExecPhase =>
        context.globalConfig.getSchemaFromSource(dataObject.id)(context.hadoopConf)
      case _ => None
    }
    schema.map(dataObject.createReadSchema)
  }

  /**
   * Get an empty DataFrame with a defined schema.
   * @param dataObjectId Snowpark implementation needs to get the Snowpark-Session from the DataObject. This should not be used otherwise.
   */
  def getEmptyDataFrame(schema: GenericSchema, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame
  def getSubFeed(dataFrame: GenericDataFrame, dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrameSubFeed

  /**
   * Create a SubFeed which transports only a schema and no DataFrame.
   * The consuming Action recreates a DataFrame on demand, see DataFrameActionImpl.enrichSubFeedDataFrame.
   */
  def getSchemaSubFeed(dataObjectId: DataObjectId, schema: GenericSchema, partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrameSubFeed
  def createSchema(fields: Seq[GenericField]): GenericSchema

  def createSchemaFromDdl(ddl: String): GenericSchema = throw new UnsupportedOperationException(s"createSchemaFromDdl is not supported for ${subFeedType.typeSymbol.name}")

  def createField(name: String, dataType: GenericDataType, nullable: Boolean, comment: Option[String]): GenericField

  def createSimpleDataType(tpe: String): GenericDataType with GenericSimpleDataType

  def createStructDataType(fields: Seq[GenericField]): GenericDataType with GenericStructDataType

  def createArrayDataType(valueTpe: GenericDataType): GenericDataType with GenericArrayDataType

  def createMapDataType(keyTpe: GenericDataType, valueTpe: GenericDataType): GenericDataType with GenericMapDataType

  def createDataFrame[A <: Product: ClassTag: TypeTag](rows: Seq[A])(implicit context: ActionPipelineContext): GenericDataFrame
  def createDataFrame[A <: Product: ClassTag: TypeTag](rows: Seq[A], colNames: Seq[String])(implicit context: ActionPipelineContext): GenericDataFrame

  /**
   * Names of this engine's currently active streaming queries, if any.
   * Default: no-op, as most engines do not support streaming.
   */
  def getStreamingQueryNames(implicit context: ActionPipelineContext): Seq[String] = Seq()

  /**
   * Block until any of this engine's active streaming queries terminates, re-throwing its exception if it failed.
   * Default: no-op, as most engines do not support streaming.
   * @param timeoutMs if defined, return after this timeout even if no query has terminated; otherwise wait indefinitely.
   */
  def awaitAnyStreamingQueryTermination(timeoutMs: Option[Long] = None)(implicit context: ActionPipelineContext): Unit = ()

  /**
   * Stop all of this engine's active streaming queries.
   * Default: no-op, as most engines do not support streaming.
   */
  def stopStreamingQueries()(implicit context: ActionPipelineContext): Unit = ()

  object implicits {
    implicit class ProductExtensions[A <: Product: ClassTag: TypeTag](rows: Seq[A]) {
      def toDF(implicit context: ActionPipelineContext): GenericDataFrame = {
        createDataFrame(rows)
      }
      def toDF(colName: String, colNames: String*)(implicit context: ActionPipelineContext): GenericDataFrame = {
        createDataFrame(rows, colName +: colNames)
      }
    }
  }
}

object DataFrameSubFeed {
  def getCompanion(tpe: Type): DataFrameSubFeedCompanion = ScalaUtil.companionOf[DataFrameSubFeedCompanion](tpe)
  private[smartdatalake] def getCompanion(fullTpeName: String): DataFrameSubFeedCompanion = ScalaUtil.companionOf[DataFrameSubFeedCompanion](fullTpeName)

  /**
   * Get implementation of generic DataFrameFunctions.
   */
  def getFunctions(tpe: Type): DataFrameFunctions = getCompanion(tpe) // down cast to reduce interface
  private[smartdatalake] def getFunctions(fullTpeName: String): DataFrameFunctions = ScalaUtil.companionOf[DataFrameFunctions](fullTpeName)

  /**
   * Helper method to throw exception for wrong subfeed type including method name of caller
   */
  private[smartdatalake] def throwIllegalSubFeedTypeException(obj: GenericTypedObject): Nothing = {
    val parentMethod = Thread.currentThread().getStackTrace.drop(2).find(_.getClassName.startsWith("io.smartdatalake")).map(_.getMethodName).getOrElse("<unknown>")
    throw new IllegalStateException(s"Unsupported subFeedType ${obj.subFeedType.typeSymbol.name} in method $parentMethod")
  }

  /**
   * Helper method to assert subfeed type for a list of generic objects, throwing exception including method name of caller
   */
  private[smartdatalake] def assertCorrectSubFeedType(expectedTpe: Type, elements: Seq[GenericTypedObject]): Unit = {
    val parentMethod = Thread.currentThread().getStackTrace.drop(2).find(_.getClassName.startsWith("io.smartdatalake")).map(_.getMethodName).getOrElse("<unknown>")
    assert(elements.forall(_.subFeedType =:= expectedTpe), s"Unsupported subFeedType(s) ${elements.filter(c => !(c.subFeedType =:= expectedTpe)).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method $parentMethod")
  }

  @transient private[smartdatalake] lazy val getKnownSubFeedTypes: Seq[Type] = {
    implicit val reflections: Reflections = ReflectionUtil.getReflections("io.smartdatalake")
    ReflectionUtil.getTraitImplClasses[DataFrameSubFeed]
      .map(ReflectionUtil.classToType)
  }
}