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
package io.smartdatalake.workflow

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataFrame, GenericDataType, GenericField, GenericSchema}
import io.smartdatalake.definitions.ExecutionModeResult
import io.smartdatalake.util.dag.DAGResult
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.ScalaUtil.optionalizeMap
import io.smartdatalake.util.misc.{ScalaUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.dataobject._
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.Type

/**
 * A SubFeed transports references to data between Actions.
 * Data can be represented by different technologies like Files or DataFrame.
 */
trait SubFeed extends DAGResult with SmartDataLakeLogger {
  def dataObjectId: DataObjectId
  def partitionValues: Seq[PartitionValues]
  def isDAGStart: Boolean
  def isSkipped: Boolean

  /**
   * Break lineage.
   * This means to discard an existing DataFrame or List of FileRefs, so that it is requested again from the DataObject.
   * On one side this is usable to break long DataFrame Lineages over multiple Actions and instead reread the data from
   * an intermediate table. On the other side it is needed if partition values or filter condition are changed.
   */
  def breakLineage(implicit context: ActionPipelineContext): SubFeed

  def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): SubFeed

  def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): SubFeed

  def clearDAGStart(): SubFeed

  def clearSkipped(): SubFeed

  def toOutput(dataObjectId: DataObjectId): SubFeed

  def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed

  override def resultId: String = dataObjectId.id

  def unionPartitionValues(otherPartitionValues: Seq[PartitionValues]): Seq[PartitionValues] = {
    // union is only possible if both inputs have partition values defined. Otherwise default is no partition values which means to read all data.
    if (this.partitionValues.nonEmpty && otherPartitionValues.nonEmpty) (this.partitionValues ++ otherPartitionValues).distinct
    else Seq()
  }

  def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): SubFeed
  def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): SubFeed
}
object SubFeed {
  def filterPartitionValues(partitionValues: Seq[PartitionValues], partitions: Seq[String]): Seq[PartitionValues] = {
    partitionValues.map( pvs => PartitionValues(pvs.elements.filterKeys(partitions.contains))).filter(_.nonEmpty)
  }
}

/**
 * An interface to be implemented by SubFeed companion objects for subfeed conversion
 */
trait SubFeedConverter[S <: SubFeed] {
  def fromSubFeed(subFeed: SubFeed)(implicit context: ActionPipelineContext): S
  def get(subFeed: SubFeed): S = subFeed match {
    case specificSubFeed: S @unchecked => specificSubFeed
  }
}

/**
 * A SubFeed that holds a DataFrame
 */
trait DataFrameSubFeed extends SubFeed  {
  def tpe: Type // concrete type of this DataFrameSubFeed
  implicit lazy val helper: DataFrameSubFeedCompanion = DataFrameSubFeed.getHelper(tpe)
  def dataFrame: Option[GenericDataFrame]
  def persist: DataFrameSubFeed
  def unpersist: DataFrameSubFeed
  def schema: Option[GenericSchema] = dataFrame.map(_.schema)
  def hasReusableDataFrame: Boolean
  def isDummy: Boolean
  def filter: Option[String]
  def clearFilter(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): DataFrameSubFeed
  override def breakLineage(implicit context: ActionPipelineContext): DataFrameSubFeed
  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): DataFrameSubFeed
  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): DataFrameSubFeed
  override def clearDAGStart(): DataFrameSubFeed
  override def clearSkipped(): DataFrameSubFeed
  def isStreaming: Option[Boolean]
  def withDataFrame(dataFrame: Option[GenericDataFrame]): DataFrameSubFeed
  def withPartitionValues(partitionValues: Seq[PartitionValues]): DataFrameSubFeed
  def withFilter(partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed
  def applyFilter: DataFrameSubFeed = {
    // apply partition filter
    val partitionValuesColumn = partitionValues.flatMap(_.keys).distinct
    val dfPartitionFiltered = if (partitionValues.isEmpty) dataFrame
    else if (partitionValuesColumn.size == 1) {
      // filter with Sql "isin" expression if only one column
      val filterExpr = helper.col(partitionValuesColumn.head).isin(partitionValues.flatMap(_.elements.values):_*)
      dataFrame.map(_.filter(filterExpr))
    } else {
      // filter with and/or expression if multiple partition columns
      val filterExpr = PartitionValues.createFilterExpr(partitionValues)
      dataFrame.map(_.filter(filterExpr))
    }
    // apply generic filter
    val dfResult = if (filter.isDefined) dfPartitionFiltered.map(_.filter(helper.expr(filter.get)))
    else dfPartitionFiltered
    // return updated SubFeed
    withDataFrame(dfResult)
  }
  def asDummy(): DataFrameSubFeed
  def transform(transformer: GenericDataFrame => GenericDataFrame): DataFrameSubFeed = withDataFrame(dataFrame.map(transformer))
  def movePartitionColumnsLast(partitions: Seq[String]): DataFrameSubFeed
}
trait DataFrameSubFeedCompanion extends SubFeedConverter[DataFrameSubFeed] {
  protected def subFeedType: universe.Type
  /**
   * This method can create the schema for reading DataObjects.
   * If SubFeed subtypes have DataObjects with other methods to create a schema, they can override this method.
   */
  def getDataObjectReadSchema(dataObject: DataObject with CanCreateDataFrame)(implicit context: ActionPipelineContext): Option[GenericSchema] = {
    dataObject match {
      case input: UserDefinedSchema if input.schema.isDefined =>
        input.schema.map(dataObject.createReadSchema)
      case input: SchemaValidation if input.schemaMin.isDefined =>
        input.schemaMin.map(dataObject.createReadSchema)
      case _ => None
    }
  }

  /**
   * Get an empty DataFrame with a defined schema.
   * @param dataObjectId Snowpark implementation needs to get the Snowpark-Session from the DataObject. This should not be used otherwise.
   */
  def getEmptyDataFrame(schema: GenericSchema, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame
  def getEmptyStreamingDataFrame(schema: GenericSchema)(implicit context: ActionPipelineContext): GenericDataFrame = throw new NotImplementedError(s"getEmptyStreamingDataFrame is not implemented for ${subFeedType.typeSymbol.name}")
  def getSubFeed(dataFrame: GenericDataFrame, dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrameSubFeed
  def createSchema(fields: Seq[GenericField]): GenericSchema
  def col(colName: String): GenericColumn
  def lit(value: Any): GenericColumn
  def max(column: GenericColumn): GenericColumn
  def explode(column: GenericColumn): GenericColumn
  /**
   * Construct array from given columns and removing null values (Snowpark API)
   */
  def array_construct_compact(columns: GenericColumn*): GenericColumn
  def array(columns: GenericColumn*): GenericColumn
  def struct(columns: GenericColumn*): GenericColumn
  def expr(sqlExpr: String): GenericColumn
  def not(column: GenericColumn): GenericColumn
  def count(column: GenericColumn): GenericColumn
  def when(condition: GenericColumn, value: GenericColumn): GenericColumn
  def stringType: GenericDataType
  def arrayType(dataType: GenericDataType): GenericDataType
  def structType(colTypes: Map[String,GenericDataType]): GenericDataType
  /**
   * Get a DataFrame with the result of the given sql statement.
   * @param dataObjectId Snowpark implementation needs to get the Snowpark-Session from the DataObject. This should not be used otherwise.
   */
  def sql(query: String, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame
}
object DataFrameSubFeed {
  def getHelper(tpe: Type): DataFrameSubFeedCompanion = ScalaUtil.companionOf[DataFrameSubFeedCompanion](tpe)
  def getHelper(fullTpeName: String): DataFrameSubFeedCompanion = ScalaUtil.companionOf[DataFrameSubFeedCompanion](fullTpeName)
}

/**
 * A FileSubFeed is used to transport references to files between Actions.
 *
 * @param fileRefs path to files to be processed
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isDAGStart true if this subfeed is a start node of the dag
 * @param isSkipped true if this subfeed is the result of a skipped action
 * @param fileRefMapping store mapping of input to output file references. This is also used for post processing (e.g. delete after read).
 */
case class FileSubFeed(fileRefs: Option[Seq[FileRef]],
                       override val dataObjectId: DataObjectId,
                       override val partitionValues: Seq[PartitionValues],
                       override val isDAGStart: Boolean = false,
                       override val isSkipped: Boolean = false,
                       fileRefMapping: Option[Seq[FileRefMapping]] = None
                      )
  extends SubFeed {

  override def breakLineage(implicit context: ActionPipelineContext): FileSubFeed = {
    this.copy(fileRefs = None, fileRefMapping = None)
  }

  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): FileSubFeed = {
    if (breakLineageOnChange && partitionValues.nonEmpty) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearPartitionValues")
      this.copy(partitionValues = Seq()).breakLineage
    } else this.copy(partitionValues = Seq())
  }

  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): FileSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    if (breakLineageOnChange && partitionValues != updatedPartitionValues) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from updatePartitionValues")
      this.copy(partitionValues = updatedPartitionValues).breakLineage
    } else this.copy(partitionValues = updatedPartitionValues)
  }

  def checkPartitionValuesColsExisting(partitions: Set[String]): Boolean = {
    partitionValues.forall(pvs => partitions.diff(pvs.keys).isEmpty)
  }

  override def clearDAGStart(): FileSubFeed = {
    this.copy(isDAGStart = false)
  }

  override def clearSkipped(): FileSubFeed = {
    this.copy(isSkipped = false)
  }

  override def toOutput(dataObjectId: DataObjectId): FileSubFeed = {
    this.copy(fileRefs = None, fileRefMapping = None, isDAGStart = false, isSkipped = false, dataObjectId = dataObjectId)
  }

  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case fileSubFeed: FileSubFeed if this.fileRefs.isDefined && fileSubFeed.fileRefs.isDefined =>
      this.copy(fileRefs = this.fileRefs.map(_ ++ fileSubFeed.fileRefs.get)
        , partitionValues = unionPartitionValues(fileSubFeed.partitionValues)
        , isDAGStart = this.isDAGStart || fileSubFeed.isDAGStart)
    case fileSubFeed: FileSubFeed =>
      this.copy(fileRefs = None, partitionValues = unionPartitionValues(fileSubFeed.partitionValues)
        , isDAGStart = this.isDAGStart || fileSubFeed.isDAGStart)
    case x => this.copy(fileRefs = None, partitionValues = unionPartitionValues(x.partitionValues), isDAGStart = this.isDAGStart || x.isDAGStart)
  }

  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): FileSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false, fileRefs = result.fileRefs, fileRefMapping = None)
  }
  override def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): FileSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false, fileRefs = None, fileRefMapping = None)
  }
}
object FileSubFeed extends SubFeedConverter[FileSubFeed] {
  /**
   * This method is used to pass an output SubFeed as input FileSubFeed to the next Action. SubFeed type might need conversion.
   */
  override def fromSubFeed( subFeed: SubFeed )(implicit context: ActionPipelineContext): FileSubFeed = {
    subFeed match {
      case fileSubFeed: FileSubFeed => fileSubFeed
      case _ => FileSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
}

/**
 * Src/Tgt tuple representing the mapping of a file reference
 */
case class FileRefMapping(src: FileRef, tgt: FileRef)

/**
 * A ScriptSubFeed is used to notify DataObjects and subsequent actions about the completion of a script.
 * It allows to pass on arbitrary informations as key/values.
 *
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isDAGStart true if this subfeed is a start node of the dag
 * @param isSkipped true if this subfeed is the result of a skipped action
 * @param parameters arbitrary informations as key/value to pass on
 */
case class ScriptSubFeed(parameters: Option[Map[String,String]] = None,
                         override val dataObjectId: DataObjectId,
                         override val partitionValues: Seq[PartitionValues],
                         override val isDAGStart: Boolean = false,
                         override val isSkipped: Boolean = false
                        )
  extends SubFeed {
  override def breakLineage(implicit context: ActionPipelineContext): ScriptSubFeed = this
  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    this.copy(partitionValues = Seq())
  }
  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    this.copy(partitionValues = updatedPartitionValues)
  }
  override def clearDAGStart(): ScriptSubFeed = {
    this.copy(isDAGStart = false)
  }
  override def clearSkipped(): ScriptSubFeed = {
    this.copy(isSkipped = false)
  }
  override def toOutput(dataObjectId: DataObjectId): ScriptSubFeed = {
    this.copy(dataObjectId = dataObjectId, parameters = None, isDAGStart = false, isSkipped = false)
  }
  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case subFeed: ScriptSubFeed =>
      this.copy(
        parameters = optionalizeMap(this.parameters.getOrElse(Map()) ++ subFeed.parameters.getOrElse(Map())),
        partitionValues = unionPartitionValues(subFeed.partitionValues), isDAGStart = this.isDAGStart || subFeed.isDAGStart
      )
    case x => this.copy(parameters = None, partitionValues = unionPartitionValues(x.partitionValues), isDAGStart = this.isDAGStart || x.isDAGStart)
  }
  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false)
  }
  override def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): ScriptSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false, parameters = None)
  }
}
object ScriptSubFeed extends SubFeedConverter[ScriptSubFeed] {
  /**
   * This method is used to pass an output SubFeed as input FileSubFeed to the next Action. SubFeed type might need conversion.
   */
  override def fromSubFeed( subFeed: SubFeed )(implicit context: ActionPipelineContext): ScriptSubFeed = {
    subFeed match {
      case subFeed: ScriptSubFeed => subFeed
      case _ => ScriptSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
}

/**
 * An InitSubFeed is used to initialize first Nodes of a [[DAG]].
 *
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isSkipped true if this subfeed is the result of a skipped action
 */
case class InitSubFeed(override val dataObjectId: DataObjectId,
                       override val partitionValues: Seq[PartitionValues],
                       override val isSkipped: Boolean = false
                      )
  extends SubFeed {
  override def isDAGStart: Boolean = true
  override def breakLineage(implicit context: ActionPipelineContext): InitSubFeed = this
  override def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): InitSubFeed = {
    if (breakLineageOnChange && partitionValues.nonEmpty) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from clearPartitionValues")
      this.copy(partitionValues = Seq()).breakLineage
    } else this.copy(partitionValues = Seq())
  }
  override def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): InitSubFeed = {
    val updatedPartitionValues = SubFeed.filterPartitionValues(newPartitionValues.getOrElse(partitionValues), partitions)
    if (breakLineageOnChange && partitionValues != updatedPartitionValues) {
      logger.info(s"($dataObjectId) breakLineage called for SubFeed from updatePartitionValues")
      this.copy(partitionValues = updatedPartitionValues).breakLineage
    } else this.copy(partitionValues = updatedPartitionValues)
  }
  override def clearDAGStart(): InitSubFeed = throw new NotImplementedException() // calling clearDAGStart makes no sense on InitSubFeed
  override def clearSkipped(): InitSubFeed = throw new NotImplementedException() // calling clearSkipped makes no sense on InitSubFeed
  override def toOutput(dataObjectId: DataObjectId): FileSubFeed = throw new NotImplementedException()
  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = other match {
    case x => this.copy(partitionValues = unionPartitionValues(x.partitionValues))
  }
  def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): SubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false)
  }
  def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): SubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, isSkipped = false)
  }
}