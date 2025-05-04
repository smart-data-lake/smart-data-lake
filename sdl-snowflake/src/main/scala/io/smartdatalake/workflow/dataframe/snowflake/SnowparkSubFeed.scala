/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.dataframe.snowflake

import com.snowflake.snowpark._
import com.snowflake.snowpark.types._
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.executionMode.ExecutionModeResult
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataobject.SnowflakeTableDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion, SubFeed}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Type, typeOf}

case class SnowparkSubFeed(@transient override val dataFrame: Option[SnowparkDataFrame],
                           override val dataObjectId: DataObjectId,
                           override val partitionValues: Seq[PartitionValues],
                           override val isDAGStart: Boolean = false,
                           override val isSkipped: Boolean = false,
                           override val isDummy: Boolean = false,
                           override val filter: Option[String] = None,
                           @transient override val observation: Option[DataFrameObservation] = None,
                           override val metrics: Option[MetricsMap] = None
                          )
  extends DataFrameSubFeed {
  @transient
  override val tpe: Type = typeOf[SnowparkSubFeed]

  override def toOutput(dataObjectId: DataObjectId): SnowparkSubFeed = {
    this.copy(dataFrame = None, filter = None, isDAGStart = false, isSkipped = false, isDummy = false, dataObjectId = dataObjectId, observation = None, metrics = None)
  }

  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = {
    val (dataFrame, dummy) = other match {
      // both subfeeds have a DataFrame to reuse -> union DataFrames
      case sparkSubFeed: SnowparkSubFeed if this.hasReusableDataFrame && sparkSubFeed.hasReusableDataFrame =>
        (this.dataFrame.map(_.unionByName(sparkSubFeed.dataFrame.get)), false)
      // both subfeeds have DataFrames, but they are not reusable, e.g. they just transport the schema
      case sparkSubFeed: SnowparkSubFeed if this.dataFrame.isDefined || sparkSubFeed.dataFrame.isDefined =>
        (this.dataFrame.orElse(sparkSubFeed.dataFrame), true) // if only one subfeed is defined, we need to get a fresh DataFrame and convert this to a dummy
      // otherwise no dataframe
      case _ =>
        (None, false)
    }
    var resultSubfeed = this.copy(dataFrame = dataFrame
      , partitionValues = unionPartitionValues(other.partitionValues)
      , isDAGStart = this.isDAGStart || other.isDAGStart
      , isSkipped = this.isSkipped && other.isSkipped
    )
    if (dummy) resultSubfeed = resultSubfeed.convertToDummy(dataFrame.get.schema)
    // return
    resultSubfeed
  }

  override def persist: SnowparkSubFeed = {
    logger.warn("Persist is not implemented by Snowpark")
    // TODO: should we use "dataFrame.map(_.inner.cacheResult())"
    this
  }

  override def unpersist: DataFrameSubFeed = this // not implemented, see persist

  override def hasReusableDataFrame: Boolean = dataFrame.isDefined && !isDummy && !isStreaming.getOrElse(false)

  private[smartdatalake] def convertToDummy(schema: SnowparkSchema)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    val dummyDf = dataFrame.map(_ => schema.getEmptyDataFrame(dataObjectId))
    this.copy(dataFrame = dummyDf, isDummy = true)
  }
  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    // apply input filter
    val inputFilter = if (this.dataObjectId == mainInputId) result.filter else None
    this.copy(partitionValues = result.inputPartitionValues, filter = inputFilter, isSkipped = false).breakLineage // breaklineage keeps DataFrame schema without content
      .asInstanceOf[SnowparkSubFeed]
  }
  override def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    this.copy(partitionValues = result.inputPartitionValues, filter = result.filter, isSkipped = false, dataFrame = None)
  }
  override def withDataFrame(dataFrame: Option[GenericDataFrame]): SnowparkSubFeed = this.copy(dataFrame = dataFrame.map(_.asInstanceOf[SnowparkDataFrame]))
  override def isStreaming: Option[Boolean] = Some(false) // no spark streaming with Snowpark
}

object SnowparkSubFeed extends DataFrameSubFeedCompanion with SmartDataLakeLogger {
  override def fromSubFeed(subFeed: SubFeed)(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    subFeed match {
      case snowparkSubFeed: SnowparkSubFeed => snowparkSubFeed
      case dataFrameSubFeed: DataFrameSubFeed =>
        val convertedSchema = dataFrameSubFeed.schema.map(_.convert(subFeedType))
        SnowparkSubFeed(convertedSchema.map(s => getEmptyDataFrame(s, subFeed.dataObjectId)), subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped, isDummy = convertedSchema.isDefined)
      case _ => SnowparkSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
  override def subFeedType: universe.Type = typeOf[SnowparkSubFeed]
  override def col(colName: String): SnowparkColumn = {
    SnowparkColumn(functions.col(colName))
  }
  override def lit(value: Any): SnowparkColumn = {
    SnowparkColumn(functions.lit(value))
  }
  override def min(column: GenericColumn): SnowparkColumn = {
    column match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(functions.min(snowparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def max(column: GenericColumn): SnowparkColumn = {
    column match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(functions.max(snowparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def count(column: GenericColumn): SnowparkColumn = {
    column match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(functions.count(snowparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def countDistinct(columns: GenericColumn*): SnowparkColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns.toSeq)
    val innerColumns = columns.map(_.asInstanceOf[SnowparkColumn].inner)
    SnowparkColumn(functions.count_distinct(innerColumns.head, innerColumns.tail:_*))
  }
  override def approxCountDistinct(column: GenericColumn, rsd: Option[Double] = None): SnowparkColumn = {
    column match {
      case snowparkColumn: SnowparkColumn =>
        if (rsd.isDefined) logger.warn("Parameter 'rsd' is ignored for approxCountDistinct within SnowflakeSubFeed")
        SnowparkColumn(functions.approx_count_distinct(snowparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def size(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SnowparkColumn => SnowparkColumn(functions.array_size(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def explode(column: GenericColumn): SnowparkColumn = {
    // TODO: check if this can be done by a udf?
    throw new NotImplementedError("explode array is not implemented in Snowpark")
  }
  override def getEmptyDataFrame(schema: GenericSchema, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): SnowparkDataFrame = {
    val snowparkSchema = SchemaConverter.convert(schema, subFeedType).asInstanceOf[SnowparkSchema]
    val dataObject = context.instanceRegistry.get[SnowflakeTableDataObject](dataObjectId)
    SnowparkDataFrame(dataObject.snowparkSession.createDataFrame(Seq(), snowparkSchema.inner))
  }
  override def getSubFeed(df: GenericDataFrame, dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): SnowparkSubFeed = {
    df match {
      case snowparkDf: SnowparkDataFrame => SnowparkSubFeed(Some(snowparkDf), dataObjectId, partitionValues)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(df)
    }
  }
  override def stringType: SnowparkDataType = SnowparkDataType(StringType)

  override def arrayType(dataType: GenericDataType): SnowparkArrayDataType = {
    dataType match {
      case snowparkDataType: SnowparkDataType => SnowparkArrayDataType(ArrayType(snowparkDataType.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
    }
  }

  override def structType(fields: Map[String, GenericDataType]): SnowparkStructDataType = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, fields.values.toSeq)
    val snowparkFields = fields.map { case (name, dataType) => StructField(name, dataType.asInstanceOf[SnowparkDataType].inner) }.toSeq
    SnowparkStructDataType(StructType(snowparkFields))
  }

  override def structType(fields: Seq[GenericField]): SnowparkStructDataType = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, fields)
    SnowparkStructDataType(StructType(fields.map(_.asInstanceOf[SnowparkField].inner)))
  }

  override def mapType(keyType: GenericDataType, valueType: GenericDataType): SnowparkMapDataType = {
    (keyType, valueType) match {
      case (sparkKeyType: SnowparkDataType, sparkValueType: SnowparkDataType) => SnowparkMapDataType(MapType(sparkKeyType.inner, sparkValueType.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(keyType)
    }
  }

  override def field(name: String, dataType: GenericDataType, nullable: Boolean): GenericField = {
    dataType match {
      case sparkDataType: SnowparkDataType => SnowparkField(StructField(name, sparkDataType.inner, nullable))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
    }
  }
  /**
   * Construct array from given columns removing null values (Snowpark API)
   */
  override def array_construct_compact(columns: GenericColumn*): SnowparkColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns.toSeq)
    SnowparkColumn(functions.array_construct_compact(columns.map(_.asInstanceOf[SnowparkColumn].inner):_*))
  }
  override def array(columns: GenericColumn*): SnowparkColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns.toSeq)
    SnowparkColumn(functions.array_construct(columns.map(_.asInstanceOf[SnowparkColumn].inner):_*))
  }
  override def struct(columns: GenericColumn*): SnowparkColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns.toSeq)
    SnowparkColumn(functions.object_construct(columns.map(_.asInstanceOf[SnowparkColumn].inner):_*))
  }
  override def expr(sqlExpr: String): SnowparkColumn = SnowparkColumn(functions.sqlExpr(sqlExpr))

  override def when(condition: GenericColumn, value: GenericColumn): SnowparkColumn with SnowparkWhen = {
    (condition, value) match {
      case (snowparkCondition: SnowparkColumn, snowparkValue: SnowparkColumn) => new SnowparkColumn(functions.when(snowparkCondition.inner, snowparkValue.inner).asInstanceOf[Column]) with SnowparkWhen
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${condition.subFeedType.typeSymbol.name}, ${value.subFeedType.typeSymbol.name} in method when")
    }
  }
  override def not(column: GenericColumn): SnowparkColumn = {
    column match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(functions.not(snowparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def concat(exprs: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, exprs.toSeq)
    SnowparkColumn(functions.concat(exprs.map(_.asInstanceOf[SnowparkColumn].inner):_*))
  }
  override def regexp_extract(column: GenericColumn, pattern: String, groupIdx: Int): GenericColumn = {
    column match {
      // must be implemented as sql expression, as function regexp_substr doesn't yet exist in snowpark
      case snowparkColumn: SnowparkColumn => SnowparkColumn(functions.sqlExpr(s"regexp_substr(${snowparkColumn.inner.getName}, $pattern, 1, 1, 'c', $groupIdx)"))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def raise_error(column: GenericColumn): GenericColumn = {
    val udfRaiseError = functions.udf((msg: String) => throw new RuntimeException(msg)) // Spark raise_error functions also creates a RuntimeException
    column match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(udfRaiseError(snowparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }

  override def hash(column: GenericColumn): GenericColumn = {
    column match {
      case snowparkColumn: SnowparkColumn => SnowparkColumn(functions.hash(snowparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }

  override def sql(query: String, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): SnowparkDataFrame = {
    val dataObject = context.instanceRegistry.get[SnowflakeTableDataObject](dataObjectId)
    SnowparkDataFrame(dataObject.snowparkSession.sql(query))
  }
  override def createSchema(fields: Seq[GenericField]): GenericSchema = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, fields)
    SnowparkSchema(StructType(fields.map(_.asInstanceOf[SnowparkField].inner)))
  }

  override def coalesce(columns: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns.toSeq)
    SnowparkColumn(functions.coalesce(columns.map(_.asInstanceOf[SnowparkColumn].inner):_*))
  }

  override def row_number: GenericColumn = SnowparkColumn(functions.row_number())

  override def window(aggFunction: () => GenericColumn, partitionBy: Seq[GenericColumn], orderBy: GenericColumn): GenericColumn = {

    partitionBy.foreach(c => assert(c.isInstanceOf[SnowparkColumn], DataFrameSubFeed.throwIllegalSubFeedTypeException(c)))

    assert(orderBy.isInstanceOf[SnowparkColumn], DataFrameSubFeed.throwIllegalSubFeedTypeException(orderBy))

    aggFunction.apply() match {
      case snowparkAggFunctionColumn: SnowparkColumn => SnowparkColumn(snowparkAggFunctionColumn
        .inner.over(
          Window.partitionBy(partitionBy.map(_.asInstanceOf[SnowparkColumn].inner): _*)
            .orderBy(orderBy.asInstanceOf[SnowparkColumn].inner))
      )
      case generic => DataFrameSubFeed.throwIllegalSubFeedTypeException(generic)
    }

  }

  override def transform(column: GenericColumn, func: GenericColumn => GenericColumn): GenericColumn = {
    // TODO: check if this can be done by a udf?
    throw new NotImplementedError("transform array is not implemented in Snowpark")
  }
  override def transform_keys(column: GenericColumn, func: (GenericColumn,GenericColumn) => GenericColumn): GenericColumn = {
    // TODO: check if this can be done by a udf?
    throw new NotImplementedError("transform_keys array is not implemented in Snowpark")
  }
  override def transform_values(column: GenericColumn, func: (GenericColumn,GenericColumn) => GenericColumn): GenericColumn = {
    // TODO: check if this can be done by a udf?
    throw new NotImplementedError("transform_keys array is not implemented in Snowpark")
  }

  override def rowFromSeq(values: Seq[Any]): GenericRow = {
    SnowparkRow(Row(values: _*))
  }

  override def schemaEvolutionUdf(srcType: GenericDataType, tgtType: GenericDataType): GenericUnaryUdf = {
    throw new NotImplementedError("schema evolution Udf can not be implemented in Snowpark")
  }
}


trait SnowparkWhen extends GenericWhen {
  col: SnowparkColumn =>
  override def when(condition: GenericColumn, value: GenericColumn): GenericColumn with GenericWhen = {
    (condition, value) match {
      case (condition: SnowparkColumn, value: SnowparkColumn) => new SnowparkColumn(col.inner.asInstanceOf[CaseExpr].when(condition.inner, value.inner)) with SnowparkWhen
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(condition)
    }
  }

  override def otherwise(value: GenericColumn): GenericColumn = {
    value match {
      case value: SnowparkColumn => SnowparkColumn(col.inner.asInstanceOf[CaseExpr].otherwise(value.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(value)
    }
  }
}