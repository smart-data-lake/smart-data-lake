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

package io.smartdatalake.workflow.dataframe.spark

import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.evolution.TypeEvolutionUtil
import io.smartdatalake.util.spark.{DummyStreamProvider, NullAwareMurmur3HashExpr, dataset}
import io.smartdatalake.workflow._
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.executionMode.ExecutionModeResult
import io.smartdatalake.workflow.connection.{HadoopFileConnection, SparkClassicConnection}
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed.getSparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Encoder, Encoders, Row, SparkSession, functions}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Type, typeOf}
import scala.reflect.runtime.universe.TypeTag

/**
 * A SparkSubFeed is used to transport [[DataFrame]]'s between Actions.
 *
 * @param dataFrame Spark [[DataFrame]] to be processed. DataFrame should not be saved to state (@transient).
 * @param dataObjectId id of the DataObject this SubFeed corresponds to
 * @param partitionValues Values of Partitions transported by this SubFeed
 * @param isDAGStart true if this subfeed is a start node of the dag
 * @param isSkipped true if this subfeed is the result of a skipped action
 * @param isDummy true if this subfeed only contains a dummy DataFrame. Dummy DataFrames can be used for validating the lineage in init phase, but not for the exec phase.
 * @param filter a spark sql filter expression. This is used by DataFrameIncrementalMode.
 */
case class SparkSubFeed(@transient override val dataFrame: Option[SparkDataFrame],
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
  @transient override val tpe: Type = typeOf[SparkSubFeed]
  override def toOutput(dataObjectId: DataObjectId): SparkSubFeed = {
    this.copy(dataFrame = None, filter=None, isDAGStart = false, isSkipped = false, isDummy = false, dataObjectId = dataObjectId, observation = None, metrics = None)
  }
  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed = {
    val (dataFrame, dummy) = other match {
      // both subfeeds have a DataFrame to reuse -> union DataFrames
      case sparkSubFeed: SparkSubFeed if this.hasReusableDataFrame && sparkSubFeed.hasReusableDataFrame =>
        (this.dataFrame.map(_.unionByName(sparkSubFeed.dataFrame.get)), false)
      // both subfeeds have DataFrames, but they are not reusable, e.g. they just transport the schema
      case sparkSubFeed: SparkSubFeed if this.dataFrame.isDefined || sparkSubFeed.dataFrame.isDefined =>
        (this.dataFrame.orElse(sparkSubFeed.dataFrame), true) // if only one subfeed is defined, we need to get a fresh DataFrame and convert this to a dummy
      // otherwise no dataframe
      case _ =>
        (None, false)
    }
    var resultSubfeed = this.copy( dataFrame = dataFrame
      , partitionValues = unionPartitionValues(other.partitionValues)
      , isDAGStart = this.isDAGStart || other.isDAGStart
      , isSkipped = this.isSkipped && other.isSkipped
    )
    if (dummy) resultSubfeed = resultSubfeed.convertToDummy(dataFrame.get.schema)
    // return
    resultSubfeed
  }
  override def persist: SparkSubFeed = {
    this.dataFrame.foreach(_.inner.persist()) // Spark's persist & cache can be called without referencing the resulting DataFrame
    this
  }
  override def unpersist: SparkSubFeed = {
    this.dataFrame.foreach(_.inner.unpersist()) // Spark's unpersist can be called without referencing the resulting DataFrame
    this
  }
  override def isStreaming: Option[Boolean] = dataFrame.map(_.inner.isStreaming)
  override def hasReusableDataFrame: Boolean = dataFrame.isDefined && !isDummy && !isStreaming.getOrElse(false)
  private[smartdatalake] def convertToDummy(schema: SparkSchema)(implicit context: ActionPipelineContext): SparkSubFeed = {
    val dummyDf = dataFrame.map{
      dataFrame =>
        if (dataFrame.inner.isStreaming) SparkDataFrame(DummyStreamProvider.getDummyDf(schema.inner)(getSparkSession))
        else schema.getEmptyDataFrame(dataObjectId)
    }
    this.copy(dataFrame = dummyDf, isDummy = true)
  }
  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): SparkSubFeed = {
    // apply input filter
    val inputFilter = if (this.dataObjectId == mainInputId) result.filter else None
    this.copy(partitionValues = result.inputPartitionValues, filter = inputFilter, isSkipped = false).breakLineage // breaklineage keeps DataFrame schema without content
      .asInstanceOf[SparkSubFeed]
  }
  override def applyExecutionModeResultForOutput(result: ExecutionModeResult, partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues, PartitionValues])(implicit context: ActionPipelineContext): SparkSubFeed = {
    this.copy(partitionValues = result.getOutputPartitionValues(partitionValuesTransform), filter = result.filter, isSkipped = false, dataFrame = None)
  }
  override def withDataFrame(dataFrame: Option[GenericDataFrame]): SparkSubFeed = this.copy(dataFrame = dataFrame.map(_.asInstanceOf[SparkDataFrame]))
}

object SparkSubFeed extends DataFrameSubFeedCompanion {
  /**
   * This method is used to pass an output SubFeed as input SparkSubFeed to the next Action. SubFeed type might need conversion.
   */
  override def fromSubFeed( subFeed: SubFeed )(implicit context: ActionPipelineContext): SparkSubFeed = {
    subFeed match {
      case sparkSubFeed: SparkSubFeed => sparkSubFeed.clearFilter().asInstanceOf[SparkSubFeed] // make sure there is no filter, as filter can not be passed between actions.
      case _ => SparkSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
    }
  }
  @transient override def subFeedType: universe.Type = typeOf[SparkSubFeed]
  override def col(colName: String): GenericColumn = {
    SparkColumn(functions.col(colName))
  }
  override def lit(value: Any): GenericColumn = {
    SparkColumn(functions.lit(value))
  }
  override def min(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.min(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def max(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.max(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def first(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.first(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def abs(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.abs(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def count(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.count(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def countDistinct(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.countDistinct(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def approxCountDistinct(column: GenericColumn, rsd: Option[Double] = None): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn =>
        if (rsd.isDefined) SparkColumn(functions.approx_count_distinct(sparkColumn.inner, rsd.get))
        else SparkColumn(functions.approx_count_distinct(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def size(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.size(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def least(columns: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    SparkColumn(functions.least(columns.map(_.asInstanceOf[SparkColumn].inner):_*))
  }
  override def greatest(columns: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    SparkColumn(functions.greatest(columns.map(_.asInstanceOf[SparkColumn].inner):_*))
  }
  override def explode(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.explode(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def getEmptyDataFrame(schema: GenericSchema, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): SparkDataFrame = {
    val sparkSchema = SchemaConverter.convert(schema, subFeedType).asInstanceOf[SparkSchema]
    SparkDataFrame(dataset.getEmptyDataFrame(sparkSchema.inner)(getSparkSession))
  }
  override def getEmptyStreamingDataFrame(schema: GenericSchema)(implicit context: ActionPipelineContext): SparkDataFrame = {
    schema match {
      case sparkSchema: SparkSchema => SparkDataFrame(DummyStreamProvider.getDummyDf(sparkSchema.inner)(getSparkSession))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(schema)
    }
  }
  override def getSubFeed(df: GenericDataFrame, dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    df match {
      case sparkDf: SparkDataFrame => SparkSubFeed(Some(sparkDf), dataObjectId, partitionValues)
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(df)
    }
  }
  override def stringType: GenericDataType = SparkDataType(StringType)

  override def arrayType(dataType: GenericDataType): SparkArrayDataType = {
    dataType match {
      case sparkDataType: SparkDataType => SparkArrayDataType(ArrayType(sparkDataType.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
    }
  }

  override def structType(fields: Map[String, GenericDataType]): SparkStructDataType = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, fields.values.toSeq)
    val sparkFields = fields.map{ case (name,dataType) => StructField(name, dataType.asInstanceOf[SparkDataType].inner)}.toSeq
    SparkStructDataType(StructType(sparkFields))
  }

  override def structType(fields: Seq[GenericField]): SparkStructDataType = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, fields)
    SparkStructDataType(StructType(fields.map(_.asInstanceOf[SparkField].inner)))
  }

  override def mapType(keyType: GenericDataType, valueType: GenericDataType): SparkMapDataType = {
    (keyType, valueType) match {
      case (sparkKeyType: SparkDataType, sparkValueType: SparkDataType) => SparkMapDataType(MapType(sparkKeyType.inner, sparkValueType.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(keyType)
    }
  }

  override def field(name: String, dataType: GenericDataType, nullable: Boolean): GenericField = {
    dataType match {
      case sparkDataType: SparkDataType => SparkField(StructField(name, sparkDataType.inner, nullable))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataType)
    }
  }
  override def array_construct_compact(columns: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    SparkColumn(functions.array_compact(functions.array(columns.map(_.asInstanceOf[SparkColumn].inner):_*)))
  }
  override def array(columns: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    SparkColumn(functions.array(columns.map(_.asInstanceOf[SparkColumn].inner):_*))
  }
  override def struct(columns: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    SparkColumn(functions.struct(columns.map(_.asInstanceOf[SparkColumn].inner):_*))
  }
  override def expr(sqlExpr: String): GenericColumn = SparkColumn(functions.expr(sqlExpr))

  override def when(condition: GenericColumn, value: GenericColumn): GenericColumn with GenericWhen = {
    (condition, value) match {
      case (sparkCondition: SparkColumn, sparkValue: SparkColumn) => new SparkColumn(functions.when(sparkCondition.inner, sparkValue.inner)) with SparkWhen
      case _ => throw new IllegalStateException(s"Unsupported subFeedType ${condition.subFeedType.typeSymbol.name}, ${value.subFeedType.typeSymbol.name} in method when")
    }
  }
  override def not(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.not(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def concat(exprs: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, exprs)
    SparkColumn(functions.concat(exprs.map(_.asInstanceOf[SparkColumn].inner):_*))
  }
  override def regexp_extract(column: GenericColumn, regexp: String, groupIdx: Int): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.regexp_extract(sparkColumn.inner, regexp, groupIdx))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }
  override def raise_error(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.raise_error(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }

  override def from_json(column: GenericColumn, dataType: GenericDataType): GenericColumn = {
    (column, dataType) match {
      case (sparkColumn: SparkColumn, sparkDataType: SparkDataType) => SparkColumn(functions.from_json(sparkColumn.inner, sparkDataType.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }

  /**
   * Spark Hash Functions ignores null values, e.g. adding a null column to a row does not change the hash value of the row.
   * This method will treat null values as regular values, which influence hash value of the row.
   */
  def nullAwareHash(cols: Column*): Column = {
    val expr = new NullAwareMurmur3HashExpr(cols.map(_.expr))
    new Column(expr)
  }

  override def hash(column: GenericColumn): GenericColumn = {
    column match {
      case sparkColumn: SparkColumn => SparkColumn(nullAwareHash(sparkColumn.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }

  override def sql(query: String, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): GenericDataFrame = {
    SparkDataFrame(getSparkSession.sql(query))
  }
  override def createSchema(fields: Seq[GenericField]): GenericSchema = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, fields)
    SparkSchema(StructType(fields.map(_.asInstanceOf[SparkField].inner)))
  }

  def apply( dataFrame: SparkDataFrame, dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues]): SparkSubFeed = {
    SparkSubFeed(Some(dataFrame), dataObjectId: DataObjectId, partitionValues)
  }

  override def coalesce(columns: GenericColumn*): GenericColumn = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns.toSeq)
    SparkColumn(functions.coalesce(columns.map(_.asInstanceOf[SparkColumn].inner):_*))
  }

  override def row_number: GenericColumn = SparkColumn(functions.row_number())

  override def window(aggFunction: () => GenericColumn, partitionBy: Seq[GenericColumn], orderBy: GenericColumn): GenericColumn = {

    partitionBy.foreach(c => assert(c.isInstanceOf[SparkColumn], DataFrameSubFeed.throwIllegalSubFeedTypeException(c)))

    assert(orderBy.isInstanceOf[SparkColumn], DataFrameSubFeed.throwIllegalSubFeedTypeException(orderBy))

    aggFunction.apply() match {
      case sparkAggFunctionColumn: SparkColumn => SparkColumn(sparkAggFunctionColumn
        .inner.over(
          Window.partitionBy(partitionBy.map(_.asInstanceOf[SparkColumn].inner): _*)
            .orderBy(orderBy.asInstanceOf[SparkColumn].inner))
      )
      case generic => DataFrameSubFeed.throwIllegalSubFeedTypeException(generic)
    }
  }

  override def transform(column: GenericColumn, func: GenericColumn => GenericColumn): GenericColumn = {
    val sparkFunc = (column: Column) => func(SparkColumn(column)).asInstanceOf[SparkColumn].inner
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.transform(sparkColumn.inner, sparkFunc))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }

  override def transform_keys(column: GenericColumn, func: (GenericColumn, GenericColumn) => GenericColumn): GenericColumn = {
    val sparkFunc = (keyColumn: Column, valueColumn: Column) => func(SparkColumn(keyColumn), SparkColumn(valueColumn)).asInstanceOf[SparkColumn].inner
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.transform_keys(sparkColumn.inner, sparkFunc))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }

  override def transform_values(column: GenericColumn, func: (GenericColumn, GenericColumn) => GenericColumn): GenericColumn = {
    val sparkFunc = (keyColumn: Column, valueColumn: Column) => func(SparkColumn(keyColumn), SparkColumn(valueColumn)).asInstanceOf[SparkColumn].inner
    column match {
      case sparkColumn: SparkColumn => SparkColumn(functions.transform_values(sparkColumn.inner, sparkFunc))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(column)
    }
  }

  override def rowFromSeq(values: Seq[Any]): GenericRow = {
    SparkRow(Row.fromSeq(values))
  }

  override def schemaEvolutionUdf(srcType: GenericDataType, tgtType: GenericDataType): GenericUnaryUdf = (srcType, tgtType) match {
    case (srcType, tgtType) if srcType.isSameType(tgtType) => SparkUnaryUdf(x => x)
    case (srcType: SparkSimpleDataType, tgtType: SparkSimpleDataType) => SparkUnaryUdf(x => x.cast(tgtType.inner))
    case (srcType: SparkStructDataType, tgtType: SparkStructDataType) => SparkUnaryUdf(TypeEvolutionUtil.schemaEvolutionUdf(srcType.inner, tgtType.inner))
    case (srcType: SparkArrayDataType, tgtType: SparkArrayDataType) => new GenericUnaryUdf {
      override def subFeedType: universe.Type = SparkSubFeed.subFeedType

      override def convert(col: GenericColumn): GenericColumn = {
        transform(col, schemaEvolutionUdf(srcType.elementDataType, tgtType.elementDataType).convert _)
      }
    }
    case (srcType: SparkMapDataType, tgtType: SparkMapDataType) => new GenericUnaryUdf {
      override def subFeedType: universe.Type = SparkSubFeed.subFeedType

      override def convert(col: GenericColumn): GenericColumn = {
        transform_values(
          transform_keys(col, (k, _) => schemaEvolutionUdf(srcType.keyDataType, tgtType.keyDataType).convert(k)),
          (_, v) => schemaEvolutionUdf(srcType.valueDataType, tgtType.valueDataType).convert(v)
        )
      }
    }
  }

  override def createField(name: String, dataType: GenericDataType, nullable: Boolean, comment: Option[String]): GenericField = {
    var field = StructField(name, dataType.asInstanceOf[SparkDataType].inner, nullable)
    comment.foreach(c => field = field.withComment(c))
    SparkField(field)
  }

  override def createSimpleDataType(tpe: String): GenericDataType with GenericSimpleDataType = {
    SparkSimpleDataType(DataType.fromJson(s""""$tpe""""))
  }

  override def createStructDataType(fields: Seq[GenericField]): GenericDataType with GenericStructDataType = {
    SparkStructDataType(StructType(fields.map(_.asInstanceOf[SparkField].inner)))
  }

  override def createArrayDataType(valueTpe: GenericDataType): GenericDataType with GenericArrayDataType = {
    SparkArrayDataType(ArrayType(valueTpe.asInstanceOf[SparkDataType].inner))
  }

  override def createMapDataType(keyTpe: GenericDataType, valueTpe: GenericDataType): GenericDataType with GenericMapDataType = {
    SparkMapDataType(MapType(keyTpe.asInstanceOf[SparkDataType].inner, valueTpe.asInstanceOf[SparkDataType].inner))
  }

  override def createDataFrame[A <: Product: ClassTag: TypeTag](rows: Seq[A])(implicit context: ActionPipelineContext): GenericDataFrame = {
    implicit val encoder: Encoder[A] = Encoders.product[A]
    val session = getSparkSession
    import session.implicits._
    SparkDataFrame(rows.toDF)
  }

  override def createDataFrame[A <: Product: ClassTag: TypeTag](rows: Seq[A], colNames: Seq[String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    implicit val encoder: Encoder[A] = Encoders.product[A]
    val session = getSparkSession
    import session.implicits._
    SparkDataFrame(rows.toDF(colNames:_*))
  }

  def getSparkSession(implicit context: ActionPipelineContext): SparkSession = {
    context.engineConnection match {
      case Some(connection) if connection.isInstanceOf[SparkClassicConnection] => connection.asInstanceOf[SparkClassicConnection].sparkSession
      case Some(connection) => throw new IllegalStateException(s"Spark connection is required to create DataFrame, but got ${connection.id} of type ${connection.getClass.getSimpleName} in context")
      case None => throw new IllegalStateException("No connection available in context. Spark connection is required to create DataFrame.")
    }
  }
}

trait SparkWhen extends GenericWhen {
  col: SparkColumn =>
  override def when(condition: GenericColumn, value: GenericColumn): GenericColumn with GenericWhen = {
    (condition, value) match {
      case (condition: SparkColumn, value: SparkColumn) => new SparkColumn(col.inner.when(condition.inner, value.inner)) with SparkWhen
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(condition)
    }
  }

  override def otherwise(value: GenericColumn): GenericColumn = {
    value match {
      case value: SparkColumn => new SparkColumn(col.inner.otherwise(value.inner))
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(value)
    }
  }
}