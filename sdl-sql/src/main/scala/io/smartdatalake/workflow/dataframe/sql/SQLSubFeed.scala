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
package io.smartdatalake.workflow.dataframe.sql

import io.smartdatalake.definitions.Environment
import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.executionMode.ExecutionModeResult
import io.smartdatalake.workflow.connection.SQLEngineConnection
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.{ActionPipelineContext, ColumnFilter, DataFrameSubFeed, DataFrameSubFeedCompanion, SubFeed}

import java.time.Duration
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{MethodSymbol, Type, TypeTag, typeOf}

/**
 * A SubFeed of the SQL engine. Its DataFrame is an SQLGlot query, which is rendered as SQL statement to be executed
 * on the database. See [[SQLEngineConnection]].
 */
case class SQLSubFeed(@transient override val dataFrame: Option[SQLDataFrame],
                      override val dataObjectId: DataObjectId,
                      override val partitionValues: Seq[PartitionValues] = Seq(),
                      override val isDAGStart: Boolean = false,
                      override val isSkipped: Boolean = false,
                      override val filters: Seq[ColumnFilter] = Seq(),
                      @transient override val observation: Option[DataFrameObservation] = None,
                      override val metrics: Option[MetricsMap] = None,
                      override val expectationsResult: Option[Map[String, String]] = None,
                      @transient override val keptSchema: Option[GenericSchema] = None,
                      override val executionModeResultOptions: Map[String, String] = Map()
                     ) extends DataFrameSubFeed {
  @transient override def tpe: Type = typeOf[SQLSubFeed]

  override def withDataFrame(dataFrame: Option[GenericDataFrame]): SQLSubFeed = this.copy(
    dataFrame = dataFrame.map {
      case df: SQLDataFrame => df
      case df => DataFrameSubFeed.throwIllegalSubFeedTypeException(df)
    },
    keptSchema = if (dataFrame.isDefined) None else this.schemaOpt
  )

  override def withSchema(schema: Option[GenericSchema]): SQLSubFeed = this.copy(dataFrame = None, keptSchema = schema)

  override def toOutput(dataObjectId: SdlConfigObject.DataObjectId): SQLSubFeed = this.copy(dataFrame = None, filters = Seq(), isDAGStart = false, isSkipped = false, dataObjectId = dataObjectId, observation = None, metrics = None, expectationsResult = None, keptSchema = None, executionModeResultOptions = Map())

  override def union(other: SubFeed)(implicit context: ActionPipelineContext): SQLSubFeed = {
    val (dataFrame, schema) = other match {
      // both subfeeds have a DataFrame to reuse -> union DataFrames
      case sqlSubFeed: SQLSubFeed if this.dataFrame.isDefined && sqlSubFeed.dataFrame.isDefined =>
        (this.dataFrame.map(_.unionByName(sqlSubFeed.dataFrame.get)), None)
      // at least one subfeed can not be reused -> transport only the schema, the DataFrame is read again from the DataObject
      case sqlSubFeed: SQLSubFeed =>
        (None, this.schemaOpt.orElse(sqlSubFeed.schemaOpt))
      case _ =>
        (None, this.schemaOpt)
    }
    this.copy(dataFrame = dataFrame
      , keptSchema = if (dataFrame.isDefined) None else schema
      , partitionValues = unionPartitionValues(other.partitionValues)
      , isDAGStart = this.isDAGStart || other.isDAGStart
      , isSkipped = this.isSkipped && other.isSkipped
      , filters = unionFilters(other)
      , executionModeResultOptions = unionExecutionModeResultOptions(other)
    )
  }

  override def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: SdlConfigObject.DataObjectId)(implicit context: ActionPipelineContext): SQLSubFeed = {
    // apply input filters
    val inputFilters = result.filtersForInput(this.dataObjectId == mainInputId)
    // the execution mode changed partition values and filters, so an existing DataFrame no longer matches them and is
    // dropped by breakLineage. Its schema is kept, see SubFeed.breakLineage.
    this.copy(partitionValues = result.inputPartitionValues, filters = inputFilters, isSkipped = false, executionModeResultOptions = result.options).breakLineage
      .asInstanceOf[SQLSubFeed]
  }

  override def applyExecutionModeResultForOutput(result: ExecutionModeResult, partitionValuesTransform: Seq[PartitionValues] => Map[PartitionValues, PartitionValues])(implicit context: ActionPipelineContext): SQLSubFeed = {
    // filters of the output are set from the main input SubFeed after the transformation, see DataFrameActionImpl.updateOutputFilters
    this.copy(partitionValues = result.getOutputPartitionValues(partitionValuesTransform), filters = Seq(), isSkipped = false, dataFrame = None, keptSchema = None, executionModeResultOptions = result.options)
  }
}

/**
 * Companion of [[SQLSubFeed]], implementing the DataFrame functions of the SQL engine.
 *
 * Column expressions are SQL text in the default SQLGlot dialect, see [[SQLColumn]]. This also applies to
 * expressions given as string, e.g. `expr("a > 1")`. SQL queries of SQL transformers are parsed in the SQL dialect
 * of the [[SQLEngineConnection]] instead.
 */
object SQLSubFeed extends DataFrameSubFeedCompanion {

  @transient override protected def subFeedType: Type = typeOf[SQLSubFeed]

  /**
   * The SQLEngineConnection of the current Action, or otherwise the default engine connection if it is a SQLEngineConnection.
   */
  def getEngineConnection(implicit context: ActionPipelineContext): Option[SQLEngineConnection] = {
    context.engineConnection.collect { case c: SQLEngineConnection => c }
      .orElse(context.instanceRegistry.getConnections.collectFirst {
        case c: SQLEngineConnection if c.id.id == Environment.defaultEngineConnectionId => c
      })
  }

  /**
   * The SQLEngineConnection to use for creating DataFrames, see [[getEngineConnection]].
   */
  def requireEngineConnection(implicit context: ActionPipelineContext): SQLEngineConnection = getEngineConnection
    .getOrElse(throw new IllegalStateException(s"No SQLEngineConnection found, neither for the current action nor with id ${Environment.defaultEngineConnectionId}"))

  /**
   * Create a SQLDataFrame reading a database table. See [[SQLDataFrame.table]].
   */
  def table(tableName: String, schema: SQLSchema)(implicit context: ActionPipelineContext): SQLDataFrame =
    SQLDataFrame.table(requireEngineConnection, tableName, schema)

  private def create(op: String, args: (String, Any)*)(implicit context: ActionPipelineContext): SQLDataFrame = {
    val connection = requireEngineConnection
    val bridge = connection.bridge
    SQLDataFrame(bridge.callDataFrame(op, args: _*), bridge, connection)
  }

  // Members declared in SubFeedConverter and DataFrameSubFeedCompanion

  override def fromSubFeed(subFeed: SubFeed)(implicit context: ActionPipelineContext): SQLSubFeed = subFeed match {
    case sqlSubFeed: SQLSubFeed => sqlSubFeed.copy(executionModeResultOptions = Map()) // no executionModeResultOptions are passed between actions. Filters are kept, only propagating filters can be present here.
    case _ => SQLSubFeed(None, subFeed.dataObjectId, subFeed.partitionValues, subFeed.isDAGStart, subFeed.isSkipped)
  }

  override def getEmptyDataFrame(schema: GenericSchema, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): SQLDataFrame = schema match {
    case sqlSchema: SQLSchema => create("empty", "columns" -> sqlSchema.toBridge)
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(schema)
  }

  override def getSubFeed(dataFrame: GenericDataFrame, dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): SQLSubFeed = dataFrame match {
    case sqlDf: SQLDataFrame => SQLSubFeed(Some(sqlDf), dataObjectId, partitionValues)
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(dataFrame)
  }

  override def getSchemaSubFeed(dataObjectId: DataObjectId, schema: GenericSchema, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): SQLSubFeed =
    SQLSubFeed(None, dataObjectId, partitionValues, keptSchema = Some(schema))

  override def createSchema(fields: Seq[GenericField]): SQLSchema = SQLSchema(fields.map(SQLField.of))

  override def createField(name: String, dataType: GenericDataType, nullable: Boolean, comment: Option[String]): SQLField =
    SQLField(name, SQLDataType.of(dataType), nullable, comment)

  override def createSimpleDataType(tpe: String): SQLSimpleDataType = SQLDataType.simple(tpe)

  override def createStructDataType(fields: Seq[GenericField]): SQLStructDataType = SQLStructDataType(fields.map(SQLField.of))

  override def createArrayDataType(valueTpe: GenericDataType): SQLArrayDataType = SQLArrayDataType(SQLDataType.of(valueTpe))

  override def createMapDataType(keyTpe: GenericDataType, valueTpe: GenericDataType): SQLMapDataType =
    SQLMapDataType(SQLDataType.of(keyTpe), SQLDataType.of(valueTpe))

  /**
   * Create a DataFrame from literal values, rendered as VALUES clause.
   */
  override def createDataFrame[A <: Product : ClassTag : TypeTag](rows: Seq[A])(implicit context: ActionPipelineContext): SQLDataFrame =
    createDataFrame(rows, productFields[A].map(_._1))

  override def createDataFrame[A <: Product : ClassTag : TypeTag](rows: Seq[A], colNames: Seq[String])(implicit context: ActionPipelineContext): SQLDataFrame = {
    val types = productFields[A].map(f => sqlTypeOf(f._2))
    require(types.size == colNames.size, s"Number of column names ${colNames.size} does not match number of fields ${types.size}")
    val values = rows.map(_.productIterator.map(v => SQLColumn.literal(v).expr).toSeq)
    create("values", "rows" -> values, "columns" -> colNames.zip(types).map { case (n, t) => Seq(n, t.sql) })
  }

  private def productFields[A: TypeTag]: Seq[(String, Type)] = {
    val tpe = typeOf[A]
    tpe.members.sorted.collect { case m: MethodSymbol if m.isCaseAccessor => (m.name.toString, m.typeSignatureIn(tpe).finalResultType) }
  }

  private def sqlTypeOf(tpe: Type): SQLDataType = tpe match {
    case t if t <:< typeOf[Option[_]] => sqlTypeOf(t.typeArgs.head)
    case t if t =:= typeOf[Int] || t =:= typeOf[Integer] => SQLSimpleDataType("INT")
    case t if t =:= typeOf[Long] || t =:= typeOf[java.lang.Long] => SQLSimpleDataType("BIGINT")
    case t if t =:= typeOf[Short] => SQLSimpleDataType("SMALLINT")
    case t if t =:= typeOf[Byte] => SQLSimpleDataType("TINYINT")
    case t if t =:= typeOf[Double] || t =:= typeOf[java.lang.Double] => SQLSimpleDataType("DOUBLE")
    case t if t =:= typeOf[Float] => SQLSimpleDataType("FLOAT")
    case t if t =:= typeOf[Boolean] || t =:= typeOf[java.lang.Boolean] => SQLSimpleDataType("BOOLEAN")
    case t if t =:= typeOf[String] => SQLSimpleDataType("TEXT")
    case t if t =:= typeOf[BigDecimal] || t =:= typeOf[java.math.BigDecimal] => SQLSimpleDataType("DECIMAL(38, 18)")
    case t if t =:= typeOf[java.sql.Date] || t =:= typeOf[java.time.LocalDate] => SQLSimpleDataType("DATE")
    case t if t =:= typeOf[java.sql.Timestamp] || t =:= typeOf[java.time.LocalDateTime] => SQLSimpleDataType("TIMESTAMP")
    case t if t =:= typeOf[java.time.Instant] => SQLSimpleDataType("TIMESTAMPTZ")
    case t if t <:< typeOf[Seq[_]] => SQLArrayDataType(sqlTypeOf(t.typeArgs.head))
    case t if t <:< typeOf[Map[_, _]] => SQLMapDataType(sqlTypeOf(t.typeArgs.head), sqlTypeOf(t.typeArgs(1)))
    case t => throw new IllegalArgumentException(s"Type $t is not supported for creating a SQLDataFrame")
  }

  // Members declared in DataFrameFunctions

  private def notImplemented(function: String): Nothing =
    throw new NotImplementedError(s"Function $function is not implemented for the SQL engine")

  private def fn(name: String, columns: GenericColumn*): SQLColumn = SQLColumn(s"$name(${columns.map(SQLColumn.of(_).expr).mkString(", ")})")

  override def col(colName: String): SQLColumn = SQLColumn.reference(colName)
  override def lit(value: Any): SQLColumn = SQLColumn.literal(value)
  override def expr(sqlExpr: String): SQLColumn = SQLColumn(sqlExpr, atomic = false)

  override def min(column: GenericColumn): SQLColumn = fn("MIN", column)
  override def max(column: GenericColumn): SQLColumn = fn("MAX", column)
  override def first(column: GenericColumn): SQLColumn = fn("FIRST", column)
  override def size(column: GenericColumn): SQLColumn = fn("ARRAY_SIZE", column)
  override def explode(column: GenericColumn): SQLColumn = fn("EXPLODE", column)
  override def abs(column: GenericColumn): SQLColumn = fn("ABS", column)
  override def least(columns: GenericColumn*): SQLColumn = fn("LEAST", columns: _*)
  override def greatest(columns: GenericColumn*): SQLColumn = fn("GREATEST", columns: _*)
  override def substring(column: GenericColumn, pos: Int, len: Int): SQLColumn = fn("SUBSTRING", column, lit(pos), lit(len))
  override def timestampAdd(column: GenericColumn, duration: Duration): SQLColumn = {
    val seconds = BigDecimal(duration.getSeconds) + BigDecimal(duration.getNano, 9)
    SQLColumn(s"${SQLColumn.of(column).operand} + INTERVAL '${seconds.bigDecimal.stripTrailingZeros.toPlainString}' SECOND", atomic = false)
  }
  override def array_construct_compact(columns: GenericColumn*): SQLColumn = notImplemented("array_construct_compact")
  override def array(columns: GenericColumn*): SQLColumn = fn("ARRAY", columns: _*)
  override def struct(columns: GenericColumn*): SQLColumn = {
    val fields = columns.map(SQLColumn.of).map { c =>
      c.getName.map(n => s"${c.expr} AS ${SQLColumn.quoteIdentifier(n)}").getOrElse(c.expr)
    }
    SQLColumn(s"STRUCT(${fields.mkString(", ")})")
  }
  override def map(columns: GenericColumn*): SQLColumn = fn("MAP", columns: _*)
  override def not(column: GenericColumn): SQLColumn = SQLColumn(s"NOT ${SQLColumn.of(column).operand}", atomic = false)
  override def count(column: GenericColumn): SQLColumn = fn("COUNT", column)
  override def countDistinct(column: GenericColumn): SQLColumn = SQLColumn(s"COUNT(DISTINCT ${SQLColumn.of(column).expr})")
  override def approxCountDistinct(column: GenericColumn, rsd: Option[Double]): SQLColumn = fn("APPROX_DISTINCT", column)
  override def coalesce(columns: GenericColumn*): SQLColumn = fn("COALESCE", columns: _*)
  override def when(condition: GenericColumn, value: GenericColumn): SQLWhenColumn = SQLWhenColumn(Seq((SQLColumn.of(condition), SQLColumn.of(value))))
  override def concat(exprs: GenericColumn*): SQLColumn = fn("CONCAT", exprs: _*)
  override def regexp_extract(e: GenericColumn, regexp: String, groupIdx: Int): SQLColumn = fn("REGEXP_EXTRACT", e, lit(regexp), lit(groupIdx))
  override def raise_error(column: GenericColumn): SQLColumn = notImplemented("raise_error")
  override def from_json(column: GenericColumn, dataType: GenericDataType): SQLColumn = notImplemented("from_json")
  override def hash(column: GenericColumn): SQLColumn = notImplemented("hash")

  override def window(aggFunction: () => GenericColumn, partitionBy: Seq[GenericColumn], orderBy: GenericColumn): SQLColumn = {
    val partition = if (partitionBy.nonEmpty) s"PARTITION BY ${partitionBy.map(SQLColumn.of(_).expr).mkString(", ")} " else ""
    SQLColumn(s"${SQLColumn.of(aggFunction()).expr} OVER (${partition}ORDER BY ${SQLColumn.of(orderBy).orderSql})")
  }
  override def row_number: SQLColumn = SQLColumn("ROW_NUMBER()")

  private def lambdaVariable(name: String) = SQLColumn(s"__sdlb_$name", name = None)
  override def transform(column: GenericColumn, func: GenericColumn => GenericColumn): SQLColumn = {
    val x = lambdaVariable("x")
    SQLColumn(s"TRANSFORM(${SQLColumn.of(column).expr}, ${x.expr} -> ${SQLColumn.of(func(x)).expr})")
  }
  override def transform_keys(column: GenericColumn, func: (GenericColumn, GenericColumn) => GenericColumn): SQLColumn = transformMap("TRANSFORM_KEYS", column, func)
  override def transform_values(column: GenericColumn, func: (GenericColumn, GenericColumn) => GenericColumn): SQLColumn = transformMap("TRANSFORM_VALUES", column, func)
  private def transformMap(name: String, column: GenericColumn, func: (GenericColumn, GenericColumn) => GenericColumn): SQLColumn = {
    val (k, v) = (lambdaVariable("k"), lambdaVariable("v"))
    SQLColumn(s"$name(${SQLColumn.of(column).expr}, (${k.expr}, ${v.expr}) -> ${SQLColumn.of(func(k, v)).expr})")
  }

  override def stringType: SQLSimpleDataType = SQLSimpleDataType("TEXT")
  override def arrayType(dataType: GenericDataType): SQLArrayDataType = createArrayDataType(dataType)
  override def structType(colTypes: Map[String, GenericDataType]): SQLStructDataType =
    SQLStructDataType(colTypes.map { case (name, tpe) => SQLField(name, SQLDataType.of(tpe)) }.toSeq)
  override def structType(fields: Seq[GenericField]): SQLStructDataType = createStructDataType(fields)
  override def mapType(keyType: GenericDataType, valueType: GenericDataType): SQLMapDataType = createMapDataType(keyType, valueType)
  override def field(name: String, dataType: GenericDataType, nullable: Boolean): SQLField = SQLField(name, SQLDataType.of(dataType), nullable)

  override def sql(query: String, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): SQLDataFrame = {
    create("sql", "query" -> query, "dialect" -> requireEngineConnection.queryDialect)
  }

  override def rowFromSeq(values: Seq[Any]): SQLRow = SQLRow(values)

  override def schemaEvolutionUdf(srcType: GenericDataType, tgtType: GenericDataType): GenericUnaryUdf = notImplemented("schemaEvolutionUdf")
}
