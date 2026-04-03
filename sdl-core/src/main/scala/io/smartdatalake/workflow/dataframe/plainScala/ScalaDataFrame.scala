/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.dataframe.plainScala

import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{ProductUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.DataFrameSubFeed.assertCorrectSubFeedType
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataframe.plainScala.ScalaDataFrameImplicits.OrderingOps

import scala.collection.immutable.Queue
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}

/**
 * A pures Scala DataFrame implementation mainly for testing without Spark dependencies.
 * See [[ScalaSubFeed]] for limitations.
 */
case class ScalaDataFrame(cols: Seq[ScalaColumn[_]], alias: Option[String] = None) extends GenericDataFrame with SmartDataLakeLogger {

  //Util functions
  def checkColumnsExist(df: ScalaDataFrame, colNames: Seq[String]): Unit = {
    assert(colNames.nonEmpty, "No Columns provided for operation")
    val dfColumns = df.columns.toSet
    val firstMismatch: Option[String] = colNames.find(c => !dfColumns.contains(c) && !c.endsWith("*") )
    require(firstMismatch.isEmpty, f"The dataframe does not contain the column ${firstMismatch.get}")
  }

  def apply(i: Int): ScalaRow = {
    ScalaRow(cols.map(c => c.data(i)).toIndexedSeq)
  }

  def apply(columnName: String): ScalaAbstractColumn = {
    columnName.split('.') match {
      case Array(_, "*") => ScalaColumnReference(columnName)
      case Array("*") => throw new IllegalArgumentException(s"Star expand without alias to get Columns from DataFrame is not supported. Use select(*) instead.")
      case _ => cols.find(_.definition.name == columnName)
        .getOrElse(throw new IllegalArgumentException(s"column name ${columnName} does not exist in the dataframe"))

    }
  }

  def rows: Seq[ScalaRow] = for (i <- 0 until nrRows) yield apply(i)

  def nrRows: Int = if (isEmpty) 0 else cols.head.data.size

  def nrCols: Int = cols.size

  def dim: (Int, Int) = (nrRows, nrCols)

  override def toString: String = {
    s"ScalaDataFrame$dim: ${cols.map(_.definition.name).mkString(" | ")}"
  }

  def showString: String = {
    def vToString(v: Any) = Option(v).map(_.toString).getOrElse("<null>")
    val colSizes = cols.map(c => (c.definition.name +: c.data.map(vToString)).map(_.length).max)
    val headerStr = cols.zip(colSizes).map{ case (c,s) => c.definition.name.padTo(s, " ").mkString}.mkString(" | ")
    val rowsStr = rows.map(_.values.zip(colSizes).map{ case (v,s) => vToString(v).padTo(s, " ").mkString}.mkString(" | ")).mkString(System.lineSeparator())
    val separatorLine = Seq.fill(headerStr.length)("-").mkString
    separatorLine + System.lineSeparator() + headerStr + System.lineSeparator() + separatorLine + System.lineSeparator() + rowsStr
  }

  def show: Unit = println(showString)


  override def schema: ScalaSchema = ScalaSchema(cols.map(_.definition))

  override def join(other: GenericDataFrame, joinCols: Seq[String], joinType: String = "inner"): ScalaDataFrame = other match {
    case otherScala: ScalaDataFrame => {
      checkColumnsExist(this, joinCols)
      checkColumnsExist(otherScala, joinCols)

      def joinColumnIndices(df: ScalaDataFrame) = joinCols.map(name => df.cols.indexWhere(_.definition.name == name))

      val indicesThisJoinCol = joinColumnIndices(this)
      val indicesThatJoinCol = joinColumnIndices(otherScala)
      val indicesThisNonJoinCol = (this.cols.indices.toSet -- indicesThisJoinCol.toSet).toSeq
      val indicesThatNonJoinCol = (otherScala.cols.indices.toSet -- indicesThatJoinCol.toSet).toSeq
      def filterRow(row: ScalaRow, indices: Iterable[Int]) = indices.map(row.values)

      val newSchema = ScalaSchema(indicesThisJoinCol.map(this.schema.fields) ++ indicesThisNonJoinCol.map(this.schema.fields) ++ indicesThatNonJoinCol.map(otherScala.schema.fields))

      lazy val leftGrouped = this.rows.groupBy(filterRow(_, indicesThisJoinCol))
      lazy val rightGrouped = otherScala.rows.groupBy(filterRow(_, indicesThatJoinCol))

      val rows = joinType.toLowerCase match {
        case "inner" =>
          this.rows.flatMap(thisRow =>
            rightGrouped.get(filterRow(thisRow, indicesThisJoinCol)) match {
              case Some(matchingRows) => matchingRows.map(thatRow => ScalaRow((filterRow(thisRow, indicesThisJoinCol) ++ filterRow(thisRow, indicesThisNonJoinCol) ++ filterRow(thatRow, indicesThatNonJoinCol)).toIndexedSeq))
              case None => Seq()
            }
          )
        case "left" =>
          this.rows.flatMap(thisRow =>
            rightGrouped.get(filterRow(thisRow, indicesThisJoinCol)) match {
              case Some(matchingRows) => matchingRows.map(thatRow => ScalaRow((filterRow(thisRow, indicesThisJoinCol) ++ filterRow(thisRow, indicesThisNonJoinCol) ++ filterRow(thatRow, indicesThatNonJoinCol)).toIndexedSeq))
              case None => Seq(ScalaRow((filterRow(thisRow, indicesThisJoinCol) ++ filterRow(thisRow, indicesThisNonJoinCol) ++ Seq.fill(indicesThatNonJoinCol.size)(null)).toIndexedSeq))
            }
          )
        case "right" =>
          otherScala.rows.flatMap(thatRow =>
            leftGrouped.get(filterRow(thatRow, indicesThatJoinCol)) match {
              case Some(matchingRows) => matchingRows.map(thisRow => ScalaRow((filterRow(thatRow, indicesThatJoinCol) ++ filterRow(thisRow, indicesThisNonJoinCol) ++ filterRow(thatRow, indicesThatNonJoinCol)).toIndexedSeq))
              case None => Seq(ScalaRow((filterRow(thatRow, indicesThatJoinCol) ++ Seq.fill(indicesThisNonJoinCol.size)(null) ++ filterRow(thatRow, indicesThatNonJoinCol)).toIndexedSeq))
            }
          )
        case "full" =>
          this.rows.flatMap(thisRow =>
            rightGrouped.get(filterRow(thisRow, indicesThisJoinCol)) match {
              case Some(matchingRows) => matchingRows.map{
                thatRow =>
                  ScalaRow((filterRow(thisRow, indicesThisJoinCol) ++ filterRow(thisRow, indicesThisNonJoinCol) ++ filterRow(thatRow, indicesThatNonJoinCol)).toIndexedSeq)
              }
              case None => Seq(ScalaRow((filterRow(thisRow, indicesThisJoinCol) ++ filterRow(thisRow, indicesThisNonJoinCol) ++ Seq.fill(indicesThatNonJoinCol.size)(null)).toIndexedSeq))
            }
          ) ++ otherScala.rows.flatMap(thatRow =>
            leftGrouped.get(filterRow(thatRow, indicesThatJoinCol)) match {
              case Some(_) => Seq() //already included in the left join part
              case None => Seq(ScalaRow((filterRow(thatRow, indicesThatJoinCol) ++ Seq.fill(indicesThisNonJoinCol.size)(null) ++ filterRow(thatRow, indicesThatNonJoinCol)).toIndexedSeq))
            }
          )
        case _ => throw new IllegalArgumentException(s"Join type $joinType is not supported. Supported join types are: inner, left, right, full")
      }
      ScalaDataFrame.fromRows(rows = rows, schemaIn = Some(newSchema))
    }
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  /**
   * Joining with condition
   * This needs a cross join and then filtering, which is not very efficient...
   */
  override def join(other: GenericDataFrame, condition: GenericColumn, joinType: String): ScalaDataFrame = (other, condition) match {
    case (that: ScalaDataFrame, scalaCondition: ScalaAbstractColumn) =>
      val newSchema = ScalaSchema(this.schema.fields ++ that.schema.fields)
      val emptyDf = ScalaDataFrame.returnEmpty(newSchema)
      scalaCondition.visit(_.markForDataReset(), (_: Unit, _: Unit) => ())

      def joinRowWithThat(thisRow: ScalaRow, thatRows: Seq[ScalaRow], joinType: String): ScalaDataFrame = {
        val combinedRows = thatRows.map(thatRow => ScalaRow(thisRow.values ++ thatRow.values))
        val df = ScalaDataFrame.fromRows(combinedRows, Some(newSchema))
          .filter(scalaCondition)
        joinType match {
          case "inner" => df
          case "left"  =>
            if (df.isEmpty) ScalaDataFrame.fromData(Seq(thisRow.values ++ Seq.fill(that.schema.fields.size)(null)), Some(newSchema))
            else df
          case "anti"  =>
            if (df.isEmpty) ScalaDataFrame.fromData(Seq(thisRow.values ++ Seq.fill(that.schema.fields.size)(null)), Some(newSchema))
            else emptyDf
        }
      }

      def joinRowWithThis(thatRow: ScalaRow, thisRows: Seq[ScalaRow], joinType: String): ScalaDataFrame = {
        val combinedRows = thisRows.map(thisRow => ScalaRow(thisRow.values ++ thatRow.values))
        val df = ScalaDataFrame.fromRows(combinedRows, Some(newSchema))
          .filter(scalaCondition)
        joinType match {
          case "inner" => df
          case "right" =>
            if (df.isEmpty) ScalaDataFrame.fromData(Seq(Seq.fill(this.schema.fields.size)(null) ++ thatRow.values), Some(newSchema))
            else df
          case "anti" =>
            if (df.isEmpty) ScalaDataFrame.fromData(Seq(Seq.fill(this.schema.fields.size)(null) ++ thatRow.values), Some(newSchema))
            else emptyDf
        }
      }

      joinType.toLowerCase match {
        case "inner" | "left" =>
          this.rows.map(thisRow => joinRowWithThat(thisRow, that.rows, joinType))
            .reduceOption(_.unionAll(_)).getOrElse(emptyDf)
        case "right" =>
          that.rows.map(thatRow => joinRowWithThis(thatRow, this.rows, joinType))
            .reduceOption(_.unionAll(_)).getOrElse(emptyDf)
        case "full" =>
          val leftJoined = this.rows.map(thisRow => joinRowWithThat(thisRow, that.rows, "left"))
            .reduceOption(_.unionAll(_)).getOrElse(emptyDf)
          val rightAntiJoined = that.rows.map(thatRow => joinRowWithThis(thatRow, this.rows, "anti"))
            .reduceOption(_.unionAll(_)).getOrElse(emptyDf)
          leftJoined.unionAll(rightAntiJoined)
        case _ => throw new IllegalArgumentException(s"Join type $joinType is not supported. Supported join types are: inner, left, right, full")
      }

    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  //override in order to avoid Spark col() expression
  override def symmetricDifference(other: GenericDataFrame, diffColName: String): GenericDataFrame = {
    other match {
      case otherScala: ScalaDataFrame => {
        require(schema.columns.map(_.toLowerCase).toSet == other.schema.columns.map(_.toLowerCase).toSet, "DataFrames must have the same columns for symmetricDifference calculation")
        val otherReordered: ScalaDataFrame = otherScala.select(this.schema.columns.toList)
        val df1 = this.except(otherReordered)
        val df2 = otherReordered.except(this)
        val newCol: Seq[Boolean] = (0 until(df1.count.toInt)).map(_ => true).toSeq ++ (0 until(df1.count.toInt)).map(_ => false).toSeq
        df1.unionByName(df2).withColumn(diffColName, ScalaColumn(diffColName, newCol))
      }
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)

    }
  }

  def select(columnName: String): ScalaDataFrame = {
    select(List(columnName))
  }

  def select(selectColumnNames: List[String]): ScalaDataFrame = {
    checkColumnsExist(this, colNames = selectColumnNames)
    val expandedCols = selectColumnNames.flatMap(filterCols(_, cols))
    ScalaDataFrame(expandedCols, None)
  }

  private def filterCols(columnName: String, cols: Seq[ScalaColumn[_]]): Seq[ScalaColumn[_]] = {
    // star expand if needed
    columnName.split('.') match {
      case Array(alias, name) if name == "*" => cols.filter(_.definition.dataFrameAlias.contains(alias))
      case Array(alias, name) => cols.filter(c => c.definition.name == name && c.definition.dataFrameAlias.contains(alias))
      case Array(name) if name == "*" => cols
      case Array(name) => cols.filter(c => c.definition.name == name)
    }
  }

  override def select(selectColumns: Seq[GenericColumn]): ScalaDataFrame = {
    assertCorrectSubFeedType(subFeedType, selectColumns)
    // star expand
    val expandedCols = selectColumns.map(_.asInstanceOf[ScalaAbstractColumn].toScalaColumn(this)).flatMap {
      case c: ScalaColumn[_] if c.definition.name == "*" && c.definition.dataFrameAlias.isDefined => this.cols.filter(_.definition.dataFrameAlias.contains(c.definition.dataFrameAlias.get))
      case c: ScalaColumn[_] if c.definition.name == "*" => this.cols
      case c => Seq(c)
    }
    ScalaDataFrame(expandedCols)
  }

  override def groupBy(columns: Seq[GenericColumn]): GenericGroupedDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    ScalaGroupedDataFrame(columns.map(_.asInstanceOf[ScalaAbstractColumn]), this)
  }

  override def agg(columns: Seq[GenericColumn]): ScalaDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    val aggCols = columns.map {
      case c: ScalaAbstractColumn =>
        val scalaCol = c.toScalaColumn(this)
        assert(scalaCol.length == 1, s"Aggregate columns must have exactly one value, ${scalaCol.definition.name} has ${scalaCol.length} values. Make sure to use an aggregate function.")
        scalaCol
      case c => DataFrameSubFeed.throwIllegalSubFeedTypeException(c)
    }
    ScalaDataFrame(aggCols)
  }

  override def unionByName(other: GenericDataFrame, allowMissingColumns: Boolean = false): ScalaDataFrame = other match {
    case otherScala: ScalaDataFrame =>
      def getDuplicatedCols(cols: Seq[ScalaColumn[_]]): Seq[String] = cols.groupBy(_.definition.name.toLowerCase).mapValues(_.size).filter(_._2 > 1).keys.toSeq.sorted
      assert(getDuplicatedCols(this.cols).isEmpty, s"Duplicate column names '${getDuplicatedCols(this.cols).mkString(", ")}' found in this dataframe, cannot perform unionByName. Make sure all column names are unique for this operation.")
      assert(getDuplicatedCols(otherScala.cols).isEmpty, s"Duplicate column names '${getDuplicatedCols(otherScala.cols).mkString(", ")}' found in other dataframe, cannot perform unionByName. Make sure all column names are unique for this operation.")
      if (!allowMissingColumns) checkColumnsExist(otherScala, this.columns)
      val thisCols = this.cols.map(c =>
        c.definition.name -> c.asInstanceOf[ScalaColumn[Any]] //TODO: can be removed when we switch to Scala 2.13, because of improved type inference
      ).toMap
      val otherCols = otherScala.cols.map(c =>
        c.definition.name -> c.asInstanceOf[ScalaColumn[Any]] //TODO: can be removed when we switch to Scala 2.13, because of improved type inference
      ).toMap
      val finalColNames = if (allowMissingColumns) columns ++ otherScala.columns.diff(columns)
      else columns
      assert(finalColNames.nonEmpty, "No common columns found between the two dataframes for unionByName. Make sure to have at least one column with the same name in both dataframes or set allowMissingColumns=true.")
      val unionData = finalColNames.map { colName =>
        val thisCol = thisCols.getOrElse(colName, otherCols(colName).definition.createColumn(IndexedSeq.fill(this.nrRows)(null)))
        val otherCol = otherCols.getOrElse(colName, thisCols(colName).definition.createColumn(IndexedSeq.fill(otherScala.nrRows)(null)))
        assert(thisCol.definition.dataType == otherCol.definition.dataType || thisCol.definition.dataType == ScalaNullDataType || otherCol.definition.dataType == ScalaNullDataType, s"Data types for column $colName do not match between the two dataframes (${thisCol.definition.dataType.getClass.getSimpleName} != ${otherCol.definition.dataType.getClass.getSimpleName}")
        if (thisCol.definition.dataType == ScalaNullDataType) otherCol.definition.createColumn(data = thisCol.data ++ otherCol.data)
        else thisCol.definition.createColumn(data = thisCol.data ++ otherCol.data)
      }
      ScalaDataFrame(unionData)
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }


  def unionAll(other: GenericDataFrame): ScalaDataFrame = other match {
    case otherScala: ScalaDataFrame =>
      // assert schema has same dataTypes in same order, otherwise union is not possible
      assert(columns.size == otherScala.columns.size, "The two dataframes must have the same number of columns for unionAll")
      val newFields = schema.fields.zip(otherScala.schema.fields).map{
        case (f1, f2) =>
          assert(f1.dataType == f2.dataType || f1.dataType == ScalaNullDataType || f2.dataType == ScalaNullDataType, s"Data types for column ${f1.name} do not match between the two dataframes (${f1.dataType} != ${f2.dataType})")
          if (f1.dataType == ScalaNullDataType) f2 else f1
      }
      ScalaDataFrame.fromData(this.rows.map(_.values) ++ otherScala.rows.map(_.values), Some(ScalaSchema(newFields)))
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  override def except(other: GenericDataFrame): ScalaDataFrame = other match {
    case otherScala: ScalaDataFrame => {
      require(schema.columns == otherScala.columns, "The except operation can only be carried out with two dataframes with the same columns")
      ScalaDataFrame.fromRows(rows = (rows.toSet -- otherScala.rows.toSet).toSeq, schemaIn = Some(schema))
    }
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  override def filter(expression: GenericColumn): ScalaDataFrame = expression match {
    case scalaExpr: ScalaAbstractColumn =>
      assert(scalaExpr.dataType == ScalaBooleanDataType, "The filter expression must have a boolean return type")
      val exprData = scalaExpr.toScalaColumn(this).data
      val filteredRows = this.rows.zip(exprData).filter(_._2 == true).map(_._1)
      ScalaDataFrame.fromRows(filteredRows, Some(schema))
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(expression)
  }

  override def orderBy(columns: Seq[GenericColumn]): ScalaDataFrame = {
    DataFrameSubFeed.assertCorrectSubFeedType(subFeedType, columns)
    require(columns.nonEmpty, "The 'orderBy' operation requires at least one column")
    val sortColDef = columns.map(_.asInstanceOf[ScalaAbstractColumn].toScalaColumn(this).definition)
    val combinedOrdering = sortColDef.map { c =>
      val ordering = c.dataType.ordering.asInstanceOf[Ordering[Any]]
      val idx = cols.indexWhere(_.definition.name == c.name)
      Ordering.by[ScalaRow, Any](row => row.values(idx))(ordering)
    }.reduceLeft(_ orElse _)
    val sortedRows = rows.sorted(combinedOrdering)
    ScalaDataFrame.fromRows(sortedRows, Some(schema))
  }

  override def collect: Seq[GenericRow] = rows

  override def distinct: ScalaDataFrame = ScalaDataFrame.fromRows(rows = rows.distinct, schemaIn = Some(schema))

  def withColumnScala(column: ScalaColumn[_]): ScalaDataFrame = {
    val filteredCols = cols.filterNot(_.definition.name == column.definition.name)
    ScalaDataFrame(cols = filteredCols :+ column, alias = None)
  }

  //In order for "filter" to work, the new column must be written at the last index
  def withColumnScala(colName: String, expression: ScalaAbstractColumn): ScalaDataFrame = {
    withColumnScala(expression.as(colName).toScalaColumn(this))
  }

  override def withColumn(colName: String, expression: GenericColumn): ScalaDataFrame = expression match {
    case exploding: ScalaExplodeExpr => exploding.explodeDataFrame(colName, this)
    case sc: ScalaAbstractColumn => withColumnScala(colName, sc)
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(expression)
  }

  override def withColumnRenamed(colName: String, newName: String): ScalaDataFrame = {
    // TODO: throw exception if colName not found
    val newCols = cols.map(c => if (c.definition.name == colName) c.as(newName).toScalaColumn(this) else c)
    ScalaDataFrame(newCols)
  }

  override def drop(colName: String): ScalaDataFrame = {
    colName.split('.') match {
      case Array(alias, name) => ScalaDataFrame(cols.filterNot(c => c.definition.dataFrameAlias.contains(alias) && c.definition.name == name))
      case Array(name) => ScalaDataFrame(cols.filterNot(c => c.definition.name == name))
    }
  }

  override def drop(col: GenericColumn): ScalaDataFrame = col match {
    case sc: ScalaColumn[_] => drop(sc.definition.getFullName())
    case sc: ScalaAbstractColumn => drop(sc.getName.getOrElse(throw new IllegalArgumentException(s"Cannot drop column ${sc}, because it does not have a name. Make sure to use a column reference with a name.")))
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(col)
  }

  override def createOrReplaceTempView(viewName: String): Unit = logger.warn("Temporary views are not available in ScalaDataFrames")

  override def dropDuplicates(cols: Seq[String]): ScalaDataFrame = {
    val colsIndices: Seq[Int] = cols.map(c => this.cols.indexWhere(_.definition.name == c))
    val uniqueRows = rows.foldLeft(Set[Any](), Queue[ScalaRow]())((setQueue, row) => {
      val (set, queue) = setQueue
      val duplValues = for (i <- colsIndices) yield row.values(i)
      if (set.contains(duplValues)) setQueue else (set + duplValues, queue :+ row)
    })._2
    ScalaDataFrame.fromRows(uniqueRows, Some(schema))
  }

  override def isEmpty: Boolean = cols.isEmpty || cols.head.data.isEmpty
  override def count: Long = nrRows

  override def cache: ScalaDataFrame = this

  override def uncache: ScalaDataFrame = this

  override def as(alias: String): ScalaDataFrame = {
    ScalaDataFrame(cols.map(_.withDataFrameAlias(Option(alias))), alias = Some(alias))
  }

  override def showString(options: Map[String, String]): String = {
    if (options.nonEmpty) logger.debug("showString for ScalaDataframe will ignore the provided options")
    showString
  }
  override def explainString(options: Map[String, String]): String = {
    if (options.nonEmpty) logger.debug("explain for ScalaDataframe will ignore the provided options and just return the dataframe as String")
    toString
  }

  /**
   * Create an Observation of metrics on this DataFrame.
   *
   * @param name             name of the observation
   * @param aggregateColumns aggregate columns to observe on the DataFrame
   * @return an Observation object which can return observed metrics after execution
   */
  override def setupObservation(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean, forceGenericObservation: Boolean): (GenericDataFrame, DataFrameObservation) = {
    val observation = GenericCalculatedObservation(this, aggregateColumns: _*)
    // Cache the DataFrame to avoid duplicate calculation. If cache is not needed, create a GenericCalculationObservation directly.
    (this, observation)
  }

  override def observe(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean): ScalaDataFrame = {
    logger.info("The 'observe' method in ScalaDataFrames will not change the dataframe")
    this
  }

  /**
   * Create an empty SubFeed for this subFeedType.
   */
  override def getDataFrameSubFeed(dataObjectId: SdlConfigObject.DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): ScalaSubFeed = {
    ScalaSubFeed(Some(this), dataObjectId, partitionValues, filter = filter)
  }

  override def subFeedType: universe.Type = universe.typeOf[ScalaSubFeed]

  def returnEmpty: ScalaDataFrame = ScalaDataFrame.returnEmpty(this.schema)

  override def limit(n: Int): ScalaDataFrame = {
    copy(cols = cols.map(_.limit(n)))
  }
}


object ScalaDataFrame {
  def fromData(rows: Seq[Seq[Any]], schema: Option[ScalaSchema] = None): ScalaDataFrame = {
    fromRows(rows.map(row => ScalaRow(row.toIndexedSeq)), schema)
  }

  def fromData[A <: Product : ClassTag](rows: Seq[A]): ScalaDataFrame = {
    val classAccessors = ProductUtil.classAccessors[A]()
    val mirror = scala.reflect.runtime.currentMirror
    val schema = ScalaSchema(classAccessors.map(acc => ScalaDataType.getFor(mirror.runtimeClass(acc.returnType)).createColumnDefinition(acc.name.toTermName.toString)))
    val cols = schema.columns
    fromRows(rows.map(row => ScalaRow(cols.map(ProductUtil.getRawFieldData(row, _)).toIndexedSeq)), Some(schema))
  }

  def fromData(rows: Seq[Seq[Any]], colNames: Seq[String]): ScalaDataFrame = {

    def inferSchema: ScalaSchema = {
      val colDefs = if (rows.isEmpty) throw new IllegalStateException("Cannot infer schema without data")
      else {
        colNames.zipWithIndex.map { case (c,idx) =>
          val sample = rows.find(row => row(idx) != null).map(row => row(idx))
          val dataType = ScalaDataType.getFor(sample.map(_.getClass).getOrElse(classOf[Null]))
          dataType.createColumnDefinition(c)
        }
      }
      ScalaSchema(colDefs)
    }

    fromRows(rows.map(row => ScalaRow(row.toIndexedSeq)), Some(inferSchema))
  }

  def fromRows(rows: Seq[ScalaRow], schemaIn: Option[ScalaSchema] = None): ScalaDataFrame = {

    def inferSchema: ScalaSchema = {
      val colDefs = if (rows.isEmpty) throw new IllegalStateException("Cannot infer schema without data")
      else rows.head.values.zipWithIndex.map { case (v, i) =>
        val dataType = ScalaDataType.getFor(v.getClass)
        dataType.createColumnDefinition(s"col$i")
      }
      ScalaSchema(colDefs)
    }

    val schema = schemaIn.getOrElse(inferSchema)

    //create columns from schema/values pairs
    val colsTry: Try[Seq[ScalaColumn[_]]] = Try(rows.map(row => row.values).transpose.zip(schema.fields)
      .map(v => v._2.createColumn(v._1.toIndexedSeq)))

    colsTry match {
      case Success(_) if rows.isEmpty => ScalaDataFrame.returnEmpty(schema)
      case Success(columns) => new ScalaDataFrame(columns, alias = None)
      case Failure(e) if e.getMessage.startsWith("transpose requires all collections to have the same size") => //error with transpose operation
        throw new IllegalArgumentException("Could not create dataframe, rows must have the same size")
      case Failure(e) => throw e
    }
  }

  def returnEmpty(schema: ScalaSchema): ScalaDataFrame = {
    schema.toEmptyScalaDataFrame
  }

  val implicits: ScalaDataFrameImplicits.type = ScalaDataFrameImplicits

}

object ScalaDataFrameImplicits {
  implicit class SeqToDataFrame(seq: Seq[Seq[Any]]) {
    def toDF(colNames: String*): ScalaDataFrame = ScalaDataFrame.fromData(seq, colNames)
  }

  // This class can be removed once we are on Scala 2.13 and can use the built-in orElse method on Ordering
  implicit class OrderingOps[T](val self: Ordering[T]) extends AnyVal {
    def orElse(other: Ordering[T]): Ordering[T] =
      Ordering.fromLessThan { (a, b) =>
        val cmp = self.compare(a, b)
        if (cmp != 0) cmp < 0
        else other.compare(a, b) < 0
      }
  }
}

