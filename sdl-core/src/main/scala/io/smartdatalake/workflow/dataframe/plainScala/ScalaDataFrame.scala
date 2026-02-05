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
import io.smartdatalake.workflow.dataframe._

import scala.collection.immutable.Queue
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}

case class ScalaDataFrame(cols: Seq[ScalaColumn[_]]) extends GenericDataFrame with SmartDataLakeLogger {

  //Util functions
  def checkColumnsExist(df: ScalaDataFrame, colNames: Seq[String]): Unit = {
    assert(!colNames.isEmpty, "No Columns provided for operation")
    val dfCols: Seq[String] = df.cols.seq.map(_.definition.name)
    val firstMismatch: Option[String] = colNames.find(!dfCols.contains(_))
    require(firstMismatch.isEmpty, f"The dataframe does not contain the column ${firstMismatch.get}")
  }

  def apply(i: Int): ScalaRow = {
    val row = for (c <- cols) yield c.data(i)
    ScalaRow(row.toIndexedSeq)
  }

  def apply(columnName: String): ScalaColumn[_] = cols.find(_.definition.name == columnName)
    .getOrElse(throw new IllegalArgumentException(s"column name ${columnName} does not exist in the dataframe"))

  def rows: Seq[ScalaRow] = for (i <- 0 until nrRows) yield apply(i)

  def nrRows: Int = if (isEmpty) 0 else cols.head.data.size

  def nrCols: Int = cols.size

  def dim: (Int, Int) = (nrRows, nrCols)

  override def toString: String = {
    val headerStr = cols.map(col => f"${col.definition.name} (${col.definition.dataType.typeName})").mkString("  |  ");
    val rowsStr = rows.map(_.values.mkString("    |    ")).mkString("\n");
    headerStr + "\n" + ("---------------" * nrCols) + "\n" + rowsStr
  }
  def show: Unit = println(this)


  //trait implementation

  override def schema: ScalaSchema = ScalaSchema(cols.map(_.definition))

  override def join(other: GenericDataFrame, joinCols: Seq[String], joinType: String = "inner"): ScalaDataFrame = other match {
    case otherScala: ScalaDataFrame => {
      require(joinType == "inner", "As of now, only an inner join is supported for ScalaDataFrames")
      checkColumnsExist(this, joinCols)
      checkColumnsExist(otherScala, joinCols)

      def joinColumnIndices(df: ScalaDataFrame) = joinCols.map(name => df.cols.indexWhere(_.definition.name == name))

      val indexThisCol = joinColumnIndices(this)
      val indexThatCol = joinColumnIndices(otherScala)
      val relevantIndices = (0 until otherScala.cols.size).toSet -- indexThatCol.toSet

      def nonJoinColumns(row: ScalaRow) = for (ix <- relevantIndices) yield row.values(ix)

      val ixZip = indexThisCol zip indexThatCol

      def joinCondition(thisRow: ScalaRow, thatRow: ScalaRow) = ixZip.forall(pair => thisRow(pair._1) == thatRow(pair._2))

      val rows: Seq[ScalaRow] = for (thisRow <- this.rows; thatRow <- otherScala.rows if joinCondition(thisRow, thatRow)) yield ScalaRow(thisRow.values ++ nonJoinColumns(thatRow))
      val newSchema: Option[ScalaSchema] =
        if (this.schema.isInferred || otherScala.schema.isInferred) None
        else Some(ScalaSchema(this.schema.fields ++ otherScala.schema.fields.filterNot(field => joinCols.contains(field.name))))
      ScalaDataFrame.fromScalaRows(rows = rows, schemaIn = newSchema)
    }
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  override def join(other: GenericDataFrame, condition: GenericColumn, joinType: String): GenericDataFrame = throw new NotImplementedError("Joining using a ScalaColumn[A] expression is not supported at the moment")

  private def reorderColumns(newOrder: Seq[String]): ScalaDataFrame = {
    require(newOrder.toSet == schema.columns.toSet, "Some of the provided columns either don't exist, or there are columns missing for reordering")
    ScalaDataFrame(cols = newOrder.map(this.apply))
  }

  //override in order to avoid Spark col() expression
  override def symmetricDifference(other: GenericDataFrame, diffColName: String): GenericDataFrame = {
    other match {
      case otherScala: ScalaDataFrame => {
        require(schema.columns.map(_.toLowerCase).toSet == other.schema.columns.map(_.toLowerCase).toSet, "DataFrames must have the same columns for symmetricDifference calculation")
        val otherReordered: ScalaDataFrame = otherScala.reorderColumns(newOrder = this.schema.columns)
        val df1 = this.except(otherReordered)
        val df2 = otherReordered.except(this)
        val newCol: Seq[Boolean] = (0 until(df1.count.toInt)).map(_ => true).toSeq ++ (0 until(df1.count.toInt)).map(_ => false).toSeq
        df1.unionByName(df2).withColumn(diffColName, ScalaColumn(diffColName, newCol))
      }
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)

    }
  }

  def select(columnNames: List[String]): ScalaDataFrame = {
    checkColumnsExist(this, colNames = columnNames)
    ScalaDataFrame(cols.filter(c => columnNames.contains(c.definition.name)))
  }

  def select(columnName: String): ScalaDataFrame = {
    select(List(columnName))
  }

  override def select(columns: Seq[GenericColumn]): ScalaDataFrame = {
    require(columns.nonEmpty && columns.forall(_.isInstanceOf[ScalaColumn[_]]), "The 'select' operation requires at least one column, which must be of type ScalaColumn")
    select(columns.map(_.asInstanceOf[ScalaColumn[_]].definition.name).toList)
  }

  //TODO
  override def groupBy(columns: Seq[GenericColumn]): GenericGroupedDataFrame = ???

  override def agg(columns: Seq[GenericColumn]): ScalaDataFrame = throw new NotImplementedError("Aggregations using the agg-expression are not supported at the moment")

  override def unionByName(other: GenericDataFrame): ScalaDataFrame = other match {
    case otherScala: ScalaDataFrame => {
      checkColumnsExist(otherScala, columns)
      val zipped = cols.sortBy(_.definition.name) zip otherScala.cols.filter(c => columns.contains(c.definition.name)).sortBy(_.definition.name)
      ScalaDataFrame(zipped.map(pair => pair._1 unsafeAppend pair._2))
    }
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  override def except(other: GenericDataFrame): GenericDataFrame = other match {
    case otherScala: ScalaDataFrame => {
      require(schema == otherScala.schema, "The except operation can only be carried out with two dataframes with the same schema")
      ScalaDataFrame.fromScalaRows(rows = (rows.toSet -- otherScala.rows.toSet).toSeq, schemaIn = Some(schema))
    }
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  override def filter(expression: GenericColumn): ScalaDataFrame = {
    val newDf = withColumn("filterExpr", expression).asInstanceOf[ScalaDataFrame]
    val filteredRows = newDf.rows.filter(_.values.last == true).map(r => r.copy(values = r.values.init)) //requires withColumn() to write the new column at the last index
    ScalaDataFrame.fromScalaRows(filteredRows, Some(schema))
  }

  override def collect: Seq[GenericRow] = rows

  override def distinct: ScalaDataFrame = ScalaDataFrame.fromScalaRows(rows = rows.distinct, schemaIn = Some(schema))

  //In order for "filter" to work, the new column must be written at the last index
  def withColumnScala[_](colName: String, expression: ScalaAbstractColumn): ScalaDataFrame = {
    ScalaDataFrame(cols = this.cols :+ expression.as(colName).toScalaColumn(this))
  }

  override def withColumn(colName: String, expression: GenericColumn): ScalaDataFrame = expression match {
    case exploding: ScalaExplodingColumn[Any] => exploding.changeColumnName(colName).mergeWithScalaDataFrame(this)
    case sc: ScalaAbstractColumn => withColumnScala(colName, sc.toScalaColumn(this))
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(expression)
  }

  override def withColumnRenamed(colName: String, newName: String): ScalaDataFrame = {
    // TODO: throw exception if colName not found
    val newCols = cols.map(c => if (c.definition.name == colName) c.as(newName).toScalaColumn(this) else c)
    ScalaDataFrame(newCols)
  }

  override def drop(colName: String): ScalaDataFrame = ScalaDataFrame(cols.filterNot(_.definition.name == colName))

  override def drop(col: GenericColumn): ScalaDataFrame = col match {
    case sc: ScalaColumn[_] => drop(sc.definition.name)
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
    ScalaDataFrame.fromScalaRows(uniqueRows, Some(schema))
  }

  override def isEmpty: Boolean = cols.isEmpty || cols.head.data.isEmpty
  override def count: Long = nrRows

  override def cache: ScalaDataFrame = this

  override def uncache: ScalaDataFrame = this

  override def as(alias: String): ScalaDataFrame = {
    logger.warn("The 'as' operation is not avaiable in ScalaDataFrames and will just return the same df")
    this
  }
  override def showString(options: Map[String, String]): String = {
    logger.info("showString for ScalaDataframe will ignore the provided options")
    toString
  }
  override def explainString(options: Map[String, String]): String = {
    logger.info("explain for ScalaDataframe will ignore the provided options and just return the dataframe as String")
    toString
  }

  /**
   * Create an Observation of metrics on this DataFrame.
   *
   * @param name             name of the observation
   * @param aggregateColumns aggregate columns to observe on the DataFrame
   * @return an Observation object which can return observed metrics after execution
   */
  override def setupObservation(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean, forceGenericObservation: Boolean): (GenericDataFrame, DataFrameObservation) = ???

  override def observe(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean): ScalaDataFrame = {
    logger.info("The 'observe' method in ScalaDataFrames will not change the dataframe")
    this
  }

  /**
   * Create an empty SubFeed for this subFeedType.
   */
  override def getDataFrameSubFeed(dataObjectId: SdlConfigObject.DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed = ???

  override def subFeedType: universe.Type = universe.typeOf[ScalaSubFeed]

  def returnEmpty: ScalaDataFrame = ScalaDataFrame.returnEmpty(this.schema)

  override def limit(n: Int): ScalaDataFrame = {
    copy(cols = cols.map(_.limit(n)))
  }
}


object ScalaDataFrame {
  def apply(rows: Seq[Seq[Any]], schema: Option[ScalaSchema] = None): ScalaDataFrame = {
    fromScalaRows(rows.map(row => ScalaRow(row.toIndexedSeq)), schema)
  }

  def apply[A <: Product : ClassTag](rows: Seq[A]): ScalaDataFrame = {
    val classAccessors = ProductUtil.classAccessors[A]()
    val mirror = scala.reflect.runtime.currentMirror
    val schema = ScalaSchema(classAccessors.map(acc => ScalaDataType.getFor(mirror.runtimeClass(acc.returnType)).createColumnDefinition(acc.name.toTermName.toString)))
    val cols = schema.columns
    fromScalaRows(rows.map(row => ScalaRow(cols.map(ProductUtil.getRawFieldData(row, _)).toIndexedSeq)), Some(schema))
  }

  def apply(rows: Seq[Seq[Any]], colNames: Seq[String]): ScalaDataFrame = {

    def inferSchema: ScalaSchema = {
      val colDefs = if (rows.isEmpty) throw new IllegalStateException("Cannot infer schema without data")
      else rows.head.zip(colNames).map { case (v, name) =>
        val dataType = ScalaDataType.getFor(v.getClass)
        dataType.createColumnDefinition(name)
      }
      ScalaSchema(colDefs)
    }

    fromScalaRows(rows.map(row => ScalaRow(row.toIndexedSeq)), Some(inferSchema))
  }

  def fromScalaRows(rows: Seq[ScalaRow], schemaIn: Option[ScalaSchema] = None): ScalaDataFrame = {

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
      case Success(columns) => new ScalaDataFrame(columns)
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
    def toDF(colNames: String*): ScalaDataFrame = ScalaDataFrame(seq, colNames)
  }
}