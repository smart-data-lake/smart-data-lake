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
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe.{DataFrameObservation, GenericColumn, GenericDataFrame, GenericGroupedDataFrame, GenericRow, GenericSchema}
import io.smartdatalake.workflow.dataframe.plainScala.ScalaDataTypeEnum.ScalaDataTypeEnum

import scala.reflect.runtime.universe
import annotation.tailrec
import scala.util.{Failure, Success, Try}
import scala.collection.immutable.Queue

case class ScalaDataframe(cols: Seq[ScalaColumn[Any]]) extends GenericDataFrame with SmartDataLakeLogger{

  //Util functions
  def checkColumnsExist(df: ScalaDataframe, colNames: Seq[String]): Unit = {
    assert(!colNames.isEmpty, "No Columns provided for operation")
    val dfCols: Seq[String] = df.cols.seq.map(_.name)
    val firstMismatch: Option[String] = colNames.find(!dfCols.contains(_))
    require(firstMismatch.isEmpty, f"The dataframe does not contain the column ${firstMismatch.get}")
  }

  def apply(i: Int): ScalaRow = {
    val row = for (c <- cols) yield c.inner(i)
    ScalaRow(row)
  }
  def apply(columnName: String): ScalaColumn[Any] = cols.find(_.name == columnName).getOrElse(throw new IllegalArgumentException(s"column name ${columnName} does not exist in the dataframe"))

  def rows: Seq[ScalaRow] = for (i <- 0 until nrRows) yield apply(i)

  def nrRows: Int = if (isEmpty) 0 else cols.head.inner.size

  def nrCols: Int = cols.size

  def dim: (Int, Int) = (nrRows, nrCols)

  override def toString: String = {
    val headerStr = cols.map(col => f"${col.name} (${col.metadata.dataType.inner})").mkString("  |  ");
    val rowsStr = rows.map(_.value.mkString("    |    ")).mkString("\n");
    headerStr + "\n" + ("---------------" * nrCols) + "\n" + rowsStr
  }
  def show: Unit = println(this)


  //trait implementation

  override def schema: ScalaSchema = ScalaSchema(cols.map(_.metadata).toList)

  override def join(other: GenericDataFrame, joinCols: Seq[String], joinType: String): GenericDataFrame = other match {
    case otherScala: ScalaDataframe => {
      require(joinType == "inner", "As of now, only an inner join is supported for ScalaDataframes")
      checkColumnsExist(this, joinCols)
      checkColumnsExist(otherScala, joinCols)
      def joinColumnIndices(df: ScalaDataframe) = joinCols.map(name => df.cols.indexWhere(_.name == name))
      val (indexThisCol, indexThatCol): (Seq[Int], Seq[Int]) = (joinColumnIndices(this), joinColumnIndices(otherScala))
      val relevantIndices = (0 until otherScala.cols.size).toSet -- indexThatCol.toSet

      def nonJoinColumns(row: ScalaRow) = for (ix <- relevantIndices) yield row.value(ix)

      val ixZip = indexThisCol zip indexThatCol

      def joinCondition(thisRow: ScalaRow, thatRow: ScalaRow) = ixZip.forall(pair => thisRow(pair._1) == thatRow(pair._2))

      val rows: Seq[ScalaRow] = for (thisRow <- this.rows; thatRow <- otherScala.rows if joinCondition(thisRow, thatRow)) yield ScalaRow(thisRow.value ++ nonJoinColumns(thatRow))
      val newSchema: Option[ScalaSchema] =
        if (this.schema.isInferred || otherScala.schema.isInferred) None
        else Some(ScalaSchema(this.schema._fields ++ otherScala.schema._fields.filterNot(field => joinCols.contains(field.name))))
      ScalaDataframe.fromScalaRows(rows = rows, _schema = newSchema)
    }
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  override def join(other: GenericDataFrame, condition: GenericColumn, joinType: String): GenericDataFrame = throw new NotImplementedError("Joining using a ScalaColumn[A] expression is not supported at the moment")

  def select(columnNames: List[String]): ScalaDataframe = {
    checkColumnsExist(this, colNames = columnNames)
    ScalaDataframe(cols.filter(c => columnNames.contains(c.name)))
  }

  def select(columnName: String): ScalaDataframe = {
    select(List(columnName))
  }
  override def select(columns: Seq[GenericColumn]): GenericDataFrame = {
    require(!columns.isEmpty && columns.forall(_.isInstanceOf[ScalaColumn[_]]), "The 'select' operation requires at least one column, which must be of type ScalaColumn")
    select(columns.map(_.asInstanceOf[ScalaColumn[_]].name).toList)
  }

  override def groupBy(columns: Seq[GenericColumn]): GenericGroupedDataFrame = ???

  override def agg(columns: Seq[GenericColumn]): GenericDataFrame = throw new NotImplementedError("Aggregations using the agg-expression are not supported at the moment")

  override def unionByName(other: GenericDataFrame): GenericDataFrame = other match {
    case otherScala: ScalaDataframe => {
      checkColumnsExist(otherScala, columns)
      val zipped = cols.sortBy(_.name) zip otherScala.cols.sortBy(_.name)
      ScalaDataframe(zipped.map(pair => pair._1 append pair._2))
    }
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }
  override def except(other: GenericDataFrame): GenericDataFrame = other match {
    case otherScala: ScalaDataframe => {
      require(dim == otherScala.dim, "The except operation can only be carried out with two dataframes of the same dimension")
      val duplRows = rows.toSet intersect otherScala.rows.toSet
      ScalaDataframe.fromScalaRows(rows.filterNot(duplRows.contains(_)))
    }
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(other)
  }

  override def filter(expression: GenericColumn): GenericDataFrame = {
    val newDf = withColumn("filterExpr", expression).asInstanceOf[ScalaDataframe]
    val filteredRows = newDf.rows.filter(_.value.last == true).map(r => r.copy(value = r.value.init)) //requires withColumn() to write the new column at the last index
    ScalaDataframe.fromScalaRows(filteredRows, Some(schema))
  }

  override def collect: Seq[GenericRow] = rows

  override def distinct: GenericDataFrame = ScalaDataframe.fromScalaRows(rows = rows.distinct, _schema = Some(schema))

  //In order for "filter" to work, the new column must be written at the last index
  def withColumnScala[A <: Any](colName: String, expression: ScalaColumn[A]): ScalaDataframe = {
    val newCol: ScalaColumn[A] = expression.copy(metadata = expression.metadata.copy(name = colName))
    ScalaDataframe(cols = this.cols :+ newCol.asInstanceOf[ScalaColumn[Any]])
  }
  override def withColumn(colName: String, expression: GenericColumn): GenericDataFrame = expression match {
    case sc: ScalaColumn[_] => sc.getAType match {
      case "String" => withColumnScala(colName, this.asInstanceOf[ScalaColumn[String]])
      case "Boolean" => withColumnScala(colName, this.asInstanceOf[ScalaColumn[Boolean]])
      case "Int" => withColumnScala(colName, this.asInstanceOf[ScalaColumn[Int]])
      case "Double" => withColumnScala(colName, this.asInstanceOf[ScalaColumn[Double]])
      case _ => throw new IllegalStateException("Unknown datatype A in column. Cannot get Ordering[A]")
    }
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(expression)
  }

  override def withColumnRenamed(colName: String, newName: String): GenericDataFrame = {
    val newCols = cols.map(c => if (c.name == colName) c.copy(metadata = c.metadata.copy(name = newName)) else c)
    ScalaDataframe(newCols)
  }

  override def drop(colName: String): GenericDataFrame = ScalaDataframe(cols.filterNot(_.name == colName))

  override def drop(col: GenericColumn): GenericDataFrame = col match {
    case sc: ScalaColumn[_] => drop(sc.name)
    case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(col)
  }

  override def createOrReplaceTempView(viewName: String): Unit = logger.warn("Temporal views are not available in ScalaDataFrames")
  override def dropDuplicates(cols: Seq[String]): GenericDataFrame = {
    val colsIndices: Seq[Int] = cols.map(c => this.cols.indexWhere(_.name == c))
    val uniqueRows = rows.foldLeft(Set[Any](), Queue[ScalaRow]())((setqueue, row) => {
      val (set, queue) = setqueue
      val duplValues = for (i <- colsIndices) yield row.value(i)
      if (set.contains(duplValues)) setqueue else (set + duplValues, queue :+ row)
    })._2.toSeq
    ScalaDataframe.fromScalaRows(uniqueRows, Some(schema))
  }
  override def isEmpty: Boolean = cols.isEmpty || cols.head.inner.isEmpty
  override def count: Long = nrRows
  override def cache: GenericDataFrame = this
  override def uncache: GenericDataFrame = this
  override def as(alias: String): GenericDataFrame = {
    logger.warn("The 'as' operation is not avaiable in ScalaDataFrames and will just return the same df")
    this
  }
  override def showString(options: Map[String, String]): String = {
    logger.warn("showString for ScalaDataframe will ignore the provided options")
    toString
  }
  override def explainString(options: Map[String, String]): String = {
    logger.warn("explain for ScalaDataframe will ignore the provided options and just return the dataframe as String")
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

  /**
   * Observe metrics on this DataFrame.
   * Note that this doesn't create a listener. These metrics will only be collected together using setupObservation.
   *
   * @param name             name of the observation
   * @param aggregateColumns aggregate columns to observe on the DataFrame
   * @return the modified DataFrame
   */
  override def observe(name: String, aggregateColumns: Seq[GenericColumn], isExecPhase: Boolean): GenericDataFrame = ???

  /**
   * Create an empty SubFeed for this subFeedType.
   */
  override def getDataFrameSubFeed(dataObjectId: SdlConfigObject.DataObjectId, partitionValues: Seq[PartitionValues], filter: Option[String]): DataFrameSubFeed = ???

  override def subFeedType: universe.Type = ???
}


object ScalaDataframe {
  def apply(rows: Seq[Seq[Any]], schema: Option[ScalaSchema]): ScalaDataframe = {
    ScalaDataframe.fromScalaRows(rows.map(ScalaRow(_)), schema)
  }

  def fromScalaRows(rows: Seq[ScalaRow], _schema: Option[ScalaSchema] = None): ScalaDataframe = {

    def schema : ScalaSchema = _schema.getOrElse(inferSchema)

    def inferSchema: ScalaSchema = if (rows.isEmpty) ScalaSchema(Seq()) else inferOneRow(rows(0))
    @tailrec
    def inferOneRow(_row: ScalaRow, col: Int = 0, agg: Seq[(String, ScalaDataTypeEnum)] = Seq()): ScalaSchema = {
      val row = _row.value
      if (row.isEmpty) ScalaSchema.inferredFromFields(agg)
      else {
        val scalaDataTypeEnum = ScalaDataType.fromValue(row.head).inner
        inferOneRow(ScalaRow(row.tail), col+1, agg :+ (s"col$col", scalaDataTypeEnum))
      }
    }
    //create columns from schema/values pairs
    val colsTry: Try[Seq[ScalaColumn[Any]]] = Try(rows.map(row => row.value).transpose.zip(schema.fields).map(v => ScalaColumn(v._2, v._1)))

    colsTry match {
      case Success(columns) => new ScalaDataframe(columns)
      case Failure(e) if e.getMessage.startsWith("transpose requires all collections to have the same size") => { //error with transpose operation
        throw new IllegalArgumentException("Could not create dataframe, rows must have the same size")
      }
      case Failure(e) => throw e
    }
  }
}
