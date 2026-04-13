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
package io.smartdatalake.util.spark.dataset

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

trait Transform extends Serializable {

  implicit class DsTransform[T](ds: Dataset[T]) {

    val asDf: DataFrame = ds.select(ds.columns.map(col): _*)


    /**
     * If colName is defined, creates an additional column with a given expression on a DataFrame
     */
    def withOptionalColumn(colName: Option[String], expr: Column): DataFrame = {
      if (colName.isDefined) ds.withColumn(colName.get, expr)
      else ds.asDf
    }

    /** * transformCols: generic workers used by many other methods ** */

    /**
     * transforms columns given by the tranforming function
     * but transforms Dataset to DataFrame
     *
     * @param transformRenameFun : function to transform and rename columns
     * @param colFilter          : predicate on column names to filter
     * @param keepOriginalCols   : whether to keep original cols (make sure that there is no ambiguity!)
     * @return DataFrame with transformed and renamed columns
     */
    def transformCols(transformRenameFun: String => Iterable[(Column, String)],
                      colFilter: String => Boolean,
                      keepOriginalCols: Boolean): DataFrame = {
      val resultCols = ds.columns.flatMap { cn =>
        if (colFilter(cn)) {
          val newCols = transformRenameFun(cn).map { case (c, cn) => c.as(cn) }.toList
          if (keepOriginalCols) col(cn) +: newCols else newCols

        } else List(col(cn))
      }
      ds.select(resultCols: _*)
    }

    /**
     * transforms columns given by the tranforming function
     * but transforms Dataset to DataFrame
     *
     * @param renameFun        : function to rename columns
     * @param transformFun     : function to transform columns
     * @param colFilter        : predicate on column names to filter
     * @param keepOriginalCols : whether to keep original cols (make sure that there is no ambiguity!)
     * @return DataFrame with renamed columns
     */
    def transformCols(renameFun: String => Iterable[String] = List(_),
                      transformFun: String => Iterable[Column] = cn => Seq(col(cn)),
                      colFilter: String => Boolean = _ => true,
                      keepOriginalCols: Boolean = false): DataFrame = {
      val transformRename: String => Iterable[(Column, String)] = cn => transformFun(cn).zip(renameFun(cn))
      transformCols(transformRenameFun = transformRename,
        colFilter = colFilter, keepOriginalCols = keepOriginalCols)
    }

    /**
     * transforms columns given by the tranforming function
     * but transforms Dataset to DataFrame
     *
     * @param renameFun        : function to rename columns
     * @param transformFun     : function to transform columns
     * @param datType          : primitive Datatype. Does not work with parametrised DataTypes
     * @param keepOriginalCols : whether to keep original cols (make sure that there is no ambiguity!)
     * @return DataFrame with renamed columns
     */
    def transformCols(renameFun: String => Iterable[String],
                      transformFun: String => Iterable[Column],
                      datType: DataType,
                      keepOriginalCols: Boolean): DataFrame = {
      val transformRename: String => Iterable[(Column, String)] = cn => transformFun(cn).zip(renameFun(cn))
      transformCols(transformRenameFun = transformRename,
        colFilter = ds.schema(_).dataType == datType,
        keepOriginalCols = keepOriginalCols)
    }

    /**
     * This method assumes that all Timestamps are given in utc
     *
     * @param timeZone      : desired time zone
     * @param excludedTimes : not to be converted, typically lowerHorizon and upperHorizon
     * @return dataFrame with converted time stamps
     */
    def fromUtc(timeZone: String = "Europe/Zurich",
                excludedTimes: Seq[Timestamp] = List(Timestamp.valueOf("0001-01-01 0:0:0"), Timestamp.valueOf("9999-12-31 0:0:0")))
    : DataFrame = {
      transformCols(renameFun = List(_), transformFun = cn =>
        List(when(col(cn).isin(excludedTimes: _*), col(cn)).otherwise(from_utc_timestamp(col(cn), timeZone))),
        datType = TimestampType, keepOriginalCols = false)
    }

    /**
     *
     * @param timeZone      : time zone of Timestamp columns
     * @param excludedTimes : not to be converted, typically lowerHorizon and upperHorizon
     * @return dataFrame with UTC time stamps
     */
    def toUtc(timeZone: String = "Europe/Zurich",
              excludedTimes: Seq[Timestamp] = List(Timestamp.valueOf("0001-01-01 0:0:0"), Timestamp.valueOf("9999-12-31 0:0:0")))
    : DataFrame = {
      transformCols(renameFun = List(_), transformFun = cn =>
        List(when(col(cn).isin(excludedTimes: _*), col(cn)).otherwise(to_utc_timestamp(col(cn), timeZone))),
        datType = TimestampType, keepOriginalCols = false)
    }

    /**
     * Converts the data type of columns to a specified type.
     *
     * Notes:
     * - This function takes a column name and a target data type and updates the column's type accordingly.
     * - The returned data frame reflects the updated column types.
     *
     * @param newType     : The desired data type to convert to
     * @param currentType : columns of this type will be casted to newType
     * @return A data frame with the column type updated
     *
     */
    def castColumnsOfTypeTo(newType: DataType)(currentType: DataType): DataFrame = transformCols(
      transformFun = cn => List(col(cn).cast(newType)),
      colFilter = ds.schema.apply(_).dataType == currentType)

    /**
     * Converts the data type of columns to a specified type.
     *
     * Notes:
     * - This function takes a column name and a target data type and updates the column's type accordingly.
     * - The returned data frame reflects the updated column types.
     *
     * @param newType  : The desired data type to convert to
     * @param colNames : List of column names whose type is to be converted
     * @return A data frame with the column type updated
     *
     */
    def castColumnsTo(newType: DataType)(colNames: Seq[String]): DataFrame = {
      transformCols(transformFun = cn => List(col(cn).cast(newType)), colFilter = colNames.contains)
    }

    /**
     * Converts the data type of a column to a specified type.
     *
     * Notes:
     * - This function takes a column name and a target data type and updates the column's type accordingly.
     * - The returned data frame reflects the updated column types.
     *
     * @param newType : The desired data type to convert to
     * @param colName : The name of the column whose type is to be converted
     * @return A data frame with the column type updated
     *
     */
    def castColumnTo(newType: DataType)(colName: String): DataFrame = castColumnsTo(newType)(List(colName))

    /**
     * Casts type of all columns to [[StringType]].
     *
     * @return casted [[DataFrame]]
     */
    def castAll2String: DataFrame = castColumnsTo(newType = StringType)(colNames = ds.columns)

    /**
     * Casts type of all [[DataType]] columns to [[TimestampType]].
     *
     * @return casted [[DataFrame]]
     */
    def castAllDate2Timestamp: DataFrame = castColumnsOfTypeTo(newType = TimestampType)(currentType = DateType)


    /**
     *
     * @param typOpt desired data type if any (should be a large enough integral type)
     * @param strict Shall the upper bounds for precision of Decicmal be a strict bound?
     *               If strict then there an overflow cannot occur
     *               if not(strict) then the precision is the number of digits of the integral type
     *               but the Decimal value may be outside of the integral range.
     *               Set strict to false on your own risk
     * @return data frame of which all unscaled Decimal columns are converted to Integral type
     */
    def castDecimalsToIntegralType(typOpt: Option[DataType] = None,
                                   strict: Boolean = true): DataFrame = {
      val oldType: String => DataType = cn => ds.schema(cn).dataType
      val oldTypePrecisionScale: String => Option[(Int, Int)] = cn => getDecimalPrecisionScale(oldType(cn))
      val cFilter: String => Boolean = oldTypePrecisionScale(_).map(_._2).contains(0)

      def isPrecisionSmallerThan(upperBound: Int)(p: Int) = p < upperBound || (!strict & p == upperBound)

      val trfFun: String => Iterable[Column] = colName => {
        val newType = if (typOpt.isDefined) typOpt.get else oldTypePrecisionScale(colName).map(_._1).get match {
          case n if n <= 0 => oldType(colName)
          case n if isPrecisionSmallerThan(3)(n) => ByteType
          case n if isPrecisionSmallerThan(5)(n) => ShortType
          case n if isPrecisionSmallerThan(10)(n) => IntegerType
          case n if isPrecisionSmallerThan(19)(n) => LongType
          case _ => oldType(colName)
        }
        Seq(col(colName).cast(newType))
      }
      transformCols(transformFun = trfFun, colFilter = cFilter)
    }

    def castDecimalsToFloatDouble(castIntegral: Boolean = false): DataFrame = {
      val oldType: String => DataType = cn => ds.schema(cn).dataType
      val oldTypePrecisionScale: String => Option[(Int, Int)] = cn => getDecimalPrecisionScale(oldType(cn))
      val cFilter: String => Boolean = oldTypePrecisionScale(_).map(castIntegral || _._2 > 0).exists(identity)
      val trfFun: String => Iterable[Column] = colName => {
        val newType = if (oldTypePrecisionScale(colName).map(_._1 < 8).get) FloatType else DoubleType
        Seq(col(colName).cast(newType))
      }
      transformCols(transformFun = trfFun, colFilter = cFilter)
    }

    /**
     * Casts type of all columns of [[DecimalType]] to an [[IntegralType]] or [[FloatType]].
     *
     * @return casted [[DataFrame]]
     */
    def castAllDecimal2IntegralFloat: DataFrame = castDecimalsToIntegralType(strict = false).castDecimalsToFloatDouble()

    /**
     * @return DataFrame with columns of MapType casted to ArrayType
     */
    def castMapsToArrays: DataFrame = {
      val mapCols: String => Boolean = ds.schema(_).dataType match {
        case MapType(_, _, _) => true
        case _ => false
      }
      transformCols(transformFun = cn => List(map_entries(col(cn))), colFilter = mapCols)
    }

    def decomposeArrayColumn[S](arrayCol: Column, indexMap: Map[S, String]): DataFrame = ds
      .withColumns(indexMap.map { case (i, s) => (s, arrayCol(i)) })

    /**
     * Enumerates groups in a dataframe. Groups are final defined based on `keyCols` and `condition`
     * (e.g. the previous value of an attribute) after ordering by `orderCols`.
     * The previous attr has the postfix "_prev", e.g. condition = $"id_prev" === $"id"
     *
     * Notes about nulls:
     *  - nulls in attr must be handled in condition, null is not equal to null!
     *  - nulls in keyCols final define a group, all nulls fall in this groups
     *  - nulls in orderCols are placed first (with ascending order). This can be changed with e.g.
     *    orderCols = Seq($"sample_nb".asc_nulls_last)
     *
     * @param attr        the attribute on which the enumeration depends
     * @param keyCols     the keys onf the dataframe
     * @param orderCols   the column for the ordering
     * @param condition   The condition for a group
     * @param groupNbName The name of the number col, final defaults to "nb"
     * @return
     */
    // TODO: Move enumerateGroups to a different trait as it does not transform the dataset
    def enumerateGroups(attr: String, keyCols: Seq[Column], orderCols: Seq[Column],
                        condition: Column, groupNbName: String = "nb")
                       (implicit ss: SparkSession): DataFrame = {
      import ss.implicits._

      ds.withColumn(attr + "_prev", lag(attr, 1).over(Window.partitionBy(keyCols: _*).orderBy(orderCols: _*)))
        .withColumn("consecutive", col(attr + "_prev").isNotNull and condition)
        .withColumn(groupNbName, sum(when($"consecutive", lit(0)).otherwise(lit(1))).over(Window.partitionBy(keyCols: _*).orderBy(orderCols: _*)))
        .drop(attr + "_prev", "consecutive")
    }

    /**
     * @return DataFrame with columns of ArrayType or MapType exploded
     */
    //noinspection NoTailRecursionAnnotation
    def explodeArrays: DataFrame = {
      // Since one array column can be exploded at time
      // we use recursion here instead of sending all columsn to the transformer
      val firstArrayCol: Option[String] = ds.columns.find(ds.schema(_).dataType.isInstanceOf[ArrayType])
      firstArrayCol match {
        case None => ds.asInstanceOf[DataFrame]
        case Some(arrCol) => ds
          .transformCols(transformFun = cn => List(explode(col(cn))), colFilter = arrCol == _)
          .explodeArrays
      }
    }

    /**
     * @return DataFrame with columns of ArrayType or MapType exploded
     */
    //noinspection NoTailRecursionAnnotation
    def explodeMaps: DataFrame = {
      val firstMapCol: Option[String] = ds.columns.find(ds.schema(_).dataType.isInstanceOf[MapType])
      firstMapCol match {
        case None => ds.asInstanceOf[DataFrame]
        case Some(mapCol) => val i = ds.columns.indexOf(mapCol)
          ds.select(ds.columns.take(i).map(col) ++ (explode(col(mapCol)) +: ds.columns.drop(i + 1).map(col)): _*)
            .withColumnRenamed("key", s"${mapCol}_key")
            .withColumnRenamed("value", s"${mapCol}_value")
            .explodeMaps
      }
    }

    ///// Renaming Columns /////

    /**
     * renames columns given by the renaming function
     * but transforms Dataset to DataFrame
     *
     * @param renameFun        : function to rename columns
     * @param colFilter        : predicate on column names to filter
     * @param keepOriginalCols : whether to keep original cols (make sure that there is no ambiguity!)
     * @return DataFrame with renamed columns
     *
     */
    def renameCols(renameFun: String => String,
                   colFilter: String => Boolean = _ => true,
                   keepOriginalCols: Boolean = false): DataFrame = {
      val rename: String => Iterable[String] = cn => Seq(renameFun(cn))
      transformCols(renameFun = rename, colFilter = colFilter, keepOriginalCols = keepOriginalCols)
    }


    ///// Unfolding Structs /////

    /**
     * loest Struct-Column in die Elementfelder auf
     *
     * @param scheme  : schema which the column belongs to
     * @param nested  : bestimmt ob verschachtelte Structs rekursiv aufgeloest werden sollen
     * @param colName : column to estruct
     * @return Sequence of column expressions
     */
    private def unfoldStructCol(scheme: StructType, nested: Boolean = true)
                               (colName: String): List[String] = {
      val fld: StructField = scheme(colName)
      fld.dataType match {
        case StructType(_) =>
          val fldStruct: StructType = fld.dataType.asInstanceOf[StructType]
          fldStruct
            .flatMap { subFld =>
              if (nested) unfoldStructCol(fldStruct)(subFld.name) else List(subFld.name)
            }
            .map(str => s"$colName.$str")
            .toList
        case _ => List(colName)
      }
    }

    /**
     * loest alle Struct-Spalten in die Elementspalten auf
     *
     * @param colNameFilter     : which columns to unfoldStructs
     * @param nested            : bestimmt ob verschachtelte Structs rekursiv aufgeloest werden sollen
     * @param fullSubcolName    : bestimmt ob der volle Spaltenpfad als Name verwendet werden soll
     * @param fullSubcolNameSep : seperator for column names if fullSubcolName
     * @return df mit aufgeloester Spalte fld.name falls diese vom StructType ist, sonst df unverändert
     */
    def unfoldStructs(colNameFilter: String => Boolean = _ => true,
                      nested: Boolean = true,
                      fullSubcolName: Boolean = true,
                      fullSubcolNameSep: String = "·"): DataFrame = {
      def getSubColAlias(cn: String): String = if (fullSubcolName)
        cn.replace(".", fullSubcolNameSep) else cn.replaceAll(".*\\.", "")

      val transformRename: String => List[(Column, String)] = str =>
        unfoldStructCol(ds.schema, nested)(str)
          .map { x => (col(x), getSubColAlias(x)) }
      transformCols(transformRenameFun = transformRename,
        colFilter = colNameFilter, keepOriginalCols = false)
    }


    ///// unpivotCast, transpose et al /////

    /**
     * Transforms (reshapes) a wide dataframe to a long DataFrame by transforming columns into rows
     * This is the opposite of pivoting and corresponds to R and Pandas "melt"
     *
     * (Adapted from [[https://stackoverflow.com/a/37865645)]]
     *
     * Note: This could also be implemented with the SQL-expression "stack
     * See [[https://confluence.sbb.ch/display/STA/DataFrame+Reshaping]]
     *
     * Note that the datatype of all columns to be transposed to rows must be the same!
     * If at least 1 column is nullable, the new column will also be nullable
     *
     * @param idCols    The id-columns which should remain
     * @param keyName   The name of the new key-column
     * @param valueName The name of the new value-column
     * @return The transformed dataframe
     */
    def colsToRows(idCols: Seq[String], keyName: String = "key", valueName: String = "value"): DataFrame = {
      // cols: all columns to be transformed to rows
      val (cols, types, nullable) = ds.schema.filter(c => !idCols.contains(c.name))
        .map { case StructField(name, dataType, nullable, _) => (name, dataType, nullable) }.unzip3

      require(types.distinct.length == 1, s"All columns need to have same type, but found ${types.distinct.mkString("Array(", ", ", ")")}")

      // check whether at least 1 columns is nullable
      val hasNullable = nullable.exists(identity)

      // make an array of the values in the columns and then explode it to generate rows
      val kvs = explode(array(
        cols.map(c => struct(
          lit(c).alias(keyName),
          // the "when" makes a non-nullable column nullable
          (if (hasNullable) when(lit(true), col(c)) else col(c)
            ).alias(valueName))): _*
      ))

      val colsToKeep = idCols.map(col)

      // construct final dataframe
      ds.select(colsToKeep :+ kvs.alias("_kvs"): _*)
        .select(colsToKeep ++ Seq(col(s"_kvs.$keyName"), col(s"_kvs.$valueName")): _*)
    }

    /**
     * Unpivotiert mehrere Spalten des Datasets
     * Gibt es eine nicht-numerische Pivotspalte, dann werden diese alle zu StringType gecastet
     * Spalten, die weder in keys oder colNamesToPivot vorkommen werden geloescht.
     *
     * @param keys            : columns which are to be kept but not pivoted
     * @param namesColName    : name of column which contains names of pivoted columns
     * @param valuesColname   : name of column which contains values of pivoted columns
     * @param colNamesToPivot : names of column to unpivotCast
     * @return df mit pivotierter Spalte
     */
    def unpivotCast(keys: Array[Column],
                    namesColName: String = "x", valuesColname: String = "y",
                    colNamesToPivot: Array[String]): DataFrame = {
      val dfTypes: Set[DataType] = ds.select(colNamesToPivot.map(col): _*).schema.map(_.dataType).toSet
      val dfCasted = if (dfTypes.forall(_.isInstanceOf[NumericType])) ds
      else colNamesToPivot.foldLeft(ds.asInstanceOf[DataFrame]) { (dtf, cn) => dtf.castColumnTo(StringType)(cn) }
      dfCasted.unpivot(ids = keys, values = colNamesToPivot.map(col),
        variableColumnName = namesColName, valueColumnName = valuesColname)
    }

    /**
     * transponiert die ersten numRows Zeilen des Dataframes
     *
     * @param numRows : Anzahl Zeilen, die transponiert werden sollen. Datentyp Byte damit nicht zu viele angegeben werden.
     * @return df 1+numRows Zeilen und Anzahl Zeilen, die der Spaltenanzahl entspricht
     */
    def transpose(numRows: Byte = 2)(implicit ss: SparkSession): DataFrame = if (ds.isEmpty) {
      ss.createDataFrame(ds.columns.map(Row(_)).toList.asJava, StructType(List(StructField("_column", StringType, nullable = false))))
    } else {
      val rows: Array[Row] = ds.asInstanceOf[DataFrame].take(numRows)

      def transposeRow(rows: Array[Row])(r: Row): DataFrame = ss
        .createDataFrame(ArrayBuffer(r).asJava, ds.schema)
        .unpivotCast(Array(), "_column", f"_${rows.indexOf(r)}%03d", ds.columns)

      rows.map(transposeRow(rows))
        .reduce { (df1, df2) => df1.join(df2, Seq("_column")) }
    }


    /**
     * dataset to curry frame
     */
    def curryDataFrameOneColumn(keyCols: List[String], cn: String): DataFrame = {
      ds.groupBy(keyCols.map(col): _*)
        .agg(collect_set(struct(col(cn), col("values"))).as("values"))
    }

    private def applyFunToValues(f: Column => Column)(v: Column): Column = f(v)

    def dataSet2curryFrame(pkCols: List[String]): DataFrame = {
      require(pkCols.nonEmpty, "dataSet2curryMap: pkCols must contain at least one element!")
      // Building column expression for curry_map
      val curryMapColumn = pkCols.size match {
        case 1 => map_from_entries(col("values")).as("curry_map")
        case _ => transform_values(map_from_entries(col("values")),
          List.range(2, pkCols.size)
            .foldLeft[(Column, Column) => Column]((_, c) => applyFunToValues(f = map_from_entries)(c)) {
              (col, _) => (_, x) => applyFunToValues(f = c => transform_values(map_from_entries(c), col))(x)
            })
          .as("curry_map")
      }

      // Building column expression for values
      val valueColumns: Array[String] = ds.columns.diff(pkCols)
      require(valueColumns.nonEmpty,
        s"dataSet2curryMap: The Dataset ds mut contain at least one column not in pkCols=(${pkCols.mkString(",")})")
      val valuesColumn: Column = valueColumns.length match {
        case 1 => col(valueColumns.head).as("values")
        case _ => struct(valueColumns.map(col): _*).as("values")
      }
      val dfDs = ds.select(pkCols.map(col) :+ valuesColumn: _*)
      val pkColsInit: List[String] = pkCols.distinct.init

      pkColsInit.foldRight[DataFrame](
        dfDs.curryDataFrameOneColumn(keyCols = pkColsInit, cn = pkCols.last)
      ) {
        case (cn, df) => df.curryDataFrameOneColumn(keyCols = pkColsInit.takeWhile(cn != _), cn)
      }.select(curryMapColumn)
    }

    def breakLineageIfNotExecPhase(isExec: Boolean): DataFrame = {
      if (!isExec) getEmptyDataFrame(ds.schema)(ds.sparkSession)
      else ds.toDF()
    }

  }
}
