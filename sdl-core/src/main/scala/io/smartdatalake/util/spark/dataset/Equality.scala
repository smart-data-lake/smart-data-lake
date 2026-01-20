/*
 * Smart Data Lake - Build your data lake the smart way.
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

import io.smartdatalake.util.Constants.epsilonDouble
import io.smartdatalake.util.{LogUtils, PrecisionDef}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Documentation can be found in Confluence, see
 * https://confluence.sbb.ch/x/4BUXf
 */

trait Equality extends Transform {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
   *
   * Returns whether two schemata equal
   *
   * @param leftSchema        the left schema
   * @param rightSchema       the right schema
   * @param ignoreColumnOrder whether or not to ignore the order of columns
   * @param ignoreNullability whether or not to ignore nullability of fields
   * @param showDiff          whether or not to log a diff
   * @return whether or not the schemata are equal
   */
  final def schemataEqual(leftSchema: StructType, rightSchema: StructType,
                          ignoreColumnOrder: Boolean = true, ignoreNullability: Boolean = true,
                          showDiff: Boolean = true)(implicit logger: Logger): Boolean = {
    LogUtils.debugLog(s"schemataEqual: leftSchema  =  ${leftSchema.catalogString}")
    LogUtils.debugLog(s"schemataEqual: rightSchema = ${rightSchema.catalogString}")
    LogUtils.debugLog(s"schemataEqual: ignoreColumnOrder = $ignoreColumnOrder , ignoreNullability = $ignoreNullability , showDiff = $showDiff")

    def fieldOrder(f1: StructField, f2: StructField): Boolean = f1.name < f2.name

    def makeNullableIfIgnored(sf: StructField): StructField = StructField(sf.name, sf.dataType, ignoreNullability || sf.nullable, sf.metadata)

    val lSch = leftSchema.map(makeNullableIfIgnored)
    val rSch = rightSchema.map(makeNullableIfIgnored)
    val result = lSch == rSch || (ignoreColumnOrder && lSch.sortWith(fieldOrder) == rSch.sortWith(fieldOrder))

    if (!result && showDiff) {
      logger.info("schemataEqual: schemata differ !")
      logger.info(s"ignoreColumnOrder = $ignoreColumnOrder")
      logger.info(s"ignoreNullability = $ignoreNullability")
      logger.info(s"leftSchema  = ${leftSchema.mkString(", ")}")
      leftSchema.printTreeString
      logger.info(s"rightSchema = ${rightSchema.mkString(", ")}")
      rightSchema.printTreeString
      logger.info(s"leftSchema minus rightSchema = ${leftSchema.diff(rightSchema).mkString(", ")}")
      logger.info(s"rightSchema minus leftSchema = ${rightSchema.diff(leftSchema).mkString(", ")}")
    }
    result
  }

  implicit class DsEqual[T](ds: Dataset[T]) {

    private def isImpreciseType(t: DataType): Boolean = t match {
      case DecimalType() => t.asInstanceOf[DecimalType].scale != 0
      case DoubleType => true
      case FloatType => true
      case _ => false
    }

    /**
     *
     * Returns set of common column names which shall be compared imprecisely
     *
     * @param that the right dataset
     * @return set of colum names
     */
    protected final def getCommonImpreciseCols(that: Dataset[T]): Set[String] = ds.columns.toSet
      .intersect(that.columns.toSet).filter(cn => isImpreciseType(ds.schema(cn).dataType))

    /**
     *
     * Returns symmetric difference both dataFrames
     * The functions requires that they have the same columns
     * It uses set comparison, i.e. symmetric difference
     *
     * @param that the right dataframe
     * @return whether or not the dataframes are equal
     */
    final def getSymmetricDifference(that: Dataset[T])(implicit logger: Logger): DataFrame = {
      // Spark 3 needs alias for joins from same DataFrame with same column names
      val thiss = ds.explodeMaps.explodeArrays.unfoldStructs()
      val thissColLogString = s"thiss.columns  = Array(${thiss.columns.mkString(",")})"
      val thatt = that.explodeMaps.explodeArrays.unfoldStructs()
      require(thiss.columns sameElements thatt.columns,
        s"""getSymmetricDifference: DFs must have the same columns!
           | $thissColLogString
           | thatt.columns = Array(${thatt.columns.mkString(",")})""".stripMargin)
      LogUtils.debugLog(s"getSymmetricDifference: $thissColLogString")
      lazy val thissMinusThatt = thiss.except(thatt).withColumn("_this", lit(true))
      lazy val thattMinusThiss = thatt.except(thiss).withColumn("_this", lit(false))
      thissMinusThatt.union(thattMinusThiss)
    }

    /**
     *
     * Returns rows which are not contained in both dataframes
     * using almost equal comparison
     * The functions requires that they have the same columns
     *
     * @param that         the right dataframe
     * @param precisionMap defines for which columns a non-exact numeric comparison suffices
     *                     column name ↦ (precision, strict, relative threshold)
     *                     When relative threshold = None, then absolut comparison
     *                     When relative threshold = Some(x),
     *                     then relative comparison if absolute value of both column values are larger than x,
     *                     absolut comparison otherwise
     * @return dataFrame ccontaining the row differences
     */
    protected final def getRowDifferences(that: Dataset[T],
                                          precisionMap: Map[String, PrecisionDef])
                                         (implicit logger: Logger): DataFrame = {
      val thiss = ds.explodeMaps.explodeArrays.unfoldStructs().as("l")
      val thissColLogString = s"thiss.columns  = Array(${thiss.columns.mkString(",")})"
      val thatt = that.explodeMaps.explodeArrays.unfoldStructs().as("r")
      require(thiss.columns sameElements thatt.columns,
        s"""getRowDifferences: DFs must have the same columns!
           | $thissColLogString
           | thatt.columns = Array(${thatt.columns.mkString(",")})""".stripMargin)
      LogUtils.debugLog(s"getRowDifferences: $thissColLogString")

      val preciseCols = thiss.columns.toSet.diff(precisionMap.keySet)
      // thiss and thatt must have equal values in precise columns
      val preciseEqualCondition: Column = preciseCols.foldLeft(lit(true)) { (c, cn) =>
        c and equal_null(col(s"l.$cn"), col(s"r.$cn"))
      }
      LogUtils.debugLog(s"getRowDifferences: preciseEqualCondition = $preciseEqualCondition")

      val impreciseCols = thiss.columns.toSet.intersect(precisionMap.keySet)
      LogUtils.debugLog(s"getRowDifferences: impreciseCols = ${impreciseCols.mkString(",")}")
      // Only numeric columns can be compared imprecisely!
      impreciseCols.foreach { cn =>
        val dType = thiss.schema(cn).dataType
        require(dType.isInstanceOf[NumericType], s"Columns for imprecise comparison must have a numeric type. But column $cn is of type $dType")
      }

      def getImpreciseComparison(cn: String): Column = {
        val precisionDef = precisionMap(cn)
        val epsilonCol = lit(precisionDef.precision)

        def comp(upperBound: Column): Column = {
          val uBound = upperBound.cast(thiss.schema(cn).dataType) // to ensure that comparison is done in type of column not in double
          if (precisionDef.strict) abs(col(s"l.$cn") - col(s"r.$cn")) < uBound else abs(col(s"l.$cn") - col(s"r.$cn")) <= uBound
        }

        val impComp: Column = if (precisionDef.relThreshold.isEmpty) comp(epsilonCol)
        else comp(epsilonCol * greatest(lit(precisionDef.relThreshold.get), least(abs(col(s"l.$cn")), abs(col(s"r.$cn")))))

        // For Floats and Doubles which are not numbers, like NaN, we add equality
        val nanComparison = col(s"l.$cn") === col(s"r.$cn") and epsilonCol >= 0 and
          (lit(!precisionDef.strict) or epsilonCol > 0)

        nanComparison or impComp
      }

      val impreciseEqualCondition: Column = impreciseCols.foldLeft(lit(true)) { (c, cn) =>
        // to be consistent with symmetric difference we consider null equals null
        c and ((col(s"l.$cn").isNull and col(s"r.$cn").isNull) or getImpreciseComparison(cn))
      }
      LogUtils.debugLog(s"getRowDifferences: impreciseEqualCondition = $impreciseEqualCondition")
      val equalCondition = preciseEqualCondition and impreciseEqualCondition

      lazy val leftMinusRight = thiss.join(thatt, equalCondition, "left_anti")
        .withColumn("_df", lit("thiss"))
      lazy val rightMinusLeft = thatt.join(thiss, equalCondition, "left_anti")
        .withColumn("_df", lit("thatt"))
      leftMinusRight.union(rightMinusLeft)
    }


    /**
     *
     * Returns whether two dataframes contain the same multiset of rows
     * The functions requires that they have the same columns
     *
     * @param that         the right dataframe
     * @param precisionMap defines for which columns a non-exact numeric comparison suffices
     *                     column name ↦ (precision, strict, relative threshold)
     *                     When relative threshold = None, then absolut comparison
     *                     When relative threshold = Some(x),
     *                     then relative comparison if absolute value of both column values are larger than x,
     *                     absolut comparison otherwise
     * @param showDiff     whether or not to log a diff
     * @param pk           List ofcolumn names consituting primary key
     *                     PK cols are compared twice: imprecisely and precisely!
     * @return whether or not the dataframes are equal
     */
    final def hasAlmostEqualRows(that: Dataset[T],
                                 precisionMap: Map[String, PrecisionDef],
                                 showDiff: Boolean,
                                 pk: Seq[String]
                                )(implicit logger: Logger): Boolean = {
      LogUtils.debugLog(s"hasAlmostEqualRows: precisionMap.size = ${precisionMap.size} , showDiff = $showDiff , pk = (${pk.mkString(",")})")
      LogUtils.debugLog(s"hasAlmostEqualRows: precisionMap = ${precisionMap.mkString(",")}")
      val thisCount = ds.count()
      val thatCount = that.count()
      val equinumerous: Boolean = thisCount == thatCount
      LogUtils.debugLog(s"hasAlmostEqualRows: equinumerous = $equinumerous")

      val thiss: DataFrame = ds.explodeMaps.explodeArrays.unfoldStructs()
      val thissColLogString = s"thiss.columns  = Array(${thiss.columns.mkString(",")})"
      val thatt: DataFrame = that.explodeMaps.explodeArrays.unfoldStructs()
      require(thiss.columns sameElements thatt.columns,
        s"""hasAlmostEqualRows: DFs must have the same columns!
           | $thissColLogString
           | thatt.columns = Array(${thatt.columns.mkString(",")})""".stripMargin)
      LogUtils.debugLog(s"hasAlmostEqualRows: $thissColLogString")

      val preciseCols: Set[String] = thiss.columns.toSet.diff(precisionMap.keySet)
      val impreciseCols: Set[String] = thiss.columns.toSet.intersect(precisionMap.keySet)

      val (symDiffCols: List[Array[Column]], rowDiffCols: List[Array[Column]]) = if (precisionMap.isEmpty)
        // if precisionMap.isEmpty then all columns shall be compared using symmetric difference
        (List(thiss.columns.map(col)), List.empty[Array[Column]])
      // if precisionMap contains elements but PK is empty
      // then all columns shall be compared using anti-join (row difference)
      else if (pk.isEmpty) (List.empty[Array[Column]], List(thiss.columns.map(col)))
      // if precisionMap contains elements and PK is given
      // then compare the precise columns using symmetric difference and the others using row difference
      else (List((pk ++ preciseCols).distinct.map(col).toArray),
        List((pk ++ impreciseCols).distinct.map(col).toArray))
      LogUtils.debugLog(s"hasAlmostEqualRows: symDiffCols = ${symDiffCols.headOption.map(_.mkString(","))}")
      LogUtils.debugLog(s"hasAlmostEqualRows: rowDiffCols = ${rowDiffCols.headOption.map(_.mkString(","))}")

      lazy val preciseRowDiff: List[DataFrame] = symDiffCols.map { x =>
        thiss.select(x: _*).getSymmetricDifference(thatt.select(x: _*))
      }
      lazy val impreciseRowDiff: List[DataFrame] = rowDiffCols.map { x =>
        thiss.select(x: _*).getRowDifferences(thatt.select(x: _*), precisionMap)
      }

      val result = equinumerous && (preciseRowDiff ++ impreciseRowDiff).forall(_.isEmpty)
      LogUtils.debugLog(s"hasAlmostEqualRows: result = $result")

      if (!result && showDiff) {
        logger.info("hasAlmostEqualRows: The multisets of rows differ between thiss and thatt !")
        logger.info(s"hasAlmostEqualRows: precisionMap = ${precisionMap.mkString("; ")}")
        logger.info(s"hasAlmostEqualRows: thisCount = $thisCount")
        logger.info(s"hasAlmostEqualRows: thatCount = $thatCount")
        logger.info("hasAlmostEqualRows: row differences:")
        (preciseRowDiff ++ impreciseRowDiff).foreach(df => {
          df.printSchema()
          df.show(false)
        })
      }
      result
    }

    /**
     *
     * Check that two DataFrames are equal. This includes
     *  - the schema is equal
     *  - the content is equal
     *
     * @param that              the right dataframe
     * @param ignoreColumnOrder whether or not to ignore the order of columns
     * @param ignoreNullability whether or not to ignore nullability of fields
     * @param precisionMap      defines for which columns a non-exact numeric comparison suffices and defines the preciseness
     * @param showDiff          whether or not to log a diff
     * @return whether or not the dataframes are equal
     */
    final def almostEqual(that: Dataset[T],
                          ignoreColumnOrder: Boolean, ignoreNullability: Boolean,
                          precisionMap: Map[String, PrecisionDef],
                          showDiff: Boolean,
                          pk: Seq[String])(implicit logger: Logger): Boolean = {
      schemataEqual(ds.schema, that.schema, ignoreColumnOrder, ignoreNullability, showDiff) &&
        ds.select(ds.columns.map(col): _*)
          .hasAlmostEqualRows(that.select(ds.columns.map(col): _*), precisionMap, showDiff, pk)
    }

    /**
     *
     * Check that two DataFrames are equal. This includes
     *  - the schema is equal
     *  - the content is equal
     *
     * @param that              the right dataframe
     * @param ignoreColumnOrder whether or not to ignore the order of columns
     * @param ignoreNullability whether or not to ignore nullability of fields
     * @param precision         defines preciseness of non-exact numeric comparison for all numeric columns
     * @param strict            configures whether comparison is strict (<) or not (≤)
     * @param relThreshold      defines threshold to switch between relative and absolute comparison
     *                          relative comparison if absolute value of both column values are larger than relThreshold,
     *                          absolut comparison otherwise
     *                          if relThreshold is None then absolute comparison for all values
     * @param showDiff          whether or not to log a diff
     * @return whether or not the dataframes are equal
     */
    final def almostEqual(that: Dataset[T],
                          ignoreColumnOrder: Boolean = true, ignoreNullability: Boolean = true,
                          precision: Double = epsilonDouble,
                          relThreshold: Option[Double] = Some(epsilonDouble),
                          strict: Boolean = true,
                          showDiff: Boolean = true,
                          pk: Seq[String] = Nil)(implicit logger: Logger): Boolean = {
      val thisUnfolded = ds.explodeMaps.explodeArrays.unfoldStructs()
      val thatUnfolded = that.explodeMaps.explodeArrays.unfoldStructs()
      LogUtils.debugLog(s"almostEqual: thisUnfolded.schema = ${thisUnfolded.schema.catalogString}")
      LogUtils.debugLog(s"almostEqual: thatUnfolded.schema = ${thatUnfolded.schema.catalogString}")
      val pm = thisUnfolded.getCommonImpreciseCols(thatUnfolded).map((_, PrecisionDef(precision, strict, relThreshold))).toMap
      LogUtils.debugLog(s"almostEqual: pm = ${pm.mkString(",")}")
      thisUnfolded.almostEqual(that = thatUnfolded,
        ignoreColumnOrder = ignoreColumnOrder, ignoreNullability = ignoreNullability,
        precisionMap = pm, showDiff = showDiff, pk)
    }

    /**
     *
     * Check that two DataFrames are equal. This includes
     *  - the schema is equal
     *  - the content is equal
     *
     * @param that              the right dataframe
     * @param ignoreColumnOrder whether or not to ignore the order of columns
     * @param ignoreNullability whether or not to ignore nullability of fields
     * @param showDiff          whether or not to log a diff
     * @return whether or not the dataframes are equal
     */
    final def equal(that: Dataset[T],
                    ignoreColumnOrder: Boolean = true, ignoreNullability: Boolean = true,
                    showDiff: Boolean = true,
                    pk: Seq[String] = Nil)(implicit logger: Logger): Boolean = ds
      .almostEqual(that = that,
        ignoreColumnOrder = ignoreColumnOrder, ignoreNullability = ignoreNullability,
        precisionMap = Map[String, PrecisionDef](), showDiff, pk)

  }
}
