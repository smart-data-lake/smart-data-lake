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

package io.smartdatalake.util.spark

import io.smartdatalake.workflow.dataframe.spark.{SparkField, SparkSchema}
import io.smartdatalake.util.misc.{SchemaUtil, SmartDataLakeLogger}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import java.text.Normalizer
import scala.jdk.CollectionConverters._

/**
 * Provides utility functions for [[DataFrame]]s.
 */
private[smartdatalake] object DataFrameUtil {

  implicit class DfSDL(df: DataFrame) extends SmartDataLakeLogger {


    /**
     * Casts type of given column to new [[DataType]].
     *
     * @param colName    Name of column to cast
     * @param newColType Type to cast to
     * @return cast [[DataFrame]]
     */
    def castDfColumnTyp(colName: String, newColType: DataType): DataFrame = if (newColType == df.schema(colName).dataType) {
      logger.debug(s"castDfColumnTyp: column is already of desired type. Nothing to do :)")
      logger.debug(s"castDfColumnTyp: colName=$colName newColType=$newColType")
      df
    } else df.withColumn(colName, df(colName).cast(newColType))

    /**
     * Casts column of [[DecimalType]] to an [[IntegralType]] or [[FloatType]].
     *
     * @param colName Name of column to cast
     * @return cast [[DataFrame]]
     */
    def castDecimalColumn2IntegralFloat(colName: String): DataFrame = {
      val dataType: DataType = df.schema(colName).dataType

      val newType: DataType = dataType match {
        case decimalType: DecimalType =>
          val preci = decimalType.precision
          if (0 == decimalType.scale) {
            if (preci < 3) ByteType
            else if (preci < 5) ShortType
            else if (preci < 11) IntegerType
            else LongType
          } else if (preci < 8) FloatType else DoubleType
        case _ => dataType
      }

      df.castDfColumnTyp(colName, newType)
    }

    /**
     * Casts type of all given columns to new [[DataType]].
     *
     * @param colNames   Array of names of columns to cast
     * @param newColType Type to cast to
     * @return cast [[DataFrame]]
     */
    def castDfColumnTyp(colNames: Seq[String], newColType: DataType): DataFrame = colNames.foldLeft(df)({ (df, s) => df.castDfColumnTyp(s, newColType) })

    /**
     * Casts type of all columns of given [[DataType]] to new [[DataType]].
     *
     * @param currentColType Current type filter of columns to be casted
     * @param newColType     Type to cast to
     * @return cast [[DataFrame]]
     */
    def castDfColumnTyp(currentColType: DataType, newColType: DataType): DataFrame = {
      logger.debug(s"castDfColumnTyp: currentColType=$currentColType   newColType=$newColType")
      logger.debug(s"castDfColumnTyp: df.columns=${df.columns.mkString(",")}")
      val colNames = df.schema.filter(currentColType == _.dataType).map(_.name)
      df.castDfColumnTyp(colNames, newColType: DataType)
    }

    /**
     * Casts type of all [[DataType]] columns to [[TimestampType]].
     *
     * @return casted [[DataFrame]]
     */
    def castAllDate2Timestamp: DataFrame = castDfColumnTyp(DateType, TimestampType)

    /**
     * Casts type of all columns to [[StringType]].
     *
     * @return casted [[DataFrame]]
     */
    def castAll2String: DataFrame = castDfColumnTyp(df.columns, StringType)

    /**
     * Casts type of all columns of [[DecimalType]] to an [[IntegralType]] or [[FloatType]].
     *
     * @return casted [[DataFrame]]
     */
    def castAllDecimal2IntegralFloat: DataFrame = df.columns.foldLeft(df)({ (df, s) => df.castDecimalColumn2IntegralFloat(s) })

    /**
     * Checks whether the specified columns contain nulls
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return true or false
     */
    def containsNull(cols: Array[String] = df.columns): Boolean = !getNulls().isEmpty

    /**
     * counts n-lets of this data frame with respect to specified columns cols.
     * The result data frame possesses the columns cols and an additional count column countColname.
     *
     * @param cols         : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @param countColname : name of count column, default name: cnt
     * @return subdataframe of n-lets
     */
    def getNonuniqueStats(cols: Array[String] = df.columns, countColname: String = "_cnt_"): DataFrame = {
      val forbiddenColumnNames = Array("count", countColname)
      // for better usability we define empty Array of cols to mean all columns of df
      val colsInDf: Array[String] = if (cols.isEmpty) df.columns else df.columns.intersect(cols)
      if (colsInDf.isEmpty) throw new IllegalArgumentException(s"Argument cols must contain at least 1 name of a column of data frame df.\n   df.columns = ${df.columns.mkString(",")}\n   cols = ${cols.mkString(",")} ")
      val projectedDf = df.select(colsInDf.head, colsInDf.tail: _*)
      val dfColumns: Array[String] = projectedDf.columns
      // If df contains forbidden column then the result contains two columns with the same name
      forbiddenColumnNames.foreach(str =>
        if (dfColumns.contains(str)) throw new IllegalArgumentException(s"data frame df must not contain column named $str. df.columns = ${dfColumns.mkString(",")}")
      )

      projectedDf.groupBy(dfColumns.head, dfColumns.tail: _*)
        .count().withColumnRenamed("count", countColname)
        .where(col(countColname) > 1)
    }

    /**
     * Returns rows of this data frame which violate uniqueness for specified columns cols.
     * The result data frame possesses an additional count column countColname.
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return subdataframe of n-lets
     */
    def getNonuniqueRows(cols: Array[String] = df.columns): DataFrame = {
      val dfNonUnique = getNonuniqueStats(cols, "_duplicationCount_").drop("_duplicationCount_")
      df.join(dfNonUnique, cols).select(df.columns.head, df.columns.tail: _*)
    }

    /**
     * returns sub data frame which consists of those rows which contain at least a null in the specified columns
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return sub data frame
     */
    def getNulls(cols: Array[String] = df.columns): DataFrame = {
      val nullSearch: Column = cols.map(col).foldLeft(lit(false))({ case (x, y) => x.or(y.isNull) })
      df.where(nullSearch)
    }

    /**
     * returns sub data frame which consists of those rows which violate PK condition for specfied columns
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return sub data frame
     */
    def getPKviolators(cols: Array[String] = df.columns): DataFrame = getNulls(cols).union(getNonuniqueRows(cols))

    /**
     * Checks whether the specified columns form a candidate key for the data frame
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return true or false
     */
    def isCandidateKey(cols: Array[String] = df.columns): Boolean = !containsNull(cols) && isMinimalUnique(cols)

    /**
     * checks whether schema is subschema of given [[StructType]].
     *
     * @param scm to test
     * @return result wether provided schema set is a subset of df.schema
     */
    def isSubSchema(scm: StructType): Boolean = scm.toSet.subsetOf(df.schema.toSet)

    /**
     * checks whether schema is superschema of given [[StructType]].
     *
     * @param scm to test
     * @return result wether provided schema set is a subset of df.schema
     */
    def isSuperSchema(scm: StructType): Boolean = df.schema.toSet.subsetOf(scm.toSet)

    /**
     * Checks whether the specified columns satisfy uniqueness within the data frame
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return true or false
     */
    def isUnique(cols: Array[String] = df.columns): Boolean = project(cols).getNonuniqueStats(cols).isEmpty

    /**
     * Checks whether the specified columns is a local minimal array of columns satisfying uniqueness within the data frame
     *
     * @param cols : names of columns which are to be considered, unspecified or empty Array mean all columns of df
     * @return true or false
     */
    def isMinimalUnique(cols: Array[String] = df.columns): Boolean = {
      def subFrameNotUnique(colName: String): Boolean = !df.isUnique(cols.filter(colName != _))

      df.isUnique(cols) && cols.forall(subFrameNotUnique)
    }

    /**
     * compares df with df2
     *
     * @param df2 : data frame to comapre with
     * @return true if both data frames have the same cardinality, schema and an empty symmetric difference
     */
    def isEqual(df2: DataFrame): Boolean = {
      // As a set-theoretic function symmetricDifference ignores multiple occurences of the same row.
      // Thus we need also to compare the cardinalities and the schemata of the two data frames.
      // For the schema, the order of columns doesn't need to match.
      // Note that we ignore the nullability of the columns to compare schemata.
      isSchemaEqualIgnoreNullabilty(df2) && symmetricDifference(df2).isEmpty && df.count() == df2.count()
    }

    def isSchemaEqualIgnoreNullabilty(df2: DataFrame): Boolean = {
      SchemaUtil.schemaDiff(SparkSchema(df.schema), SparkSchema(df2.schema), ignoreNullable = true).isEmpty && SchemaUtil.schemaDiff(SparkSchema(df2.schema), SparkSchema(df.schema), ignoreNullable = true).isEmpty
    }

    /**
     * projects a data frame onto array of columns
     *
     * @param cols : names of columns on which the data frame is to be projected
     * @return projection of data frame df
     */
    def project(cols: Array[String] = df.columns): DataFrame = df.select(cols.map(col): _*)

    /**
     * symmetric difference of two data frames: (df∪df2)∖(df∩df2) = (df∖df2)∪(df2∖df)
     *
     * @param df2         : data frame to compare with
     * @param diffColName : name of boolean column which indicates whether the row belongs to df
     * @return data frame
     */
    def symmetricDifference(df2: DataFrame, diffColName: String = "_in_first_df"): DataFrame = {
      require(df.columns.map(_.toLowerCase).toSet == df2.columns.map(_.toLowerCase).toSet, "DataFrames must have the same columns for symmetricDifference calculation")
      // reorder columns according to the original df for calculating symmetricDifference
      val colOrder = df.columns.map(col)
      df.except(df2.select(colOrder: _*)).withColumn(diffColName, lit(true))
        .unionByName(df2.select(colOrder: _*).except(df).withColumn(diffColName, lit(false)))
    }

    /**
     * Computes the set difference between the columns of `otherSchema` and of the columns defined in this data frame's
     * schema: `Set(otherSchema)` \ `Set(this.schema)`.
     *
     * Note the order: this returns the set of columns contained in
     * `otherSchema` that are missing in this data frame's schema.
     *
     * @param schemaOther    the schema whose [[StructField]]s to subtract.
     * @param ignoreNullable if `true`, columns that only differ in their `nullable` property are considered equal.
     * @return the set of columns contained in `otherSchema` but not in `this.schema`.
     */
    def schemaDiffTo(schemaOther: StructType, ignoreNullable: Boolean = false, deep: Boolean = false): Set[StructField] = {
      SchemaUtil.schemaDiff(SparkSchema(schemaOther), SparkSchema(df.schema), ignoreNullable = ignoreNullable, deep = deep)
        .map(_.asInstanceOf[SparkField].inner)
    }

    /**
     * If colName is defined, creates an additional column with a given expression on a DataFrame
     */
    def withOptionalColumn(colName: Option[String], expr: Column): DataFrame = {
      if (colName.isDefined) df.withColumn(colName.get, expr)
      else df
    }

    /**
     * Execute df.show and return it as String instead of printing it directly
     */
    def showString(): String = DatasetHelper.showString(df)
  }

  /**
   * Persists a [[DataFrame]] with [[StorageLevel.MEMORY_AND_DISK_SER]].
   *
   * @param dataFrame [[DataFrame]] to persist
   * @return persisted [[DataFrame]]
   */
  def defaultPersistDf(dataFrame: DataFrame): DataFrame = {
    dataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  /**
   * Persists a  [[DataFrame]] with given storage level [[StorageLevel.MEMORY_AND_DISK_SER]] if persisting is allowed.
   *
   * @param df           [[DataFrame]] to persist
   * @param doPersist    Allowed to persist?
   * @param storageLevel [[StorageLevel]] to use
   * @return persisted [[DataFrame]]
   */
  def persistDfIfPossible(df: DataFrame, doPersist: Boolean,
                          storageLevel: Option[StorageLevel] = None): DataFrame = {
    if (doPersist) {
      if (storageLevel.isDefined) {
        df.persist(storageLevel.get)
      } else {
        DataFrameUtil.defaultPersistDf(df)
      }
    }
    else df
  }

  /**
   * Transforms a name in CamelCase to lowercase with underscores, i.e. TestString -> test_string
   *
   * @param x [[String]] to transform
   * @return transformed [[String]]
   */
  def strCamelCase2LowerCaseWithUnderscores(x: String): String = {
    val normalized = "([A-Z]+[^A-Z_]*)|[^A-Z_]+".r.findAllMatchIn(x).map(_.group(0).toLowerCase.filter(_ != '_'))
      .filter(_.nonEmpty).mkString("_")
    // preserve leading underscores
    x.takeWhile(_ == '_') + normalized
  }

  /**
   * Transforms name with dashs and underscores to LowerCamelCase.
   */
  def strToLowerCamelCase(x: String): String = {
    val parts = x.split("[_\\- ]")
    val camelCase = parts.map(_.capitalize).mkString
    // lowercase first letter
    camelCase.head.toLower +: camelCase.tail
  }

  /**
   * Transform a string with UTF8 chars (e.g. diacritics, umlauts) to ASCII chars (best effort)
   */
  def normalizeToAscii(x: String): String = {
    // replace umlauts
    val normalizedUmlauts = x
      .replace("Ä", "Ae")
      .replace("Ö", "Oe")
      .replace("Ü", "Ue")
      .replace("ä", "ae")
      .replace("ö", "oe")
      .replace("ü", "ue")
    // decompose diacritics (e.g. accents) into separate UTF characters
    val normalizedUtf = Normalizer.normalize(normalizedUmlauts, Normalizer.Form.NFD)
    // remove all non-ascii characters
    normalizedUtf.replaceAll("[^\\p{ASCII}]","")
  }

  /**
   * Remove all hyphen and blanks from a string with underscores
   */
  def replaceNonSqlWithUnderscores(x: String): String = {
    x.replaceAll("[^a-zA-Z0-9_]+", "_")
  }
  /**
   * Remove all chars from a string which dont belong to lowercase SQL standard naming characters
   */
  def removeNonStandardSQLNameChars(x: String): String = {
    x.toLowerCase.replaceAll("[^a-zA-Z0-9_]", "")
  }

  def getEmptyDataFrame(schema: StructType)(implicit session: SparkSession): DataFrame = {
    session.createDataFrame(Seq.empty[Row].asJava, schema)
  }

  /**
   * pimpMyLibrary pattern to add DataFrameReader utility functions
   */
  implicit class DataFrameReaderUtils(reader: DataFrameReader) {
    def optionalSchema(schema: Option[StructType]): DataFrameReader = {
      if (schema.isDefined) reader.schema(schema.get) else reader
    }

    def optionalOption(key: String, value: Option[String]): DataFrameReader = {
      if (value.isDefined) reader.option(key, value.get) else reader
    }
  }

  /**
   * pimpMyLibrary pattern to add DataFrameWriter utility functions
   */
  implicit class DataFrameWriterUtils[T](writer: DataFrameWriter[T]) {
    def optionalPartitionBy(partitions: Seq[String]): DataFrameWriter[T] = {
      if (partitions.nonEmpty) writer.partitionBy(partitions: _*) else writer
    }

    def optionalOption(key: String, value: Option[String]): DataFrameWriter[T] = {
      if (value.isDefined) writer.option(key, value.get) else writer
    }

    def conditionalOption(key: String, activated: Boolean, value: () => String): DataFrameWriter[T] = {
      if (activated) writer.option(key, value()) else writer
    }
  }
}
