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
package io.smartdatalake.util.historization

import java.time.LocalDateTime

import io.smartdatalake.definitions.{HiveConventions, TechnicalTableColumn}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}


/**
 * Unit tests for historization
 *
 */
class HistorizationTest extends FunSuite with BeforeAndAfter with SmartDataLakeLogger {

  private implicit val session: SparkSession = TestUtil.sessionHiveCatalog

  private object HistorizationPhase extends Enumeration {
    type HistorizationPhase = Value
    val Existing = Value
    val UpdatedNew = Value
    val UpdatedOld = Value
    val NewlyAdded = Value
    val TechnicallyDeleted = Value
  }

  private val ts1: java.time.LocalDateTime => Column = t => lit(t.toString).cast(TimestampType)
  private val doomsday =
    HiveConventions.getHistorizationSurrogateTimestamp
  private val erfasstTimestampOldHist = LocalDateTime.now.minusDays(2)
  private val ersetztTimestampOldHist = doomsday
  private val erfasstTimestampOldDeletedHist = LocalDateTime.now.minusDays(30)
  private val ersetztTimestampOldDeletedHist = LocalDateTime.now.minusDays(23)
  private val colNames = Seq("id", "name", "age", "health_state")
  private val colNullability = Map("id" -> true, "name" -> true, "age" -> true,
    "health_state" -> true)
  private val histColNames = Seq("id", "name", "age", "health_state", TechnicalTableColumn.captured.toString,
    TechnicalTableColumn.delimited.toString)
  private val primaryKeyColumns = Array("id", "name")
  private val referenceTimestampNew = LocalDateTime.now
  private val offsetNs = 1000000L
  private val referenceTimestampOld = referenceTimestampNew.minusNanos(offsetNs)

  // Executed before each test
  before {
    init()
  }

  /*
  Initializing of Hadoop components is only done once, otherwise you get errors 
  regarding mulitple instances of Hive Metastore
   */
  private def init(): Unit = {
  }

  private def toHistorizedDf(session: SparkSession, records: List[Tuple4[Int, String, Int, String]],
                             phase: HistorizationPhase.HistorizationPhase): DataFrame = {
    import session.sqlContext.implicits._
    val rddHist = session.sparkContext.parallelize(records)
    phase match {
      case HistorizationPhase.Existing => {
        val dfHist = rddHist.toDF(colNames: _*)
        .withColumn(s"${TechnicalTableColumn.captured.toString}", ts1(erfasstTimestampOldHist))
        .withColumn(s"${TechnicalTableColumn.delimited.toString}", ts1(ersetztTimestampOldHist))
        setNullableStateOfColumn(dfHist, colNullability)
      }
      case HistorizationPhase.UpdatedOld => {
        val dfHist = rddHist.toDF(colNames: _*)
          .withColumn(s"${TechnicalTableColumn.captured.toString}", ts1(erfasstTimestampOldHist))
          .withColumn(s"${TechnicalTableColumn.delimited.toString}", ts1(referenceTimestampOld))
        setNullableStateOfColumn(dfHist, colNullability)
      }
      case HistorizationPhase.UpdatedNew => {
        val dfHist = rddHist.toDF(colNames: _*)
          .withColumn(s"${TechnicalTableColumn.captured.toString}", ts1(referenceTimestampNew))
          .withColumn(s"${TechnicalTableColumn.delimited.toString}", ts1(doomsday))
        setNullableStateOfColumn(dfHist, colNullability)
      }
      case HistorizationPhase.NewlyAdded => {
        val dfHist = rddHist.toDF(colNames: _*)
          .withColumn(s"${TechnicalTableColumn.captured.toString}", ts1(referenceTimestampNew))
          .withColumn(s"${TechnicalTableColumn.delimited.toString}", ts1(doomsday))
        setNullableStateOfColumn(dfHist, colNullability)
      }
      case HistorizationPhase.TechnicallyDeleted => {
        val dfHist = rddHist.toDF(colNames: _*)
          .withColumn(s"${TechnicalTableColumn.captured.toString}", ts1(erfasstTimestampOldDeletedHist))
          .withColumn(s"${TechnicalTableColumn.delimited.toString}", ts1(ersetztTimestampOldDeletedHist))
        setNullableStateOfColumn(dfHist, colNullability)
      }
    }
  }

  private def toNewFeedDf(session: SparkSession, records: List[Tuple4[Int, String, Int, String]]) = {
    import session.sqlContext.implicits._
    val rdd = session.sqlContext.sparkContext.parallelize(records)
    val df = rdd.toDF(colNames: _*)
    setNullableStateOfColumn(df, colNullability)
  }

  def setNullableStateOfColumn(df: DataFrame, nullValues: Map[String, Boolean]) : DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField( c, t, _, m) if nullValues.contains(c) => StructField( c, t,
        nullable = nullValues.get(c).get, m)
      case y: StructField => y
    })
    df.sqlContext.createDataFrame( df.rdd, newSchema )
  }

  private def sortResults(df: DataFrame): DataFrame = {
    df.sort("id", TechnicalTableColumn.delimited.toString)
  }

  test("History unchanged with new columns but unchanged data..") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(session, baseColumnsOldHist, HistorizationPhase.Existing)
    logger.debug(s"History at beginning: ${dfOldHist.collect.foreach(println)}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toNewFeedDf(session, baseColumnsNewFeed)
    val dfNewFeedWithAdditionalCols = dfNewFeed.withColumn("new_col1", lit(null).cast(StringType))
    logger.debug(s"New feed: ${dfNewFeedWithAdditionalCols.collect.foreach(println)}")

    val dfHistorized = Historization.getHistorized(dfOldHist, dfNewFeedWithAdditionalCols, primaryKeyColumns,
      referenceTimestampNew, None, None)
    logger.debug(s"Historization result: ${dfHistorized.collect.foreach(println)}")

    val baseColumnsUnchanged = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfUnchanged = toHistorizedDf(session, baseColumnsUnchanged, HistorizationPhase.Existing)
    val dfUnchangedWithAdditionalCols = dfUnchanged.withColumn("new_col1", lit(null).cast(StringType))

    dfHistorized.printSchema()
    dfHistorized.show

    val dfExpected = dfUnchangedWithAdditionalCols
    logger.debug(s"Expected result: ${dfExpected.collect.foreach(println)}")
    TestUtil.isDataFrameEqual(sortResults(dfExpected), sortResults(dfHistorized))

  }

  test("History should change with new not-null columns.") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(session, baseColumnsOldHist, HistorizationPhase.Existing)
    logger.debug(s"History at beginning: ${dfOldHist.collect.foreach(println)}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toNewFeedDf(session, baseColumnsNewFeed)
    val dfNewFeedWithAdditionalCols = dfNewFeed.withColumn("new_col1", lit("Test").cast(StringType))
    logger.debug(s"New feed: ${dfNewFeedWithAdditionalCols.collect.foreach(println)}")

    val dfHistorized = Historization.getHistorized(dfOldHist, dfNewFeedWithAdditionalCols, primaryKeyColumns,
      referenceTimestampNew, None, None)
    logger.debug(s"Historization result: ${dfHistorized.collect.foreach(println)}")


    val baseColumnsUpdatedOld = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfUpdatedOld = toHistorizedDf(session, baseColumnsUpdatedOld, HistorizationPhase.UpdatedOld)
      .withColumn("new_col1", lit(null).cast(StringType))

    val baseColumnsUpdatedNew = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfUpdatedNew = toHistorizedDf(session, baseColumnsUpdatedNew, HistorizationPhase.UpdatedNew)
      .withColumn("new_col1", lit("Test").cast(StringType))

    val dfExpected = dfUpdatedNew.union(dfUpdatedOld)

    dfOldHist.show(10, truncate = false)
    dfNewFeed.show(10, truncate = false)

    dfHistorized.show(10, truncate = false)
    dfExpected.show(10, truncate = false)

    logger.debug("dfHistorized:")
    dfHistorized.printSchema()
    dfHistorized.show

    logger.debug(s"Expected result: ${dfExpected.collect.foreach(println)}")
    TestUtil.isDataFrameEqual(sortResults(dfExpected), sortResults(dfHistorized))

  }

  ignore("History unchanged when deleting columns but unchanged data.") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(session, baseColumnsOldHist, HistorizationPhase.Existing)
    logger.debug(s"History at beginning: ${dfOldHist.collect.foreach(println)}")

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toNewFeedDf(session, baseColumnsNewFeed)

    val dfNewFeedWithDeletedCols = dfNewFeed.drop("health_state")
    logger.debug(s"New feed: ${dfNewFeedWithDeletedCols.collect.foreach(println)}")

    val dfHistorized = Historization.getHistorized(dfOldHist, dfNewFeedWithDeletedCols, primaryKeyColumns,
      referenceTimestampNew, None, None)
    logger.debug(s"Historization result: ${dfHistorized.collect.foreach(println)}")

    val baseColumnsUpdatedOld = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfExpected = toHistorizedDf(session, baseColumnsUpdatedOld, HistorizationPhase.Existing)

    logger.debug(s"Expected result: ${dfExpected.collect.foreach(println)}")
    TestUtil.isDataFrameEqual(sortResults(dfExpected), sortResults(dfHistorized))

  }

  test("The history should stay unchanged when using the current load again.") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(session, baseColumnsOldHist, HistorizationPhase.Existing)
    if (logger.isDebugEnabled) {
      logger.debug("History at beginning:")
      dfOldHist.collect.foreach(println)
    }

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toNewFeedDf(session, baseColumnsNewFeed)
    if (logger.isDebugEnabled) {
      logger.debug("New feed:")
      dfNewFeed.collect.foreach(println)
    }

    val dfHistorized = Historization.getHistorized(dfOldHist, dfNewFeed, primaryKeyColumns,
      referenceTimestampNew, None, None)
    if (logger.isDebugEnabled) {
      logger.debug("Historization result:")
      dfHistorized.collect.foreach(println)
    }

    val baseColumnsUnchanged = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfUnchanged = toHistorizedDf(session, baseColumnsUnchanged, HistorizationPhase.Existing)

    val dfExpected = dfUnchanged
    if (logger.isDebugEnabled) {
      logger.debug("Expected result:")
      dfExpected.collect.foreach(println)
    }

    dfOldHist.show(10, truncate = false)
    dfNewFeed.show(10, truncate = false)

    dfExpected.show(10, truncate = false)
    dfHistorized.show(10, truncate = false)

    TestUtil.isDataFrameEqual(sortResults(dfExpected), sortResults(dfHistorized))
  }


  test("History should stay unchanged when using current load but with different column sorting.") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(session, baseColumnsOldHist, HistorizationPhase.Existing)
    if (logger.isDebugEnabled) {
      logger.debug("History at beginning:")
      dfOldHist.collect.foreach(println)
    }

    import session.implicits._

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toNewFeedDf(session, baseColumnsNewFeed).select($"age", $"health_state", $"id", $"name")
    if (logger.isDebugEnabled) {
      logger.debug("New feed:")
      dfNewFeed.collect.foreach(println)
    }

    val dfHistorized = Historization.getHistorized(dfOldHist, dfNewFeed, primaryKeyColumns,
      referenceTimestampNew, None, None)
    if (logger.isDebugEnabled) {
      logger.debug("Historization result:")
      dfHistorized.collect.foreach(println)
    }

    val baseColumnsUnchanged = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfUnchanged = toHistorizedDf(session, baseColumnsUnchanged, HistorizationPhase.Existing)

    val dfExpected = dfUnchanged
    if (logger.isDebugEnabled) {
      logger.debug("Expected result:")
      dfExpected.collect.foreach(println)
    }

    dfOldHist.show(10, truncate = false)
    dfNewFeed.show(10, truncate = false)

    dfExpected.show(10, truncate = false)
    dfHistorized.show(10, truncate = false)

    TestUtil.isDataFrameEqual(sortResults(dfExpected), sortResults(dfHistorized))
  }


  test("When updating 1 record, the history should contain the old and the new version of the values.") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(session, baseColumnsOldHist, HistorizationPhase.Existing)
    if (logger.isDebugEnabled) {
      logger.debug("History at beginning:")
      dfOldHist.collect.foreach(println)
    }

    val baseColumnsNewFeed = List((123, "Egon", 23, "sick"), (124, "Erna", 27, "healthy"))
    val dfNewFeed = toNewFeedDf(session, baseColumnsNewFeed)
    if (logger.isDebugEnabled) {
      logger.debug("New feed:")
      dfNewFeed.collect.foreach(println)
    }

    val dfHistorized = Historization.getHistorized(dfOldHist, dfNewFeed, primaryKeyColumns,
      referenceTimestampNew, None, None)
    if (logger.isDebugEnabled) {
      logger.debug("Historization result:")
      dfHistorized.collect.foreach(println)
    }

    val baseColumnsUpdatedOld = List((123, "Egon", 23, "healthy"))
    val dfUpdatedOld = toHistorizedDf(session, baseColumnsUpdatedOld, HistorizationPhase.UpdatedOld)

    val baseColumnsUpdatedNew = List((123, "Egon", 23, "sick"))
    val dfUpdatedNew = toHistorizedDf(session, baseColumnsUpdatedNew, HistorizationPhase.UpdatedNew)

    val baseColumnsUnchanged = List((124, "Erna", 27, "healthy"))
    val dfUnchanged = toHistorizedDf(session, baseColumnsUnchanged, HistorizationPhase.Existing)

    val dfExpected = dfUpdatedNew.union(dfUpdatedOld).union(dfUnchanged)
    if (logger.isDebugEnabled) {
      logger.debug("Expected result:")
      dfExpected.collect.foreach(println)
    }

    TestUtil.isDataFrameEqual(sortResults(dfExpected), sortResults(dfHistorized))
  }

  test("When deleting 1 record (technical deletion) the dl_ts_delimited column should be updated.") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(session, baseColumnsOldHist, HistorizationPhase.Existing)
    if (logger.isDebugEnabled) {
      logger.debug("History at beginning:")
      dfOldHist.collect.foreach(println)
    }

    val baseColumnsNewFeed = List((124, "Erna", 27, "healthy"))
    val dfNewFeed = toNewFeedDf(session, baseColumnsNewFeed)
    if (logger.isDebugEnabled) {
      logger.debug("New feed:")
      dfNewFeed.collect.foreach(println)
    }

    val dfHistorized = Historization.getHistorized(dfOldHist, dfNewFeed, primaryKeyColumns,
      referenceTimestampNew, None, None)
    if (logger.isDebugEnabled) {
      logger.debug("Historization result:")
      dfHistorized.collect.foreach(println)
    }

    val baseColumnsUpdatedOld = List((123, "Egon", 23, "healthy"))
    val dfUpdatedOld = toHistorizedDf(session, baseColumnsUpdatedOld, HistorizationPhase.UpdatedOld)

    val baseColumnsUnchanged = List((124, "Erna", 27, "healthy"))
    val dfUnchanged = toHistorizedDf(session, baseColumnsUnchanged, HistorizationPhase.Existing)

    val dfExpected = dfUpdatedOld.union(dfUnchanged)
    if (logger.isDebugEnabled) {
      logger.debug("Expected result:")
      dfExpected.collect.foreach(println)
    }

    TestUtil.isDataFrameEqual(sortResults(dfExpected), sortResults(dfHistorized))
  }

  test("When adding 1 record, the history should contain the new record.") {
    val baseColumnsOldHist = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfOldHist = toHistorizedDf(session, baseColumnsOldHist, HistorizationPhase.Existing)
    if (logger.isDebugEnabled) {
      logger.debug("History at beginning:")
      dfOldHist.collect.foreach(println)
    }

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"),
      (125, "Edeltraut", 54, "healthy"))
    val dfNewFeed = toNewFeedDf(session, baseColumnsNewFeed)
    if (logger.isDebugEnabled) {
      logger.debug("New feed:")
      dfNewFeed.collect.foreach(println)
    }

    val dfHistorized = Historization.getHistorized(dfOldHist, dfNewFeed, primaryKeyColumns,
      referenceTimestampNew, None, None)
    if (logger.isDebugEnabled) {
      logger.debug("Historization result:")
      dfHistorized.collect.foreach(println)
    }

    val baseColumnsUnchanged = List((123, "Egon", 23, "healthy"), (124, "Erna", 27, "healthy"))
    val dfUnchanged = toHistorizedDf(session, baseColumnsUnchanged, HistorizationPhase.Existing)

    val baseColumnsAdded = List((125, "Edeltraut", 54, "healthy"))
    val dfAdded = toHistorizedDf(session, baseColumnsAdded, HistorizationPhase.NewlyAdded)

    val dfExpected = dfAdded.union(dfUnchanged)
    if (logger.isDebugEnabled) {
      logger.debug("Expected result:")
      dfExpected.collect.foreach(println)
    }

    dfHistorized.show()

    TestUtil.isDataFrameEqual(sortResults(dfExpected), sortResults(dfHistorized))
  }

  test("When adding 1 record that was technically deleted in the past already, the history should contain the new version.") {
    val baseColumnsOldExistingHist = List((123, "Egon", 23, "healthy"))
    val dfOldExistingHist = toHistorizedDf(session, baseColumnsOldExistingHist, HistorizationPhase.Existing)

    val baseColumnsOldDeletedHist = List((124, "Erna", 27, "healthy"))
    val dfOldDeletedHist = toHistorizedDf(session, baseColumnsOldDeletedHist, HistorizationPhase.TechnicallyDeleted)

    val dfOldHist = dfOldExistingHist.union(dfOldDeletedHist)

    if (logger.isDebugEnabled) {
      logger.debug("History at beginning:")
      dfOldHist.collect.foreach(println)
    }

    val baseColumnsNewFeed = List((123, "Egon", 23, "healthy"), (124, "Erna", 28, "healthy"))
    val dfNewFeed = toNewFeedDf(session, baseColumnsNewFeed)
    if (logger.isDebugEnabled) {
      logger.debug("New feed:")
      dfNewFeed.collect.foreach(println)
    }

    val dfHistorized = Historization.getHistorized(dfOldHist, dfNewFeed, primaryKeyColumns,
      referenceTimestampNew, None, None)
    if (logger.isDebugEnabled) {
      logger.debug("Historization result:")
      dfHistorized.collect.foreach(println)
    }

    val baseColumnsUnchangedExistingHist = List((123, "Egon", 23, "healthy"))
    val dfUnchangedExistingHist = toHistorizedDf(session, baseColumnsUnchangedExistingHist, HistorizationPhase.Existing)

    val baseColumnsUnchangedDeletedHist = List((124, "Erna", 27, "healthy"))
    val dfUnchangedDeletedHist = toHistorizedDf(session, baseColumnsUnchangedDeletedHist, HistorizationPhase.TechnicallyDeleted)

    val dfUnchanged = dfUnchangedExistingHist.union(dfUnchangedDeletedHist)

    val baseColumnsAdded = List((124, "Erna", 28, "healthy"))
    val dfAdded = toHistorizedDf(session, baseColumnsAdded, HistorizationPhase.NewlyAdded)

    val dfExpected = dfAdded.union(dfUnchanged)
    if (logger.isDebugEnabled) {
      logger.debug("Expected result:")
      dfExpected.collect.foreach(println)
    }

    TestUtil.isDataFrameEqual(sortResults(dfExpected), sortResults(dfHistorized))
  }
}
