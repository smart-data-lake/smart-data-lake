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

package io.smartdatalake.util.historization

import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
import org.apache.spark.sql.{Encoder, SparkSession}

import java.sql.Timestamp
import java.time.{Duration, LocalDateTime}

object HistorizationTestUtils {

  object HistorizationPhase extends Enumeration {
    type HistorizationPhase = Value
    val Existing: HistorizationPhase = Value
    val UpdatedNew: HistorizationPhase = Value
    val UpdatedOld: HistorizationPhase = Value
    val NewlyAdded: HistorizationPhase = Value
    val TechnicallyDeleted: HistorizationPhase = Value
  }

  private[smartdatalake] val defaultTimeAxisUnit = Some(Duration.ofMillis(1))

  private[historization] val doomsday = Environment.historizationUpperHorizonTimestamp.toLocalDateTime
  private[historization] val doomsdayTs = Environment.historizationUpperHorizonTimestamp
  private[historization] val erfasstTimestampOldHist = LocalDateTime.now.minusDays(2)
  private[historization] val erfasstTimestampOldHistTs = Timestamp.valueOf(erfasstTimestampOldHist)
  private[historization] val ersetztTimestampOldHist = doomsday
  private[historization] val ersetztTimestampOldHistTs = Timestamp.valueOf(ersetztTimestampOldHist)
  private[historization] val erfasstTimestampOldDeletedHist = LocalDateTime.now.minusDays(30)
  private[historization] val erfasstTimestampOldDeletedHistTs = Timestamp.valueOf(erfasstTimestampOldDeletedHist)
  private[historization] val ersetztTimestampOldDeletedHist = LocalDateTime.now.minusDays(23)
  private[historization] val ersetztTimestampOldDeletedHistTs = Timestamp.valueOf(ersetztTimestampOldDeletedHist)
  private[historization] val colNames = Seq("id", "name", "age", "health_state")
  private[historization] val primaryKeyColumns = Array("id", "name")
  private[historization] val referenceTimestampNew = LocalDateTime.now
  private[historization] val referenceTimestampNewTs = Timestamp.valueOf(referenceTimestampNew)

  private[smartdatalake] def getReferenceTimestampOldTs(timeUnitAxis: Option[Duration] = defaultTimeAxisUnit) = Timestamp.valueOf(timeUnitAxis.map(referenceTimestampNew.minus(_)).getOrElse(referenceTimestampNew))

  def toHistorizedDf[T <: Product : Encoder](records: Seq[T], phase: HistorizationPhase.HistorizationPhase, colNames: Seq[String] = this.colNames, withHashCol: Boolean = false, withOperation: Boolean = false, timeUnitAxis: Option[Duration] = defaultTimeAxisUnit)
                                            (implicit session: SparkSession, functions: DataFrameFunctions): GenericDataFrame = {
    import functions._
    val referenceTimestampOldTs = getReferenceTimestampOldTs(timeUnitAxis)
    var operation: Option[String] = None
    var dfHist = phase match {
      case HistorizationPhase.Existing =>
        toDataDf(records, colNames)
          .withColumn(s"${Environment.capturedColumnName}", lit(erfasstTimestampOldHistTs))
          .withColumn(s"${Environment.delimitedColumnName}", lit(ersetztTimestampOldHistTs))
      case HistorizationPhase.UpdatedOld =>
        operation = Some(HistorizationRecordOperations.updateClose)
        toDataDf(records, colNames)
          .withColumn(s"${Environment.capturedColumnName}", lit(erfasstTimestampOldHistTs))
          .withColumn(s"${Environment.delimitedColumnName}", lit(referenceTimestampOldTs))
      case HistorizationPhase.UpdatedNew =>
        operation = Some(HistorizationRecordOperations.insertNew)
        toDataDf(records, colNames)
          .withColumn(s"${Environment.capturedColumnName}", lit(referenceTimestampNew))
          .withColumn(s"${Environment.delimitedColumnName}", lit(doomsdayTs))
      case HistorizationPhase.NewlyAdded =>
        operation = Some(HistorizationRecordOperations.insertNew)
        toDataDf(records, colNames)
          .withColumn(s"${Environment.capturedColumnName}", lit(referenceTimestampNewTs))
          .withColumn(s"${Environment.delimitedColumnName}", lit(doomsdayTs))
      case HistorizationPhase.TechnicallyDeleted =>
        operation = Some(HistorizationRecordOperations.updateClose)
        toDataDf(records, colNames)
          .withColumn(s"${Environment.capturedColumnName}", lit(erfasstTimestampOldDeletedHistTs))
          .withColumn(s"${Environment.delimitedColumnName}", lit(ersetztTimestampOldDeletedHistTs))
    }
    if (withHashCol) dfHist = Historization.addHashCol(dfHist, None, None, useHash = true, colsToIgnore = Seq(Environment.capturedColumnName, Environment.delimitedColumnName))
    if (withOperation) dfHist = dfHist.withColumn(Historization.historizeOperationColName, operation.map(lit).getOrElse(lit(null)))
    dfHist
  }

  def toDataDf[T <: Product : Encoder](records: Seq[T], colNames: Seq[String] = this.colNames)
                                      (implicit session: SparkSession): GenericDataFrame = {
    import session.sqlContext.implicits._
    SparkDataFrame(records.toDF(colNames: _*))
  }

}
