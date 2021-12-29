/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import java.sql.Timestamp

import io.smartdatalake.definitions.{HiveConventions, TechnicalTableColumn}
import io.smartdatalake.util.historization.Historization.{localDateTimeToCol, localDateTimeToTstmp}
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.apache.spark.sql.functions.lit
import java.time.LocalDateTime

object HistorizationTestUtils {

  object HistorizationPhase extends Enumeration {
    type HistorizationPhase = Value
    val Existing: HistorizationPhase = Value
    val UpdatedNew: HistorizationPhase = Value
    val UpdatedOld: HistorizationPhase = Value
    val NewlyAdded: HistorizationPhase = Value
    val TechnicallyDeleted: HistorizationPhase = Value
  }

  private[historization] val doomsday = HiveConventions.getHistorizationSurrogateTimestamp
  private[historization] val doomsdayTs = localDateTimeToTstmp(HiveConventions.getHistorizationSurrogateTimestamp)
  private[historization] val erfasstTimestampOldHist = LocalDateTime.now.minusDays(2)
  private[historization] val erfasstTimestampOldHistTs = localDateTimeToTstmp(erfasstTimestampOldHist)
  private[historization] val ersetztTimestampOldHist = doomsday
  private[historization] val ersetztTimestampOldHistTs = localDateTimeToTstmp(ersetztTimestampOldHist)
  private[historization] val erfasstTimestampOldDeletedHist = LocalDateTime.now.minusDays(30)
  private[historization] val erfasstTimestampOldDeletedHistTs = localDateTimeToTstmp(erfasstTimestampOldDeletedHist)
  private[historization] val ersetztTimestampOldDeletedHist = LocalDateTime.now.minusDays(23)
  private[historization] val ersetztTimestampOldDeletedHistTs = localDateTimeToTstmp(ersetztTimestampOldDeletedHist)
  private[historization] val colNames = Seq("id", "name", "age", "health_state")
  private[historization] val primaryKeyColumns = Array("id", "name")
  private[historization] val referenceTimestampNew = LocalDateTime.now
  private[historization] val referenceTimestampNewTs = localDateTimeToTstmp(referenceTimestampNew)
  private[historization] val offsetNs = 1000000L
  private[historization] val referenceTimestampOld = referenceTimestampNew.minusNanos(offsetNs)
  private[historization] val referenceTimestampOldTs = localDateTimeToTstmp(referenceTimestampOld)

  def toHistorizedDf[T <: Product : Encoder](records: Seq[T], phase: HistorizationPhase.HistorizationPhase, colNames: Seq[String] = this.colNames, withHashCol: Boolean = false, withOperation: Boolean = false)
                                            (implicit session: SparkSession): DataFrame = {
    var operation: Option[String] = None
    var dfHist = phase match {
      case HistorizationPhase.Existing =>
        toDataDf(records, colNames)
          .withColumn(s"${TechnicalTableColumn.captured}", lit(erfasstTimestampOldHistTs))
          .withColumn(s"${TechnicalTableColumn.delimited}", lit(ersetztTimestampOldHistTs))
      case HistorizationPhase.UpdatedOld =>
        operation = Some(HistorizationRecordOperations.updateClose)
        toDataDf(records, colNames)
          .withColumn(s"${TechnicalTableColumn.captured}", lit(erfasstTimestampOldHistTs))
          .withColumn(s"${TechnicalTableColumn.delimited}", lit(referenceTimestampOldTs))
      case HistorizationPhase.UpdatedNew =>
        operation = Some(HistorizationRecordOperations.insertNew)
        toDataDf(records, colNames)
          .withColumn(s"${TechnicalTableColumn.captured}", lit(Timestamp.valueOf(referenceTimestampNew)))
          .withColumn(s"${TechnicalTableColumn.delimited}", lit(Timestamp.valueOf(doomsday)))
      case HistorizationPhase.NewlyAdded =>
        operation = Some(HistorizationRecordOperations.insertNew)
        toDataDf(records, colNames)
          .withColumn(s"${TechnicalTableColumn.captured}", lit(referenceTimestampNewTs))
          .withColumn(s"${TechnicalTableColumn.delimited}", lit(doomsdayTs))
      case HistorizationPhase.TechnicallyDeleted =>
        operation = Some(HistorizationRecordOperations.updateClose)
        toDataDf(records, colNames)
          .withColumn(s"${TechnicalTableColumn.captured}", lit(erfasstTimestampOldDeletedHistTs))
          .withColumn(s"${TechnicalTableColumn.delimited}", lit(ersetztTimestampOldDeletedHistTs))
    }
    if (withHashCol) dfHist = Historization.addHashCol(dfHist, None, None, useHash = true, colsToIgnore = Seq(TechnicalTableColumn.captured, TechnicalTableColumn.delimited))
    if (withOperation) dfHist = dfHist.withColumn(Historization.historizeOperationColName, operation.map(lit).getOrElse(lit(null)))
    dfHist
  }

  def toDataDf[T <: Product : Encoder](records: Seq[T], colNames: Seq[String] = this.colNames)
                                      (implicit session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    records.toDF(colNames: _*)
  }

}
