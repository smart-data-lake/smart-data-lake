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

package io.smartdatalake.workflow.dataframe

/**
 * An Observation can observe metrics during evaluation of DataFrames.
 * In case the engine or implementation does not support observing metrics, they can also be calculated when calling waitFor method.
 */
trait DataFrameObservation {

  /**
   * Get the observed metrics.
   *
   * @param timeoutSec max wait time in seconds. Throws NoMetricsReceivedException if metrics were not received in time.
   *                   timeoutSec can be ignored if the Observation implementation is calculating results.
   * @param otherMetricsPrefix metric name prefix of others metrics to extract if possible. This is used to extract spark observations setup independently, using ActionId as prefix.
   * @return the observed metrics as a `Map[String, Any]`
   */
  @throws[InterruptedException]
  def waitFor(timeoutSec: Int = 10, otherMetricsPrefix: Option[String] = None): Map[String, _]

}


/**
 * Calculate metrics to fake observation result.
 * For Snowpark this is the only method to observe metrics.
 */
private[smartdatalake] case class GenericCalculatedObservation(df: GenericDataFrame, aggregateColumns: GenericColumn*) extends DataFrameObservation {
  override def waitFor(timeoutSec: Int, otherMetricsPrefix: Option[String] = None): Map[String, _] = {
    // calculate aggregate expressions on DataFrame
    val dfObservations = df.agg(aggregateColumns)
    val metricsRow = dfObservations.collect.headOption
    if (metricsRow.isDefined) {
      // convert results to metrics map
      dfObservations.schema.columns.zip(metricsRow.get.toSeq).toMap
    } else Map()
  }
}

