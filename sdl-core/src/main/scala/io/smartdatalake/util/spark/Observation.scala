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

import org.apache.spark.sql._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.UUID

/**
 * This code is inspired from Spark 3.3.0 when it was not yet released (and simplified).
 * See https://github.com/apache/spark/blob/v3.3.0-rc1/sql/core/src/main/scala/org/apache/spark/sql/Observation.scala
 *
 * Note: the name is used to make metrics unique across parallel queries in the same Spark session
 */
private[smartdatalake] class Observation(name: String = UUID.randomUUID().toString) {

  private var listener: ObservationListener = ObservationListener(this)

  @volatile private var sparkSession: Option[SparkSession] = None

  @volatile private var metrics: Option[Map[String, Any]] = None

  def on[T](ds: Dataset[T], expr: Column, exprs: Column*): Dataset[T] = {
    if (ds.isStreaming) throw new IllegalArgumentException("Observation does not support streaming Datasets")
    sparkSession = Some(ds.sparkSession)
    ds.sparkSession.listenerManager.register(listener)
    ds.observe(name, expr, exprs: _*)
  }

  /**
   * Get the observed metrics. This waits for the observed dataset to finish its first action.
   * Only the result of the first action is available. Subsequent actions do not modify the result.
   *
   * @timeoutSec max wait time in seconds. Throws NoMetricsReceivedException if metrics were not received in time.
   * @return the observed metrics as a `Map[String, Any]`
   */
  @throws[InterruptedException]
  def waitFor(timeoutSec: Int = 10): Map[String, _] = {
    synchronized {
      // we need to loop as wait might return without us calling notify
      // https://en.wikipedia.org/w/index.php?title=Spurious_wakeup&oldid=992601610
      val ts = System.currentTimeMillis()
      while (metrics.isEmpty) {
        wait(timeoutSec * 1000L)
        if (ts + timeoutSec * 1000L <= System.currentTimeMillis) throw NoMetricsReceivedException(s"Observation $name did not receive metrics within timeout of $timeoutSec seconds.")
      }
    }
    metrics.get
  }

  private[spark] def onFinish(qe: QueryExecution): Unit = {
    synchronized {
      if (metrics.isEmpty) {
        val row = qe.observedMetrics.get(name)
        this.metrics = row.map(r => r.getValuesMap[Any](r.schema.fieldNames))
        if (metrics.isDefined) {
          notifyAll()
          sparkSession.foreach(_.listenerManager.unregister(listener))
        }
      }
    }
  }
}

private[smartdatalake] case class ObservationListener(observation: Observation) extends QueryExecutionListener {
  // forward result on success
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = observation.onFinish(qe)
  // ignore result on failure
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = Unit
}

case class NoMetricsReceivedException(msg: String) extends Exception(msg)