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

package io.smartdatalake.workflow.dataframe.spark

import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.spark.PushPredicateThroughTolerantCollectMetricsRuleObject.pushDownTolerantMetricsMarker
import io.smartdatalake.workflow.dataframe.DataFrameObservation
import org.apache.spark.sql._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import java.util.UUID


/**
 * This code is inspired from org.apache.spark.sql.Observation.
 *
 * Note 1: Observations are not supported streaming Datasets
 * Note 2: the name is used to make metrics unique across parallel queries in the same Spark session
 */
private[smartdatalake] class SparkObservation(name: String = UUID.randomUUID().toString) extends DataFrameObservation with SmartDataLakeLogger {

  private val listener: SparkObservationListener = SparkObservationListener(this)

  @volatile private var sparkSession: Option[SparkSession] = None

  @volatile private var metrics: Option[Map[String, Row]] = None

  def getName: String = name

  def on[T](ds: Dataset[T], registerListener: Boolean, exprs: Column*): Dataset[T] = {
    // check this is no streaming Dataset. It would need registering a StreamingQueryListener instead of a QueryExecutionListener.
    if (ds.isStreaming) throw new IllegalArgumentException("SparkObservation does not yet support streaming Datasets")
    sparkSession = Some(ds.sparkSession)
    if (registerListener) ds.sparkSession.listenerManager.register(listener)
    ds.observe(name, exprs.head, exprs.tail: _*)
  }

  /**
   * Get the observed metrics. This waits for the observed dataset to finish and deliver metrics with the name of this SparkObservation.
   *
   * @param timeoutSec max wait time in seconds. Throws NoMetricsReceivedException if metrics were not received in time.
   * @return the observed metrics as a `Map[String, Any]`
   */
  @throws[NoMetricsReceivedException]
  def waitFor(timeoutSec: Int = 1): Map[String, _] = {
    synchronized {
      // we need to loop as wait might return without us calling notify
      // https://en.wikipedia.org/w/index.php?title=Spurious_wakeup&oldid=992601610
      val ts = System.currentTimeMillis()
      while (metrics.isEmpty) {
        logger.debug(s"($name) waiting for metrics")
        wait(timeoutSec * 1000L)
        if (ts + timeoutSec * 1000L <= System.currentTimeMillis) throw NoMetricsReceivedException(s"SparkObservation $name did not receive metrics within timeout of $timeoutSec seconds.")
      }
    }
    extractMetrics()
  }

  /**
   * Set an observation name prefix of others metrics to extract if possible.
   * This is used to extract spark observations setup independently, using e.g. ActionId as prefix.
   */
  def setOtherObservationsPrefix(prefix: String): Unit = {
    otherObservationsPrefix = Some(prefix)
  }
  private var otherObservationsPrefix: Option[String] = None

  /**
   * Set names of other observation to extract if possible.
   */
  def setOtherObservationNames(names: Seq[String]): Unit = {
    otherObservationNames = names
  }
  private var otherObservationNames: Seq[String] = Seq()

  private[spark] def extractMetrics(): Map[String, _] = {
    // also extract other observations according to otherObservationsPrefix and otherObservationNames.
    metrics.getOrElse(Map())
      .filterKeys(k => k == name || otherObservationsPrefix.exists(k.startsWith) || otherObservationNames.contains(k)).toMap
      .flatMap { case (metricName, r) =>
        val namePostfix = if (metricName != name) {
          Some(otherObservationsPrefix.map(metricName.stripPrefix).getOrElse(metricName).stripSuffix(pushDownTolerantMetricsMarker).takeWhile(_ != '#'))
        } else None
        val metricEntries = r.getValuesMap[Any](r.schema.fieldNames).map(e => createMetric(namePostfix, e))
        logger.debug(s"($name) extractMetrics for $metricName got ${metricEntries.map { case (k, v) => s"$k=$v" }.mkString(" ")}")
        metricEntries
      }
  }

  private[spark] def onFinish(qe: QueryExecution): Unit = {
    synchronized {
      val observedMetrics = qe.observedMetrics
      if (metrics.isEmpty && observedMetrics.isDefinedAt(name)) {
        logger.debug(s"($name) onFinish got observations: ${observedMetrics.keys.mkString(", ")}")
        metrics = Some(qe.observedMetrics)
        notifyAll()
        sparkSession.foreach(_.listenerManager.unregister(listener))
      }
    }
  }

  private def createMetric(namePostfix: Option[String], observation: (String, Any)) = {
    val (k,v) = observation
    val metricName = namePostfix.map(p => s"$k#$p").getOrElse(k)
    (metricName, Option(v).getOrElse(None)) // if value is null convert to None
  }
}

private[smartdatalake] case class SparkObservationListener(observation: SparkObservation) extends QueryExecutionListener {
  // forward result on success
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = observation.onFinish(qe)
  // ignore result on failure
  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
}

case class NoMetricsReceivedException(msg: String) extends Exception(msg)
