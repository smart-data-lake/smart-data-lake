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
package io.smartdatalake.workflow.dataframe.sparkconnect

import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataframe.{DataFrameObservation, GenericCalculatedObservation, GenericColumn}
import org.apache.spark.sql.{Column, Dataset, Observation, Row}

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * DataFrameObservation implementation for the Spark Connect engine, based on the standard Spark API
 * `org.apache.spark.sql.Observation` and `Dataset.observe(Observation, ...)`.
 *
 * Unlike `SparkObservation` of sdl-spark this needs no QueryExecutionListener (there is none on a Spark Connect
 * client): the observed metrics are transported back to the client with the response of the query or command
 * executing the plan, and Spark completes the future of the Observation registered in the sessions observation registry.
 *
 * Not every execution delivers observed metrics though. The response then contains an empty metrics Row, and the
 * metrics are calculated with a separate query on the (cached) DataFrame instead, like GenericCalculatedObservation.
 * Known cases:
 * - writing with a DataSource which creates its own QueryExecution on the server, e.g. delta lake and Iceberg
 * - plans which are not sent to the server as such, e.g. because the observed DataFrame was registered as a
 *   temporary view and is referenced by a SQL statement (SQLDfTransformer, SQLDfsTransformer)
 * - the observed DataFrame is not part of the executed plan at all, e.g. an input which is not used by the
 *   transformation of an Action
 *
 * An Observation delivers the metrics of its own CollectMetrics node only. Metrics of the sibling input observations
 * are therefore collected by this observation as well, see linkWithInputObservations: as the input observations are
 * normally part of the same plan as the output observation, their metrics are delivered with the same response.
 *
 * Note 1: Observations are not supported for streaming Datasets.
 * Note 2: an Observation can be used with one Dataset only.
 * Note 3: the name is used to make metrics unique across parallel queries in the same Spark session.
 */
private[smartdatalake] class SparkConnectObservation(name: String = UUID.randomUUID().toString, df: SparkConnectDataFrame, aggregateColumns: Seq[GenericColumn]) extends DataFrameObservation with SmartDataLakeLogger {

  private val observation = new Observation(name)

  def getName: String = name

  /**
   * Attach this observation to the given Dataset.
   * @return the Dataset with the observation attached. Metrics are only delivered when this Dataset is executed.
   */
  def on[T](ds: Dataset[T], exprs: Column*): Dataset[T] = {
    // check this is no streaming Dataset. It would need registering a StreamingQueryListener instead.
    if (ds.isStreaming) throw new IllegalArgumentException("SparkConnectObservation does not support streaming Datasets")
    ds.observe(observation, exprs.head, exprs.tail.toIndexedSeq: _*)
  }

  override def linkWithInputObservations(inputObservations: Seq[DataFrameObservation], prefix: String): Unit = {
    otherObservations = inputObservations.collect { case x: SparkConnectObservation => x }
  }
  private var otherObservations: Seq[SparkConnectObservation] = Seq()

  // input observations are handled by linkWithInputObservations, so that they can be read without waiting for a timeout
  override def includeInInputObservationCombine: Boolean = false

  /**
   * Get the observed metrics. This waits for the observed Dataset to be executed and its metrics to be delivered,
   * and calculates them with a separate query if the execution delivered no metrics.
   * Metrics of linked input observations are added with the DataObjectId as postfix, e.g. count#src1.
   *
   * @param timeoutSec max wait time in seconds for the observed metrics.
   * @return the observed metrics as a `Map[String, Any]`
   */
  override def waitFor(timeoutSec: Int = 1): Map[String, _] = {
    getMetrics(timeoutSec, addNamePostfix = false) ++
      otherObservations.flatMap(_.getMetrics(SparkConnectObservation.otherObservationsWaitSec, addNamePostfix = true))
  }

  /**
   * @param addNamePostfix if true, the DataObjectId of this observations name is added as postfix to the metric names, e.g. count#src1.
   */
  private def getMetrics(timeoutSec: Int, addNamePostfix: Boolean): Map[String, _] = {
    logger.debug(s"($name) waiting for metrics")
    // Observation.get/getRow would block forever, use the underlying future to implement the timeout.
    // Note that an empty Row is delivered if the execution did not report metrics for this observation.
    val observedRow = Try(Await.result(observation.future, Duration(timeoutSec, TimeUnit.SECONDS))).toOption.filter(_.length > 0)
    val metrics = observedRow.map(createMetrics)
      .getOrElse {
        logger.debug(s"($name) no metrics observed, calculating them with a separate query")
        GenericCalculatedObservation(df, aggregateColumns: _*).waitFor(timeoutSec)
      }
    val postfixedMetrics = if (addNamePostfix) metrics.map { case (k, v) => (s"$k#$metricsPostfix", v) } else metrics
    logger.debug(s"($name) got metrics ${postfixedMetrics.map { case (k, v) => s"$k=$v" }.mkString(" ")}")
    postfixedMetrics
  }

  /**
   * The observation name is composed by ExpectationValidation as "<dataObjectId>#<uuid>[!pushDownTolerant]".
   */
  private def metricsPostfix: String = name.stripSuffix(SparkConnectObservation.pushDownTolerantMetricsMarker).takeWhile(_ != '#')

  private def createMetrics(row: Row): Map[String, _] = {
    row.getValuesMap[Any](row.schema.fieldNames.toList)
      .map { case (k, v) => (k, Option(v).getOrElse(None)) } // if value is null convert to None
  }
}

private[smartdatalake] object SparkConnectObservation {

  /**
   * Marker added to observation names by ExpectationValidation, see PushPredicateThroughTolerantCollectMetricsRule of sdl-spark.
   * Note that the corresponding catalyst rule is a server side session extension and normally not installed on a Spark Connect server.
   */
  private val pushDownTolerantMetricsMarker = "!pushDownTolerant"

  /**
   * Max wait time for metrics of linked input observations. They are delivered with the same response as the metrics
   * of this observation, so this is just to bridge the gap until all observations of a response are completed.
   */
  private val otherObservationsWaitSec = 1
}
