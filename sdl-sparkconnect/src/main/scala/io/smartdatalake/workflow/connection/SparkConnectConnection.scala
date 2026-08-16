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
package io.smartdatalake.workflow.connection

import com.typesafe.config.Config
import io.smartdatalake.app.AppUtil.createMaskedSecretsKVLog
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.sparkconnect.SparkConnectSubFeed
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Connection information for a Spark Connect session.
 *
 * Note that a Spark Connect session is a thin client connected to a remote Spark Connect server.
 * There is no Hadoop FileSystem API available to access remote data - all data access happens through
 * spark.read/write on the server side.
 *
 * @param url
 *   remote URL of the Spark Connect server, e.g. "sc://localhost:15002".
 *   Additional parameters like tokens can be appended according to the Spark Connect connection string spec,
 *   e.g. "sc://host:port/;token=ABCDEFG;user_id=user".
 * @param sparkOptions
 *   spark options to set on the session builder
 */
case class SparkConnectConnection(
    override val id: ConnectionId,
    url: String,
    sparkOptions: Map[String, StringOrSecret] = Map(),
    override val metadata: Option[ConnectionMetadata] = None
) extends Connection with EngineConnection with SmartDataLakeLogger {

  val subFeedType: Type = typeOf[SparkConnectSubFeed]

  @transient private var _sparkSession: Option[SparkSession] = None
  def sparkSession(implicit context: ActionPipelineContext): SparkSession = {
    if (_sparkSession.isEmpty) {
      logger.info(s"($id) creating Spark Connect session for remote url $url")
      if (sparkOptions.nonEmpty) logger.info(s"($id) additional sparkOptions: " + sparkOptions.map { case (k, v) => createMaskedSecretsKVLog(k, v.toString) }.mkString(", "))
      val builder = SparkSession.builder().remote(url)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") // default value for normal operation of SDL; can be overwritten by configuration (sparkOptions)
      val session = sparkOptions.foldLeft(builder) {
        case (builder, (key, value)) => builder.config(key, value.resolve())
      }.getOrCreate()
      _sparkSession = Some(session)
    }
    _sparkSession.get
  }

  /**
   * Tag operations on the remote session for better traceability on the Spark Connect server.
   */
  override def activate(operation: Option[String])(implicit context: ActionPipelineContext): Unit = {
    val metadataId = context.currentAction.map(_.id).getOrElse(id)
    sparkSession.clearTags()
    sparkSession.addTag(s"${context.appConfig.appName}-$metadataId-runId-${context.executionId.runId}".replaceAll("[,.]", "-"))
  }
}

object SparkConnectConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SparkConnectConnection = {
    extract[SparkConnectConnection](config)
  }
}
