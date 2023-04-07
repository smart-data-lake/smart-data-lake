/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.azure

import com.azure.core.util.serializer.{ObjectSerializer, TypeReference}
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.monitor.ingestion.LogsIngestionClientBuilder
import com.azure.monitor.ingestion.models.LogsUploadOptions
import io.smartdatalake.util.azure.client.loganalytics.LogAnalyticsClient
import org.apache.logging.log4j.core.{Layout, LogEvent}
import org.apache.logging.log4j.layout.template.json.JsonTemplateLayout
import reactor.core.publisher.Mono

import java.io.{InputStream, OutputStream}
import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}

trait LogAnalyticsBackend {
  def send(events: Seq[LogEvent])
  def batchSize: Int
}

/**
 * LogAnalytics Http Data Collector API backend implementation
 *
 * @param workspaceId the id of the LogAnalytics workspace
 * @param workspaceKey the shared secret to access the LogAnalytics workspace
 * @param logType log type parameter submitted to LogAnalytics. LogAnalytics appends "_CL" to create the custom table name.
 * @param layout custom Log4j2 layout using [[JsonTemplateLayout]]. See also https://logging.apache.org/log4j/2.x/manual/json-template-layout.html.
 */
class LogAnalyticsHttpCollectorBackend(workspaceId: String, workspaceKey: String, logType: String, layout: JsonTemplateLayout) extends LogAnalyticsBackend {
  lazy private val client = new LogAnalyticsClient(workspaceId, workspaceKey)

  override val batchSize = 100 // azure log analytics' limit

  override def send(events: Seq[LogEvent]): Unit = {
    val body = "["+events.map(layout.toSerializable).mkString(",")+"]"
    client.send(body, logType)
  }
}

/**
 * LogAnalytics Ingestion API backend implementation
 *
 * @param endpoint Azure Monitor Data Collector Endpoint URI
 * @param ruleId Azure Monitor Data Collection Rule ID
 * @param tableName LogAnalytics table name configure in Data Collection Rule. The name normally ends with '_CL' as it is a Custom Log table.
 */
class LogAnalyticsIngestionBackend(endpoint: String, ruleId: String, tableName: String, layout: JsonTemplateLayout, override val batchSize: Int) extends LogAnalyticsBackend {
  private val azureCredentialBuilder = new DefaultAzureCredentialBuilder
  private val credential = azureCredentialBuilder.build
  private val client = new LogsIngestionClientBuilder().endpoint(endpoint).credential(credential).buildClient()
  private val uploadOptions = new LogsUploadOptions()
    .setLogsUploadErrorConsumer(err => println("ERROR LogAnalyticsIngestionBackend: "+err.getResponseException)) // don't log this to avoid loops
    .setObjectSerializer(new Log4jLayoutSerializer(layout))

  override def send(events: Seq[LogEvent]): Unit = {
    client.upload(ruleId, tableName, events.asInstanceOf[Seq[Object]].asJava, uploadOptions)
  }
}

/**
 * Implementation of Serializer for LogsIngestionClient using a Log4j2 layout.
 */
class Log4jLayoutSerializer(layout: Layout[_]) extends ObjectSerializer {
  override def deserialize[T](stream: InputStream, typeReference: TypeReference[T]): T = {
    throw new NotImplementedError("deserialize is not implemented")
  }

  override def deserializeAsync[T](stream: InputStream, typeReference: TypeReference[T]): Mono[T] = {
    Mono.fromCallable(() => deserialize(stream, typeReference))
  }

  override def serialize(stream: OutputStream, value: Any): Unit = {
    value match {
      case event: LogEvent => stream.write(layout.toByteArray(event))
      case x => throw new IllegalStateException(s"value must be instance of LogEvent, but is a ${x.getClass.getSimpleName}")
    }
  }

  override def serializeAsync(stream: OutputStream, value: Any): Mono[Void] = {
    Mono.fromCallable(() => serialize(stream, value).asInstanceOf[Void])
  }
}
