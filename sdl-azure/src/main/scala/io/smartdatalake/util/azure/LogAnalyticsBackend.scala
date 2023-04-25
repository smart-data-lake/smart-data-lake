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

import com.azure.core.credential.TokenCredential
import com.azure.core.util.serializer.{ObjectSerializer, TypeReference}
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.monitor.ingestion.LogsIngestionClientBuilder
import com.azure.monitor.ingestion.models.LogsUploadOptions
import io.smartdatalake.util.azure.client.loganalytics.LogAnalyticsClient
import reactor.core.publisher.Mono

import java.io.{InputStream, OutputStream}
import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}

trait LogAnalyticsBackend[A] {
  def send(events: Seq[A]): Unit

  def batchSize: Int
}

/**
 * LogAnalytics Http Data Collector API backend implementation
 *
 * @param workspaceId  the id of the LogAnalytics workspace
 * @param workspaceKey the shared secret to access the LogAnalytics workspace
 * @param logType      log type parameter submitted to LogAnalytics. LogAnalytics appends "_CL" to create the custom table name.
 * @param serialize    a function to serialize the Log object to a Json String
 */
class LogAnalyticsHttpCollectorBackend[A](workspaceId: String, workspaceKey: String, logType: String, serialize: A => String) extends LogAnalyticsBackend[A] {
  lazy private val client = new LogAnalyticsClient(workspaceId, workspaceKey)

  override val batchSize = 100 // azure log analytics' limit

  override def send(events: Seq[A]): Unit = {
    events.grouped(batchSize).foreach { eventGroup =>
      val body = "[" + eventGroup.map(serialize).mkString(",") + "]"
      client.send(body, logType)
    }
  }
}

/**
 * LogAnalytics Ingestion API backend implementation
 *
 * @param endpoint   Azure Monitor Data Collector Endpoint URI
 * @param ruleId     Azure Monitor Data Collection Rule ID
 * @param streamName LogAnalytics stream name configured in Data Collection Rule.
 * @param serialize  a function to serialize the Log object to a Json String.
 * @param credential optional TokenCredential object to authenticate against Azure AD. Default is to use DefaultAzureCredentialBuilder.
 */
class LogAnalyticsIngestionBackend[A: ClassTag](endpoint: String, ruleId: String, streamName: String, override val batchSize: Int, serialize: A => String, credential: Option[TokenCredential] = None) extends LogAnalyticsBackend[A] {
  private val credentialPrep = credential.getOrElse(new DefaultAzureCredentialBuilder().build())
  private val client = new LogsIngestionClientBuilder().endpoint(endpoint).credential(credentialPrep).buildClient()
  private val uploadOptions = new LogsUploadOptions()
    .setLogsUploadErrorConsumer(err => System.err.println("ERROR LogAnalyticsIngestionBackend: " + err.getResponseException)) // don't log this to avoid loops
    .setObjectSerializer(new Log4jLayoutSerializer[A](serialize))

  override def send(events: Seq[A]): Unit = {
    events.grouped(batchSize).foreach { eventGroup =>
      client.upload(ruleId, streamName, eventGroup.asInstanceOf[Seq[Object]].asJava, uploadOptions)
    }
  }
}

/**
 * Implementation of Serializer for LogsIngestionClient using a Log4j2 layout.
 */
class Log4jLayoutSerializer[A: ClassTag](serialize: A => String) extends ObjectSerializer {
  private val classNameA = classTag[A].runtimeClass.getSimpleName

  override def deserialize[T](stream: InputStream, typeReference: TypeReference[T]): T = {
    throw new NotImplementedError("deserialize is not implemented")
  }

  override def deserializeAsync[T](stream: InputStream, typeReference: TypeReference[T]): Mono[T] = {
    Mono.fromCallable(() => deserialize(stream, typeReference))
  }

  override def serialize(stream: OutputStream, value: Any): Unit = {
    value match {
      case event: A => stream.write(serialize(event).getBytes)
      case x => throw new IllegalStateException(s"value must be instance of $classNameA, but is a ${x.getClass.getSimpleName}")
    }
  }

  override def serializeAsync(stream: OutputStream, value: Any): Mono[Void] = {
    Mono.fromCallable(() => serialize(stream, value).asInstanceOf[Void])
  }
}
