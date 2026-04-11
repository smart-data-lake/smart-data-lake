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
package io.smartdatalake.workflow.connection

import com.google.cloud.spark.bigquery.repackaged.com.google.auth.oauth2.ServiceAccountCredentials
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.connection.authMode.{AuthMode, GCPCredentialsKeyAuth}
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.{BigQuery, BigQueryOptions}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.io.Source

/**
 * Connection information for GCP BigQuery Tables
 *
 * @param id unique id of this connection
 * @param parentProject project used for this connection. It is defined as the GCP Project ID of the table
 *                      to bull for the export. It defaults to the project of the Service Account being used
 *                      in the credentials.
 * @param authMode Credentials to be used for this connection using [[GCPCredentialsKeyAuth]]
 *                 As of now, only Service Account keys (either as encoded Strings or as json-files)
 *                 are supported as an authentication method.
 * @param metadata
 */
case class BigQueryTableConnection(override val id: ConnectionId,
                                   val authMode: AuthMode,
                                   val parentProject: String,
                                   override val metadata: Option[ConnectionMetadata] = None
                                  ) extends Connection with SmartDataLakeLogger {

  private val supportedAuths = Seq(classOf[GCPCredentialsKeyAuth])
  require(supportedAuths.contains(authMode.getClass), s"($id) ${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}.")
  val credentialsOptions: (String, String) = {
    authMode match {
      case g: GCPCredentialsKeyAuth => g.getAuthCredentials()
      case _ => throw new IllegalArgumentException(s"($id) No supported authMode given for Google BigQuery connection.")
    }
  }

  def getConnectionOptions(): Map[String, String] = Map(credentialsOptions) + ("parentProject" -> parentProject)

  private def getBigQueryObject: BigQuery = {
    val credentialsByteArray: Array[Byte] = credentialsOptions._1 match {
      case "credentials" => Base64.getDecoder.decode(credentialsOptions._2)
      case "credentialsFile" => Source.fromFile(credentialsOptions._2).mkString.getBytes(StandardCharsets.UTF_8)
      case _ => throw new IllegalStateException(f"BigQuery Authentication is not working properly as it is sending a credential String of ${credentialsOptions._1}")
    }
    val credentials: ServiceAccountCredentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(credentialsByteArray))
    BigQueryOptions.newBuilder().setCredentials(credentials).build().getService()
  }

  //evaluate lazily (only once per connection) to let prepare() catch configuration errors
  lazy val bigQueryObject = getBigQueryObject


  override def factory: FromConfigFactory[Connection] = BigQueryTableConnection

}

object BigQueryTableConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): BigQueryTableConnection = {
    extract[BigQueryTableConnection](config)
  }
}