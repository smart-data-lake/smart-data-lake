/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{AuthMode, GCPCredentialsKeyAuth}
import io.smartdatalake.util.misc.{AclDef, SmartDataLakeLogger}

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
                                   authMode: AuthMode,
                                   parentProject: Option[String],
                                   override val metadata: Option[ConnectionMetadata] = None
                                   ) extends Connection with SmartDataLakeLogger {

  private val supportedAuths = Seq(classOf[GCPCredentialsKeyAuth])
  require(supportedAuths.contains(authMode.getClass), s"($id) ${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}.")
  def getCredentialsOptions(): (String, String) = {
    authMode match {
      case g: GCPCredentialsKeyAuth => g.getAuthCredentials()
      case _ => throw new IllegalArgumentException(s"($id) No supported authMode given for Snowflake connection.")
    }
  }

  def getBigQueryConnectionOptions(): Map[String, String] = {
    val projectOptions =  if (parentProject.isDefined) Map("parentProject" -> parentProject.get) else Map()
    Map(getCredentialsOptions) ++ projectOptions
  }


  override def factory: FromConfigFactory[Connection] = BigQueryTableConnection

}

object BigQueryTableConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): BigQueryTableConnection = {
    extract[BigQueryTableConnection](config)
  }
}
