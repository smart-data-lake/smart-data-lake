/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2025 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.connection.authMode

import com.typesafe.config.Config
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.secrets.StringOrSecret

/**
 * Authorization parameters to access a GCP-ressource using Service Account Keys.
 * A Service Account Key can be either read as Base64-encoded JSON file (String),
 * or as a path containing the JSON-File with the key, as described by Google. Only one of the options should be provided.
 * @param serviceAccountKey A String that represents the Service Account Key encoded in Base64.
 * @param serviceAccountKeyFile A path that represents the location of the JSON-file with the service account key.
 */
case class GCPCredentialsKeyAuth(private val serviceAccountKey: Option[StringOrSecret],
                                 private val serviceAccountKeyFile: Option[StringOrSecret]) extends AuthMode {

  override def prepare(): Unit = {
    (serviceAccountKey, serviceAccountKeyFile) match {
      case (None, None) => throw ConfigurationException("Either a serviceAccountKey or a serviceAccountKeyFile must be defined for the GCP-authorization")
      case (Some(_), Some(_)) => throw ConfigurationException("Either a serviceAccountKey or a serviceAccountKeyFile must be defined for the GCP-authorization, but not both!")
      case _ =>
    }
  }

  private[smartdatalake] def getAuthCredentials(): (String, String) = {
    if (serviceAccountKey.isDefined) ("credentials", serviceAccountKey.get.resolve()) else ("credentialsFile", serviceAccountKeyFile.get.resolve())
  }

  override def factory: FromConfigFactory[AuthMode] = GCPCredentialsKeyAuth
}

object GCPCredentialsKeyAuth extends FromConfigFactory[AuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): GCPCredentialsKeyAuth = {
    extract[GCPCredentialsKeyAuth](config)
  }
}
