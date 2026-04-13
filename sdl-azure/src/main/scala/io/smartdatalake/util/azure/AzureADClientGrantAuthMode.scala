/*
 * sdl-azure - Build your data lake the smart way.
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
package io.smartdatalake.util.azure

import com.microsoft.aad.msal4j.{ClientCredentialFactory, ClientCredentialParameters, ConfidentialClientApplication}
import com.typesafe.config.Config
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.connection.authMode.HttpAuthMode

import java.util.Collections

case class AzureADClientGrantAuthMode(authority: StringOrSecret, applicationId: StringOrSecret, clientSecret: StringOrSecret, scope: StringOrSecret) extends HttpAuthMode with SmartDataLakeLogger {

  override def getHeaders: Map[String,String] = {
    logger.info(s"getting token from $authority")
    // building Azure AD client
    val app = ConfidentialClientApplication.builder(applicationId.resolve(), ClientCredentialFactory.createFromSecret(clientSecret.resolve()))
      .authority(authority.resolve())
      .build()
    val clientCredentialParam = ClientCredentialParameters.builder(Collections.singleton(scope.resolve()))
      .build()
    // get token
    val future = app.acquireToken(clientCredentialParam)
    val token = future.get.accessToken()
    logger.info(s"got token for $scope")
    // return as header
    Map("Authorization" -> s"Bearer $token")
  }

  override def factory: FromConfigFactory[HttpAuthMode] = AzureADClientGrantAuthMode

}

object AzureADClientGrantAuthMode extends FromConfigFactory[HttpAuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AzureADClientGrantAuthMode = {
    extract[AzureADClientGrantAuthMode](config)
  }
}