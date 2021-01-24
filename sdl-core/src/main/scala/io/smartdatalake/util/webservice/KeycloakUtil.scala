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
package io.smartdatalake.util.webservice

import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.keycloak.admin.client.{Keycloak, KeycloakBuilder}
import scalaj.http.Http

import scala.util.{Failure, Success, Try}

private[smartdatalake] object KeycloakUtil extends SmartDataLakeLogger {

  /**
   * * Method used to invalidate the refresh token.
   * * It's good practice to call this method when you're done. The token won't be used anymore
   * * (we don't save it), so there's no need for the server to keep this session open.
   */
  def logout(ssoServer: String, ssoRealm: String, clientId: String, clientSecret: String, keycloak: Keycloak): Unit = {
    val logoutUrl: String = ssoServer +"/realms/" +ssoRealm+"/protocol/openid-connect/logout?"
    logger.debug(s"Calling logout url: $logoutUrl")
    val refreshToken = keycloak.tokenManager().refreshToken().getRefreshToken
    logger.debug(s"Got refresh Token from keycloak: $refreshToken")

    // Form basic HTTP Call to logout
    val request = Http(logoutUrl)
      .postForm
      .param("client_id", clientId)
      .param("client_secret", clientSecret)
      .param("refresh_token", refreshToken)

    // execute http request and parse response
    Try(request.asString) match {
      case Success(httpResponse) =>
        if(httpResponse.code == 204)
          logger.info("Keycloak successfull with code 204")
        else
          logger.info(s"Keycloak did not logout successfully, return code was ${httpResponse.code} instead of 204.")
      case Failure(exception) => throw new WebserviceException(s"Keycloak logout request call failed with $exception")
    }
  }
}
