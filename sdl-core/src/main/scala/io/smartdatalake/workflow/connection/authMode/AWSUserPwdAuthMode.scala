/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.webservice.{OAuth2Response, OAuth2Service}
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import sttp.client3._
import sttp.model.{Header, MediaType, Uri}


/**
 * AWS User/Pwd Authentication.
 *
 * This first calls AWS Cognito InitiateAuth endpoint from the selected region to authenticate the user.
 * The tokens returned can then be refreshed with the corresponding token endpoint of the user pool.
 *
 * @param region     AWS region
 * @param clientId   client id of the AWS application
 * @param user       user to login
 * @param password   password to use for login.
 * @param useIdToken If true, id_token is used for Http Authorization header, otherwise access_token. Default is false.
 */
case class AWSUserPwdAuthMode(region: String, userPool: String, clientId: StringOrSecret, user: StringOrSecret, password: StringOrSecret, useIdToken: Boolean = false) extends HttpAuthMode with SmartDataLakeLogger {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  val cognitoUrl = s"https://cognito-idp.$region.amazonaws.com"
  val userPoolTokenUrl = s"https://$userPool.auth.$region.amazoncognito.com/oauth2/token"

  private lazy val oAuth2Service = OAuth2Service(userPoolTokenUrl, Some(clientId.resolve()), awsInitiateAuth)

  override def prepare(): Unit = {
    // initialize oAuth2Service
    oAuth2Service
  }

  /**
   * See also https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_InitiateAuth.html
   *
   * @return an OAuth2 response. Note that InitiateAuth response is not in OAuth2 format, but can be mapped to OAuth2Response.
   */
  private def awsInitiateAuth(): OAuth2Response = {
    logger.info(s"Authenticating with AWS Cognito $region")

    val request = basicRequest
      .post(Uri.unsafeParse(cognitoUrl))
      .header("X-Amz-Target", "AWSCognitoIdentityProviderService.InitiateAuth")
      .header("Content-Type", "application/x-amz-json-1.1")
      .header(Header.accept(MediaType.ApplicationJson))
      .followRedirects(true)
      .body(Serialization.write(
        Map(
          "ClientId" -> clientId.resolve(),
          "AuthFlow" -> "USER_PASSWORD_AUTH",
          "AuthParameters" -> Map("USERNAME" -> user.resolve(), "PASSWORD" -> password.resolve())
        )
      ))
    val response = parse(OAuth2Service.sendRequest(request, "AWS initiate auth")).extract[AWSAuthResponse]

    // map to OAuth2 response
    response.toOAuth2Response
  }

  override def getHeaders: Map[String, String] = {
    Some(oAuth2Service.getToken.getAuthHeader(useIdToken)).toMap
  }

  override def factory: FromConfigFactory[HttpAuthMode] = AWSUserPwdAuthMode

}

object AWSUserPwdAuthMode extends FromConfigFactory[HttpAuthMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): AWSUserPwdAuthMode = {
    extract[AWSUserPwdAuthMode](config)
  }
}


private case class AWSAuthResult(AccessToken: String, Scope: Option[String], TokenType: String, RefreshToken: Option[String], ExpiresIn: Int, IdToken: Option[String])

private case class AWSAuthResponse(AuthenticationResult: AWSAuthResult, ChallengeParameters: Map[String, String]) {
  def toOAuth2Response: OAuth2Response = {
    OAuth2Response(
      access_token = AuthenticationResult.AccessToken,
      refresh_token = AuthenticationResult.RefreshToken,
      id_token = AuthenticationResult.IdToken,
      expires_in = AuthenticationResult.ExpiresIn,
      token_type = AuthenticationResult.TokenType
    )
  }
}
