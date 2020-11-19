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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpResponse}

import scala.util.{Failure, Success, Try}

/**
 *  Simple client to call webservices
 *
 *  Requests are sent with [[HttpRequest]]
 *  See https://github.com/scalaj/scalaj-http
 *
 *  Currently, only GET requests are supported, the response is returned as String.
 *  Proper handling of response it done by caller.
 *
 *  TODO: More request methods (PUT, POST, ...)
 *         Request parameter and body
 *
 */
private[smartdatalake] class DefaultWebserviceClient(httpRequest: HttpRequest) extends Webservice with SmartDataLakeLogger {

  /**
    * Sends a GET request to a webservice
    *
    * @return Success(with response as Array[Byte]) or Failure(with reason of failure)
    */
  override def get(): Try[Array[Byte]] = {
    Try(httpRequest.asBytes) match {
      case Success(httpResponse) => checkResponse(httpResponse)
      case Failure(exception) => Failure(exception)
    }
  }

  /**
   * Sends a POST request with a body in the given mimetype
   * @param body body
   * @param mimeType mimetype to use for the body
   * @return
   */
  override def post(body: Array[Byte], mimeType: String): Try[Array[Byte]] = {
    logger.info("Sending POST request with content-type: " +mimeType)
     Try(httpRequest.method("POST").header("content-type",mimeType).postData(body).asBytes) match {
       case Success(httpResponse) => checkResponse(httpResponse)
       case Failure(exception) => Failure(exception)
     }
  }

  /**
   *
   * Check response of webservice call
   *
   * If a request was sent and a response was received, check:
   * 1. Response has an error status: Failure[WebserviceException] is returned
   * 2. Response has a code 200: Return body as Success[String]
   *
   * @param httpResponse Response of webservice call or Failure
   * @return Success[Array[Byte]] or Failure[WebserviceException]
   */
  def checkResponse(httpResponse: HttpResponse[Array[Byte]]): Try[Array[Byte]] = {
    httpResponse match {
      // Request was sent, but the response contains an error
      case errorResponse if errorResponse.isError =>
        logger.error(s"Error when calling ${httpRequest.url}: Http status code: ${errorResponse.code}, response body: ${new String(errorResponse.body).take(200)}...")
        Failure(new WebserviceException(s"Webservice Request failed with error <${httpResponse.code}>"))
      // Request was successfull and response can be processed further
      case normalResponse if normalResponse.isSuccess => Success(normalResponse.body)
    }
  }

}

private[smartdatalake] object DefaultWebserviceClient extends SmartDataLakeLogger {

  private val AuthorizationHeaderName = "Authorization"

  /**
    * Creates a [[scalaj.http.HttpRequest]] with user and password credentials (basic authentication)
    *
    * @param url      Url to call
    * @param user     username for authentication (always use together with password)
    * @param password password for authentication (always use together with user)
    * @return [[scalaj.http.HttpRequest]]
    */
  def buildRequestWithCredentials(url: String, user: String, password: String): HttpRequest = {
    logger.debug(s"Building HttpRequest with url: <$url>, user:<$user>, password:<...>")
    buildRequest(url).auth(user, password)
  }


  /**
   * Creates a [[scalaj.http.HttpRequest]] with a token
   *
   * Uses the given token to authenticate
   * Header: Authentication = Bearer <token>
   *
   * @param url      Url to call
   * @param token    Token to use for authentication
   * @return [[scalaj.http.HttpRequest]]
   */
  def buildRequestWithToken(url: String, token: String): HttpRequest = {
    logger.debug(s"Building HttpRequest with url: <$url>, token:<${token.substring(0, 10)}...>")
    buildRequest(url).header(AuthorizationHeaderName, "Bearer "+token)
  }

  /**
   * Creates a [[scalaj.http.HttpRequest]] with a manually overwritten authentication header
   *
   * @param url                 Url to call
   * @param authorizationHeader Authentication header used instead of user/password
   * @return [[scalaj.http.HttpRequest]]
   */
  def buildRequestWithAuthorizationHeader(url: String, authorizationHeader: String): HttpRequest = {
    logger.debug(s"Building HttpRequest with url: <$url>, authorizationHeader:<${authorizationHeader.substring(0, 4)}...>")
    buildRequest(url).header(AuthorizationHeaderName, authorizationHeader)
  }

  /**
   * Creates a [[scalaj.http.HttpRequest]] with client-id and client-secret
   *
   * Usually used to get token in OAuth2
    *
    * @param url           Url to call
    * @param clientId      client-id for token refresh
    * @param clientSecret  client-secret for token refresh
    * @return [[scalaj.http.HttpRequest]]
    */
  def buildRequestWithClientId(url: String, clientId: String, clientSecret: String): HttpRequest = {
    logger.debug(s"Building HttpRequest with url: <$url>, client_id:<$clientId>, client_secret:<...hidden...>")
    buildRequest(url)
      .postForm(Seq("grant_type"->"client_credentials","client_id"->clientId,"client_secret"->clientSecret))
  }

  /**
    * Creates a simple [[scalaj.http.HttpRequest]] without authentication
    *
    * @param url Url to call
    * @return [[scalaj.http.HttpRequest]]
    */
  def buildRequest(url: String): HttpRequest = {
    logger.debug(s"Building HttpRequest with url: <$url>")
    Http(url)
      .option(HttpOptions.followRedirects(true))
  }

  /**
   * Creates a [[scalaj.http.HttpRequest]] with given options to authenticate
   * Possible options are User/Pass, Token (OAuth2), client-id/client-secret (to refresh token in OAuth2)
   * or manually defined authentication headers
   *
   * NOte that you usually don't refresh the token yourself, you let Keycloak handle it for you.
   *
   * @param url Url to Call
   * @param authOptions Authentication options as [[Map]]
   * @return [[HttpRequest]]
   */
  def buildRequest(url: String, authOptions: Map[String, String]): HttpRequest = {
    logger.debug(s"Building HttpRequest with url: <$url> and AuthOptions <$authOptions>.")

    if(authOptions.contains("auth-header"))
      buildRequestWithAuthorizationHeader(url, authOptions.getOrElse("auth-header", ""))
    else if(authOptions.contains("client-id") && authOptions.contains("client-secret"))
      buildRequestWithClientId(url, authOptions.getOrElse("client-id", ""), authOptions.getOrElse("client-secret",""))
    else if (authOptions.contains("user") && authOptions.contains("password"))
      buildRequestWithCredentials(url, authOptions.getOrElse("user",""), authOptions.getOrElse("password",""))
    else if (authOptions.contains("token"))
      buildRequestWithToken(url, authOptions.getOrElse("token",""))
    else
      buildRequest(url)
  }

  def apply(url: Option[String]): DefaultWebserviceClient = {
    url match {
      case Some(definedUrl) => new DefaultWebserviceClient(buildRequest(definedUrl))
      case _ => throw new ConfigurationException("fehlender Parameter: Url")
    }
  }


  def apply(url: Option[String], authOptions: Option[Map[String,String]], connTimeoutMs: Option[Int], readTimeoutMs: Option[Int]) : DefaultWebserviceClient = {
    (url, authOptions, connTimeoutMs, readTimeoutMs) match {
      case (None, Some(definedAuthOptions), _ ,_ ) => throw new ConfigurationException("fehlende Parameter: Url")
      case (None, None, _ , _ ) => throw new ConfigurationException("fehlende Parameter: Url / AuthOptions")
      case (Some(definedUrl), None, _, _) => new DefaultWebserviceClient(buildRequest(definedUrl))
      case (Some(definedUrl), Some(definedAuthOptions), Some(definedConnTimeoutMs), Some(definedReadTimeoutMs)) =>
        if(
          definedAuthOptions.contains("auth-header") ||
          (definedAuthOptions.contains("client-id") && definedAuthOptions.contains("client-secret")) ||
          (definedAuthOptions.contains("user") && definedAuthOptions.contains("password")) ||
            definedAuthOptions.contains("token")
        ) new DefaultWebserviceClient(buildRequest(definedUrl, definedAuthOptions).timeout(definedConnTimeoutMs, definedReadTimeoutMs))
        else throw new ConfigurationException("falsche Parameter: AuthOptions")
      case (Some(definedUrl), Some(definedAuthOptions), None, None) =>
        if(
          definedAuthOptions.contains("auth-header") ||
            (definedAuthOptions.contains("client-id") && definedAuthOptions.contains("client-secret")) ||
            (definedAuthOptions.contains("user") && definedAuthOptions.contains("password")) ||
            definedAuthOptions.contains("token")
        ) new DefaultWebserviceClient(buildRequest(definedUrl, definedAuthOptions))
        else throw new ConfigurationException("falsche Parameter: AuthOptions")
        new DefaultWebserviceClient(buildRequest(definedUrl, definedAuthOptions))
      case _ => throw new ConfigurationException("fehlende Parameter: Url / AuthOptions")
    }
  }
}
