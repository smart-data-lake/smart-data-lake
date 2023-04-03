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
package io.smartdatalake.definitions

import io.smartdatalake.util.misc.CustomCodeUtil
import io.smartdatalake.util.secrets.{SecretsUtil, StringOrSecret}
import io.smartdatalake.util.webservice.KeycloakUtil
import org.keycloak.admin.client.{Keycloak, KeycloakBuilder}

/**
 * Authentication modes define how an application authenticates itself
 * to a given data object/connection
 *
 * You need to define one of the AuthModes (subclasses) as type, i.e.
 * {{{
 * authMode {
 *   type = BasicAuthMode
 *   user = myUser
 *   password = myPassword
 * }
 * }}}
 *
 */
sealed trait AuthMode {
  /**
   * This method is called in prepare phase through the data object.
   * It allows the check configuration and setup variables.
   */
  private[smartdatalake] def prepare(): Unit = Unit

  /**
   * This method is called after exec phase throught the postExec method of the data object.
   * It allows to release any resources that were reserved.
   */
  private[smartdatalake] def close(): Unit = Unit
}

/**
 * Connect by basic authentication
 */
case class BasicAuthMode(private val user: Option[StringOrSecret],
                         private val password: Option[StringOrSecret],
                         @Deprecated @deprecated("Use `user` instead", "2.5.0") private val userVariable: Option[String] = None,
                         @Deprecated @deprecated("Use `password` instead", "2.5.0") private val passwordVariable: Option[String] = None) extends AuthMode {
  private val _user: StringOrSecret = user.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(userVariable.get))
  private val _password: StringOrSecret = password.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(passwordVariable.get))

  def userSecret: StringOrSecret = _user
  def passwordSecret: StringOrSecret = _password
}

/**
 * Connect by token
 * For HTTP Connection this is used as Bearer token in Authorization header.
 */
case class TokenAuthMode(@Deprecated @deprecated("Use `token` instead", "2.5.0") private val tokenVariable: Option[String] = None,
                         private val token: Option[StringOrSecret]) extends AuthMode with HttpHeaderAuth {
  private val _token =  token.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(tokenVariable.get))

  private[smartdatalake] val tokenSecret: StringOrSecret = _token
  private[smartdatalake] override def getHeaders: Map[String, String] = {
    Map("Authorization" -> s"Bearer ${tokenSecret.resolve()}")
  }
}


/**
 * Connect by custom authorization header
 */
case class AuthHeaderMode(
                           headerName: String = "Authorization",
                           private val secret: Option[StringOrSecret],
                           @Deprecated @deprecated("Use `secret` instead", "2.5.0") private val secretVariable: Option[String] = None
                         ) extends AuthMode with HttpHeaderAuth {
  private val _secret = secret.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(secretVariable.get))
  private[smartdatalake] val stringOrSecret: StringOrSecret = _secret
  private[smartdatalake] override def getHeaders: Map[String,String] = Map(headerName -> stringOrSecret.resolve())
}

/**
 * Connect by using Keycloak to manage token and token refresh giving clientId/secret as information.
 * For HTTP Connection this is used as Bearer token in Authorization header.
 */
case class KeycloakClientSecretAuthMode(
                                         ssoServer: String,
                                         ssoRealm: String,
                                         ssoGrantType: String,
                                         @Deprecated @deprecated("Use `clientId` instead", "2.5.0") private val clientIdVariable: Option[String] = None,
                                         private val clientId: Option[StringOrSecret],
                                         @Deprecated @deprecated("Use `clientSecret` instead", "2.5.0") private val clientSecretVariable: Option[String] = None,
                                         private val clientSecret: Option[StringOrSecret],
                                       ) extends AuthMode with HttpHeaderAuth {

  private val _clientId: StringOrSecret = clientId.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(clientIdVariable.get))
  private val _clientSecret: StringOrSecret = clientSecret.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(clientSecretVariable.get))

  private var keycloakClient: Keycloak = _
  private[smartdatalake] override def prepare(): Unit = {
    if (keycloakClient==null) {
      keycloakClient = KeycloakBuilder
        .builder
        .serverUrl(ssoServer)
        .realm(ssoRealm)
        .grantType(ssoGrantType)
        .clientId(_clientId.resolve())
        .clientSecret(_clientSecret.resolve())
        .build
    }
    // check connection
    keycloakClient.tokenManager().getAccessToken.getToken
  }
  private[smartdatalake] override def getHeaders: Map[String,String] = {
    assert(keycloakClient!=null, "keycloak client not initialized")
    val token = keycloakClient.tokenManager.getAccessToken.getToken
    Map("Authorization" -> s"Bearer $token")
  }
  private[smartdatalake] override def close(): Unit = {
    if (keycloakClient!=null) {
      KeycloakUtil.logout(ssoServer, ssoRealm, _clientId.resolve(), _clientSecret.resolve(), keycloakClient)
      keycloakClient.close
      keycloakClient = null
    }
  }
}

/**
 * Connect with custom HTTP authentication
 * @param className class name implementing trait [[CustomHttpAuthModeLogic]]
 * @param options Options to pass to the custom auth mode logic in prepare function.
 */
case class CustomHttpAuthMode(className: String, options: Map[String,StringOrSecret]) extends AuthMode with HttpHeaderAuth {
  private val impl = CustomCodeUtil.getClassInstanceByName[CustomHttpAuthModeLogic](className)
  private[smartdatalake] override def prepare(): Unit = impl.prepare(options)
  private[smartdatalake] override def getHeaders: Map[String, String] = impl.getHeaders
}
trait CustomHttpAuthModeLogic extends HttpHeaderAuth {
  def prepare(options: Map[String,StringOrSecret]): Unit = Unit
}

/**
 * Validate by user and private/public key
 * Private key is read from .ssh
 */
case class PublicKeyAuthMode(@Deprecated @deprecated("Use `user` instead", "2.5.0") private val userVariable: Option[String] = None,
                             private val user: Option[StringOrSecret]) extends AuthMode {
  private val _user: StringOrSecret = user.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(userVariable.get))
  private[smartdatalake] val userSecret: StringOrSecret = _user
}

/**
 * Validate by SSL Certificates : Only location an credentials. Additional attributes should be
 * supplied via options map
 */
case class SSLCertsAuthMode (
                            keystorePath: String,
                            keystoreType: Option[String],
                            @Deprecated @deprecated("Use `keystorePass` instead", "2.5.0") private val keystorePassVariable: Option[String] = None,
                            private val keystorePass: Option[StringOrSecret],
                            truststorePath: String,
                            truststoreType: Option[String],
                            @Deprecated @deprecated("Use `truststorePass` instead", "2.5.0") private val truststorePassVariable: Option[String] = None,
                            private val truststorePass: Option[StringOrSecret]
                           ) extends AuthMode {
  private val _keystorePass = keystorePass.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(keystorePassVariable.get))
  private val _truststorePass = truststorePass.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(truststorePassVariable.get))

  private[smartdatalake] val truststorePassSecret: StringOrSecret = _keystorePass
  private[smartdatalake] val keystorePassSecret: StringOrSecret = _truststorePass
}

/**
 * Validate by SASL_SSL Authentication : user / password and truststore
 */
case class SASLSCRAMAuthMode (
                                 username: String,
                                 @Deprecated @deprecated("Use `password` instead", "2.5.0") private val passwordVariable: Option[String] = None,
                                 private val password: Option[StringOrSecret],
                                 sslMechanism: String,
                                 truststorePath: String,
                                 truststoreType: Option[String],
                                 @Deprecated @deprecated("Use `truststorePass` instead", "2.5.0") private val truststorePassVariable: Option[String] = None,
                                 private val truststorePass: Option[StringOrSecret],
                               ) extends AuthMode {
  private val _password: StringOrSecret = password.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(passwordVariable.get))
  private val _truststorePass = truststorePass.getOrElse(SecretsUtil.convertSecretVariableToStringOrSecret(truststorePassVariable.get))

  private[smartdatalake] val passwordSecret: StringOrSecret = _password
  private[smartdatalake] val truststorePassSecret: StringOrSecret  = _truststorePass
}


/**
 * Interface to generalize authentication for HTTP requests
 */
private[smartdatalake] trait HttpHeaderAuth {
  /**
   * Return additional headers to add/overwrite in http request.
   */
  private[smartdatalake] def getHeaders: Map[String,String] = Map()
}