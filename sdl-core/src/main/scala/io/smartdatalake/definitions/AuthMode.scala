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

import io.smartdatalake.util.misc.{CredentialsUtil, CustomCodeUtil}
import io.smartdatalake.util.webservice.KeycloakUtil
import io.smartdatalake.workflow.action.customlogic.CustomDfsTransformer
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
case class BasicAuthMode(userVariable: String, passwordVariable: String) extends AuthMode {
  private[smartdatalake] val user: String = CredentialsUtil.getCredentials(userVariable)
  private[smartdatalake] val password: String = CredentialsUtil.getCredentials(passwordVariable)
}

/**
 * Connect by token
 * For HTTP Connection this is used as Bearer token in Authorization header.
 */
case class TokenAuthMode(tokenVariable: String) extends AuthMode with HttpHeaderAuth {
  private[smartdatalake] val token: String = CredentialsUtil.getCredentials(tokenVariable)
  private[smartdatalake] override def getHeaders: Map[String,String] = {
    Map("Authorization" -> s"Bearer $token")
  }
}


/**
 * Connect by custom authorization header
 */
case class AuthHeaderMode(headerName: String = "Authorization", secretVariable: String) extends AuthMode with HttpHeaderAuth {
  private[smartdatalake] val secret: String = CredentialsUtil.getCredentials(secretVariable)
  private[smartdatalake] override def getHeaders: Map[String,String] = Map(headerName -> secret)
}

/**
 * Connect by using Keycloak to manage token and token refresh giving clientId/secret as information.
 * For HTTP Connection this is used as Bearer token in Authorization header.
 */
case class KeycloakClientSecretAuthMode(ssoServer: String, ssoRealm: String, ssoGrantType: String, clientIdVariable: String, clientSecretVariable: String) extends AuthMode with HttpHeaderAuth {
  private[smartdatalake] val clientId: String = CredentialsUtil.getCredentials(clientIdVariable)
  private[smartdatalake] val clientSecret: String = CredentialsUtil.getCredentials(clientSecretVariable)
  private var keycloakClient: Keycloak = _
  private[smartdatalake] override def prepare(): Unit = {
    if (keycloakClient==null) {
      keycloakClient = KeycloakBuilder
        .builder
        .serverUrl(ssoServer)
        .realm(ssoRealm)
        .grantType(ssoGrantType)
        .clientId(clientId)
        .clientSecret(clientSecret)
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
      KeycloakUtil.logout(ssoServer, ssoRealm, clientId, clientSecret, keycloakClient)
      keycloakClient.close
      keycloakClient = null
    }
  }
}

/**
 * Connect with custom HTTP authentication
 * @param className class name implementing trait [[CustomHttpAuthModeLogic]]
 * @param options Options to pass to the custom auth mode logc in prepare function
 */
case class CustomHttpAuthMode(className: String, options: Map[String,String]) extends AuthMode with HttpHeaderAuth {
  private val impl = CustomCodeUtil.getClassInstanceByName[CustomHttpAuthModeLogic](className)
  private[smartdatalake] override def prepare(): Unit = impl.prepare(options)
  private[smartdatalake] override def getHeaders: Map[String, String] = impl.getHeaders
}
trait CustomHttpAuthModeLogic extends HttpHeaderAuth {
  def prepare(options: Map[String,String]): Unit = Unit
}

/**
 * Validate by user and private/public key
 * Private key is read from .ssh
 */
case class PublicKeyAuthMode(userVariable: String) extends AuthMode {
  private[smartdatalake] val user: String = CredentialsUtil.getCredentials(userVariable)
}

/**
 * Validate by SSL Certificates : Only location an credentials. Additional attributes should be
 * supplied via options map
 */
case class SSLCertsAuthMode (
                            keystorePath: String,
                            keystoreType: Option[String],
                            keystorePassVariable: String,
                            truststorePath: String,
                            truststoreType: Option[String],
                            truststorePassVariable: String
                           ) extends AuthMode {
  private[smartdatalake] val truststorePass: String = CredentialsUtil.getCredentials(truststorePassVariable)
  private[smartdatalake] val keystorePass: String = CredentialsUtil.getCredentials(keystorePassVariable)
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