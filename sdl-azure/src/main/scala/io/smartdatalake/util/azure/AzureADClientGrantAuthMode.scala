package io.smartdatalake.util.azure

import com.microsoft.aad.msal4j.{ClientCredentialFactory, ClientCredentialParameters, ConfidentialClientApplication}
import io.smartdatalake.definitions.CustomHttpAuthModeLogic
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret

import java.util.Collections

class AzureADClientGrantAuthMode extends CustomHttpAuthModeLogic with SmartDataLakeLogger {
  var authority: StringOrSecret = _
  var applicationId: StringOrSecret = _
  var clientSecret: StringOrSecret = _
  var scope: StringOrSecret = _
  override def prepare(options: Map[String, StringOrSecret]): Unit = {
    authority = options("authority")
    applicationId = options("applicationId")
    clientSecret = options("clientSecret")
    scope = options("scope")
  }
  override def getHeaders: Map[String,String] = {
    logger.info(s"getting token from $authority")
    // building Azure AD client
    val app = ConfidentialClientApplication.builder(applicationId.resolve(), ClientCredentialFactory.createFromSecret(clientSecret.resolve()))
      .authority(authority.resolve())
      .build();
    val clientCredentialParam = ClientCredentialParameters.builder(Collections.singleton(scope.resolve()))
      .build();
    // get token
    val future = app.acquireToken(clientCredentialParam)
    val token = future.get.accessToken()
    logger.info(s"got token for $scope")
    // return as header
    Map("Authorization" -> s"Bearer $token")
  }
}
