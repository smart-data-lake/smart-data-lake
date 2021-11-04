package io.smartdatalake.util.azure

import com.microsoft.aad.msal4j.{ClientCredentialFactory, ClientCredentialParameters, ConfidentialClientApplication}
import io.smartdatalake.definitions.CustomHttpAuthModeLogic
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.SecretsUtil

import java.util.Collections

class AzureADClientGrantAuthMode extends CustomHttpAuthModeLogic with SmartDataLakeLogger {
  var authority: String = _
  var applicationId: String = _
  var clientSecret: String = _
  var scope: String = _
  override def prepare(options: Map[String, String]): Unit = {
    authority = options("authority")
    applicationId = SecretsUtil.getSecret(options("applicationId"))
    clientSecret = SecretsUtil.getSecret(options("clientSecret"))
    scope = options("scope")
  }
  override def getHeaders: Map[String,String] = {
    logger.info(s"getting token from $authority")
    // building Azure AD client
    val app = ConfidentialClientApplication.builder(applicationId, ClientCredentialFactory.createFromSecret(clientSecret))
      .authority(authority)
      .build();
    val clientCredentialParam = ClientCredentialParameters.builder(Collections.singleton(scope))
      .build();
    // get token
    val future = app.acquireToken(clientCredentialParam)
    val token = future.get.accessToken()
    logger.info(s"got token for $scope")
    // return as header
    Map("Authorization" -> s"Bearer $token")
  }
}
