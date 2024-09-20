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