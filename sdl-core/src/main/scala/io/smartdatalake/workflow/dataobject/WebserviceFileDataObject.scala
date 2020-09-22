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
package io.smartdatalake.workflow.dataobject

import java.io.{ByteArrayInputStream, InputStream}

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{CredentialsUtil, SmartDataLakeLogger}
import io.smartdatalake.util.webservice._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.keycloak.admin.client.Keycloak
import org.keycloak.representations.AccessTokenResponse

import scala.util.{Failure, Success, Try}

case class WebservicePartitionDefinition( name: String, values: Seq[String])

/**
 * [[DataObject]] to call webservice and return response as InputStream
 * This is implemented as FileRefDataObject because the response is treated as some file content.
 * FileRefDataObjects support partitioned data. For a WebserviceFileDataObject partitions are mapped as query parameters to create query string.
 * All possible query parameter values must be given in configuration.
 * @param partitionDefs list of partitions with list of possible values for every entry
 * @param partitionLayout definition of partitions in query string. Use %<partitionColName>% as placeholder for partition column value in layout.
 */
case class WebserviceFileDataObject(override val id: DataObjectId,
                                    webserviceOptions: WebserviceOptions,
                                    partitionDefs: Seq[WebservicePartitionDefinition] = Seq(),
                                    override val partitionLayout: Option[String] = None,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends FileRefDataObject with CanCreateInputStream with SmartDataLakeLogger {

  private val webServiceClientId = webserviceOptions.clientIdVariable.map(CredentialsUtil.getCredentials)
  private val webServiceClientSecret = webserviceOptions.clientSecretVariable.map(CredentialsUtil.getCredentials)
  private val webServiceUser = webserviceOptions.userVariable.map(CredentialsUtil.getCredentials)
  private val webServicePassword = webserviceOptions.passwordVariable.map(CredentialsUtil.getCredentials)

  val keycloak: Option[Keycloak] = webserviceOptions.keycloakAuth.map(_.prepare(webServiceClientId.get, webServiceClientSecret.get))

  // not used for now as writing data is not yet implemented
  override val saveMode: SaveMode = SaveMode.Overwrite

  override def partitions: Seq[String] = partitionDefs.map(_.name)
  override def expectedPartitionsCondition: Option[String] = None // all partitions are expected to exist

  /**
   * Get Access Token through Keycloak
   * @return
   */
  def getAccessToken : Option[AccessTokenResponse] = {
    keycloak.map(_.tokenManager.getAccessToken)
  }

  /**
   * Initialized the webservice
   * @return
   */
  def prepareWebservice(query: String) : Webservice = {
    // If we have a keycloak config, we use Keycloak for all webservice calls and let it handle the tokens
    if(webserviceOptions.keycloakAuth.isDefined) {

      val token = getAccessToken.get

      logger.debug(s"Token is: ${token.getToken.substring(0,30)}...")
      logger.debug("Token expires in " + token.getExpiresIn)

      // Call webservice with token
      initWebservice(webserviceOptions.url + query, webserviceOptions.connectionTimeoutMs, webserviceOptions.readTimeoutMs
        , webserviceOptions.authHeader, webServiceClientId, webServiceClientSecret, webServiceUser
        , webServicePassword, Some(token.getToken))
    }
    // Call webservice without token
    else {
      initWebservice(webserviceOptions.url + query, webserviceOptions.connectionTimeoutMs, webserviceOptions.readTimeoutMs
        , webserviceOptions.authHeader, None, None, webServiceUser, webServicePassword, None)
    }

  }

  /**
   *  Calls webservice and returns response as string
   *  Supports different methods
   *  client-id / client-secret --> Call with Bearer token incl. automatic refresh of token if necessary
   *  Normal call with optional custom header and user/password
    *
    * @return Response as Array[Byte]
    */
  def getResponse(query: String) : Array[Byte] = {
    val webserviceClient = prepareWebservice(query)

    webserviceClient.get() match {
      case Success(c) => c
      case Failure(e) => logger.error(e.getMessage, e)
        throw new WebserviceException(e.getMessage)
    }
  }

  /**
   * Same as getResponse, but returns response as InputStream
   *
   * @param query it should be possible to define the partition to read as query string, but this is not yet implemented
   */
  override def createInputStream(query: String)(implicit session: SparkSession): InputStream = {
    val webserviceClient = prepareWebservice(query)
    webserviceClient.get() match {
      case Success(c) => new ByteArrayInputStream(c)
      case Failure(e) => throw new WebserviceException(s"Could not create InputStream for $id and $query: ${e.getClass.getSimpleName} - ${e.getMessage}")
    }
  }

  /**
   * Initializes the webservice according to given parameters
   * @param url URL to call
   * @param connectionTimeoutMs Connection timeout in milliseconds
   * @param readTimeoutMs Read timeout in milliseconds
   * @param authHeader custom authentication header
   * @param clientId client-id for OAuth2
   * @param clientSecret client-secret for OAuth2
   * @param user user for basic authentication
   * @param pwd password for basic authentication
   * @param token token for direct call with token
   * @return
   */
  def initWebservice(url: String, connectionTimeoutMs: Option[Int], readTimeoutMs: Option[Int], authHeader: Option[String], clientId: Option[String], clientSecret: Option[String], user: Option[String], pwd: Option[String], token: Option[String] = None): Webservice = {
    if (token.isEmpty) {
      // "classic" call if no token is provided. In this case, we still distinguish between:
      // auth-header: custom auth-header was provided, use it
      // client-id / client-secret: OAuth2, token refresh currently handled by Keycloak
      // user / password: Basic Authentication
      if (authHeader.isDefined) {
        DefaultWebserviceClient(Some(url), Some(Map("auth-header" -> authHeader.get)), connectionTimeoutMs, readTimeoutMs)
      } else if (clientId.isDefined) {
        val client_params = Map("client-id" -> clientId.get, "client-secret" -> clientSecret.getOrElse(throw new ConfigurationException("client-secret missing for client-id authentification")))
        DefaultWebserviceClient(Some(url), Some(client_params), connectionTimeoutMs, readTimeoutMs)
      } else if (user.isDefined) {
        DefaultWebserviceClient(Some(url), Some(Map("user" -> user.get, "password" -> pwd.getOrElse(throw new ConfigurationException("password missing for user authentification")))), connectionTimeoutMs, readTimeoutMs)
      } else {
        // Call without any special headers (no authentication)
        DefaultWebserviceClient(Some(url), None, connectionTimeoutMs, readTimeoutMs)
      }
    } else {
      // Call with Token (without Keycloak)
      val client_params = Map("token"->token.get)
      DefaultWebserviceClient(Some(url), Some(client_params), connectionTimeoutMs, readTimeoutMs)
    }
  }

  /**
   * For WebserviceFileDataObject, every partition is mapped to one FileRef
   */
  override def getFileRefs(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Seq[FileRef] = {
    val partitionValuesToProcess = if (partitions.nonEmpty) {
      // as partitionValues don't need to define a value for every partition, we need to create all partition values and filter them
      val allPartitionValues = createAllPartitionValues
      if (partitionValues.isEmpty || partitionValues.contains(PartitionValues(Map()))) allPartitionValues
      else allPartitionValues.filter(allPv => partitionValues.exists(_.elements.forall(filterPvElement => allPv.get(filterPvElement._1).contains(filterPvElement._2))))
    } else Seq(PartitionValues(Map())) // create empty default PartitionValue
    // create one FileRef for every PartitionValue
    partitionValuesToProcess.map(createFileRef)
  }

  /**
   * create a FileRef for one given partitionValues
   */
  private def createFileRef(partitionValues: PartitionValues)(implicit session: SparkSession): FileRef = {
    val queryString = getPartitionString(partitionValues)
    // translate urls special characters into a regular filename
    def translate( s: String, translation: Map[Char,Char] ): String = s.map( c => translation.getOrElse(c,c))
    val translationMap = Map('?' -> '.', '&' -> '.', '=' -> '-')
    FileRef( fullPath = queryString.getOrElse(""), fileName = translate(queryString.getOrElse("result"), translationMap), partitionValues)
  }

  /**
   * generate all partition value combinations from possible query parameter values
   */
  private def createAllPartitionValues = {
    // create partition values from first partition definition
    val headPartitionValuess: Seq[PartitionValues] = partitionDefs.head.values.map(v => PartitionValues(Map(partitionDefs.head.name -> v)))
    // add the following partition definitions
    partitionDefs.tail.foldLeft(headPartitionValuess) {
      case (partitionValuess, partitionDef) =>
        partitionValuess.flatMap(partitionValues => partitionDef.values.map(v => PartitionValues(partitionValues.elements + (partitionDef.name -> v))))
    }
  }

  /**
   * List partition values defined for this web service.
   * Note that this is a fixed list.
   */
  override def listPartitions(implicit session: SparkSession): Seq[PartitionValues] = createAllPartitionValues

  /**
   * No root path needed for Webservice. It can be included in webserviceOptions.url.
   */
  override def path: String = ""

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = WebserviceFileDataObject
}

object WebserviceFileDataObject extends FromConfigFactory[DataObject] with SmartDataLakeLogger {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): WebserviceFileDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[WebserviceFileDataObject].value
  }

  def getKeyCloakConfig(webserviceOptions: Config): Option[KeycloakConfig] = {
    if (webserviceOptions.hasPath("client-id")) {
      val ssoServer: String = Try(webserviceOptions.getString("ssoServer")).getOrElse(
        throw ConfigurationException.createMissingMessage("webservice-options.ssoServer")
      )
      val ssoRealm: String = Try(webserviceOptions.getString("ssoRealm")).getOrElse(
        throw ConfigurationException.createMissingMessage("webservice-options.ssoRealm")
      )
      val ssoGrantType: String = Try(webserviceOptions.getString("ssoGrantType")).getOrElse(
        throw ConfigurationException.createMissingMessage("webservice-options.ssoGrantType")
      )
      Some(KeycloakConfig(ssoServer, ssoRealm, ssoGrantType))
    }
    else {
      None
    }
  }
}

case class WebserviceOptions (url: String,
                              connectionTimeoutMs: Option[Int] = None,
                              readTimeoutMs: Option[Int] = None,
                              authHeader: Option[String] = None,
                              clientIdVariable: Option[String] = None,
                              clientSecretVariable: Option[String] = None,
                              keycloakAuth: Option[KeycloakConfig] = None,
                              userVariable: Option[String] = None,
                              passwordVariable: Option[String] = None,
                              token: Option[String] = None)

