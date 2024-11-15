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

package io.smartdatalake.util.webservice

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.OpenApiUtil.simplifyContentType
import io.smartdatalake.util.webservice.SttpUtil.sendRequest
import org.apache.spark.sql.confluent.json.JsonSchemaConverter
import org.apache.spark.sql.types._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{JNothing, _}
import sttp.client3.{HttpClientSyncBackend, Identity, SttpBackend, basicRequest}
import sttp.model.{Header, MediaType, Uri}

import scala.collection.mutable

/**
 * Utils to handle OpenApi webservices.
 *
 * For now it can query the specification and extract the schema of an operation.
 */
object OpenApiUtil extends SmartDataLakeLogger {
  private val definitionsPath = "components"
  private val specCache = mutable.Map[String, OpenApiSpec]()
  val defaultApiDocsPath = "v3/api-docs"
  val defaultResponseContentType = "application/json"
  @transient private lazy val httpBackend: SttpBackend[Identity, Any] = HttpClientSyncBackend()

  /**
   * Query OpenApi specification and extract the schema of an operation.
   *
   * For now this supports OpenApi V3, most parts of json schema and reusable schema components.
   *
   * @param baseUrl             base Url of the webservice
   * @param operationId         Id of operation to extract schema for
   * @param apiDocsPath         path appended to baseUrl to query OpenApi documentation/specification
   *                            Default is 'v3/api-docs'
   * @param responseContentType response content type to extract schema for.
   *                            Default is 'application/json'.
   * @return The Spark DataType for the given operation.
   */
  def queryOperationSchema(baseUrl: String, operationId: String, responseContentType: String, apiDocsPath: String = defaultApiDocsPath): (String, DataType) = {
    // query webservice and cache result
    val url = s"$baseUrl/$apiDocsPath"
    val spec = specCache.getOrElseUpdate(url, getAndParseSpec(url)(httpBackend))
    // find operation in spec
    val operation = spec.operations.find(p => p.operationId == operationId)
      .getOrElse(throw ConfigurationException(s"operationId $operationId not found in OpenApi Spec operations: ${spec.operations.map(_.operationId).mkString(", ")}"))
    // get schema
    operation.responseSchema(responseContentType)
  }

  private[smartdatalake] def getAndParseSpec(url: String)(implicit httpBackend: SttpBackend[Identity, Any]): OpenApiSpec = {
    val request = basicRequest
      .get(Uri.unsafeParse(url))
      .header(Header.accept(MediaType.ApplicationJson))
      .followRedirects(true)
    val schema = sendRequest(request, s"get OpenApi specification")
    logger.debug(s"got response $schema")
    val jsonSpec = parse(schema)
    val operations = extractOperationsFromJson(jsonSpec)
    val server = extractFirstServerFromJson(jsonSpec)
    OpenApiSpec(operations, server)
  }

  private[smartdatalake] def extractFirstServerFromJson(spec: JValue): Option[String] = {
    (spec \ "servers") match {
      case servers: JArray =>
        servers.arr.collectFirst { case x: JString => x.s }
      case JNothing => None
    }
  }

  private[smartdatalake] def extractOperationsFromJson(spec: JValue): Seq[OpenApiOperation] = {
    spec \ "paths" match {
      case paths: JObject =>
        paths.obj.flatMap { case (path, pathSpec: JObject) =>
          pathSpec.obj.flatMap { case (operation, opSpec: JObject) =>
            val response200 = opSpec \ "responses" \ "200"
            if (response200 != JNothing) {
              response200 \ "content" match {
                case contents: JObject =>
                  val sparkSchemas = contents.obj.map { case (contentType, contentSpec: JObject) =>
                    logger.debug(s"parsing operation $path:$operation: element responses\\200\\content\\$contentType\\schema")
                    val jsonSchema = contentSpec \ "schema" match {
                      case JNothing => throw new IllegalStateException(s"Element responses\\200\\content\\$contentType\\schema not found in OpenApi Spec operation $path:$operation")
                      case obj: JObject =>
                        // enrich with definitions
                        val definitions = (spec \ definitionsPath) merge (response200 \ definitionsPath)
                        obj ~ (definitionsPath -> definitions) // enrich with definitions
                      case any => any
                    }
                    // sparkSchema is parsed lazy to avoid errors on irrelevant operations
                    val sparkSchema = () => JsonSchemaConverter.convertParsedSchemaToSparkDataType(jsonSchema, definitionsPath = definitionsPath)
                    (contentType, sparkSchema)
                  }
                  val operationId = opSpec \ "operationId" match {
                    case JNothing => s"$path:$operation" // default if operationId is not set
                    case id: JString => id.s
                    case any => any.toString
                  }
                  Some(OpenApiOperation(path, operation, operationId, sparkSchemas.toMap))
                case JNothing =>
                  logger.debug(s"Element responses\\200\\content not found in OpenApi Spec operation $path:$operation")
                  None
              }
            } else {
              logger.debug(s"Element responses\\200 not found in OpenApi Spec operation $path:$operation")
              None
            }
          }
        }
    }
  }

  def simplifyContentType(contentType: String): String = {
    contentType.takeWhile(_ != ';')
  }
}

case class OpenApiSpec(operations: Seq[OpenApiOperation], server: Option[String] = None)

case class OpenApiOperation(path: String, operation: String, operationId: String, responseSchemasFun: Map[String, () => DataType]) {
  private val responseSchemasSimpleTypeFun = responseSchemasFun.map { case (k, v) => (simplifyContentType(k), v) }
  private val responseSchemaCache = mutable.Map[String, DataType]()

  def responseSchema(contentType: String): (String, DataType) = {
    if (responseSchemasFun.contains(contentType)) {
      (contentType, responseSchemaCache.getOrElseUpdate(contentType, responseSchemasFun(contentType)()))
    } else if (responseSchemasSimpleTypeFun.contains(simplifyContentType(contentType))) {
      (simplifyContentType(contentType), responseSchemaCache.getOrElseUpdate(contentType, responseSchemasSimpleTypeFun(simplifyContentType(contentType))()))
    } else {
      throw ConfigurationException(s"No schema found for operationId $operationId and contentType $contentType. Available contentTypes are ${responseSchemasFun.keys.mkString(", ")}")
    }
  }
}
