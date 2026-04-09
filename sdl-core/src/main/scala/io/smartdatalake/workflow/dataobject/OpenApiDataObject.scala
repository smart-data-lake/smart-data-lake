/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import com.jayway.jsonpath.PathNotFoundException
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{ResourceUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.dataset.getEmptyDataFrame
import io.smartdatalake.util.spark.json.JsonUtils
import io.smartdatalake.util.webservice.OpenApiUtil.{defaultApiDocsPath, defaultResponseContentType}
import io.smartdatalake.util.webservice.SttpUtil.{SttpRequestExtension, createDefaultBackend}
import io.smartdatalake.util.webservice._
import io.smartdatalake.workflow.connection.SparkClassicConnection
import io.smartdatalake.workflow.connection.authMode.HttpAuthMode
import io.smartdatalake.workflow.dataobject.spark.CanCreateSparkDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DatasetHelper, SparkSession}
import org.json4s.{JArray, JObject, JValue}
import sttp.client3.{Identity, SttpBackend, asByteArray, basicRequest}
import sttp.model.{MediaType, Uri}

/**
 * Reads data from an OpenApi compliant WebService operation.
 * The operation must return Json and have its schema defined in the OpenApi specification.
 *
 * Limitations:
 *   - Only GET method implemented for now
 *   - No dynamic URL parameters supported for now
 *   - Only responseContentType=application/json will be parsed into schema. Otherwise schema will be a single column with name 'content' of type String or Binary.
 *   - Only token based pagination is supported. Use pagingLinkJsonPath to extract Url for the next page.
 *   - OpenApi Webservice requests can not be parallelized and distributed to executors. They run on the driver.
 *     In order to avoid memory problems Spark BlockManager is used to create a new Spark partition after every maxRecordsPerPartition number of records.
 *
 * Also note that the getDataFrame method is not lazy in Exec-Phase. It will query the WebService before creating the DataFrame.
 *
 * @param id                        DataObject identifier
 * @param baseUrl                   the server url to use for querying the OpenApi specification and content
 * @param operationId               the operationId from the OpenApi specification to use to get data for this DataObject
 *                                  If OpenApi specification has no operationId defined, `<sub-path>:<operation>` is used, e.g. `/test/abc:get`.
 * @param apiDocsUrl                The url to load the OpenApi specification from.
 *                                  If it is a relative Url it is appended to baseUrl.
 *                                  Default is "v3/api-docs", which will be concatenated to `<baseUrl>/v3/api-docs`.
 *                                  Alternatively this can also be a hadoop file or classpath resource.
 *                                  An url starting with protocol "cp:" will be resolved as classpath resource.
 *                                  All protocols different from http/https/cp will be resolved as Hadoop path. To use a relative Hadoop path start with "./".
 * @param useFirstOpenApiSpecServer if true content is queried using first server definition from OpenApi specification, instead of using baseUrl.
 *                                  Default is false.
 * @param responseContentType       Content-type of OpenApi response to use. Default is application/json.
 *                                  Note that additional attributes such as `; charset=utf-8` are ignored if not found in the first try.
 * @param proxy                     optional Proxy configuration used to make HTTP-connection.
 * @param urlParameters             Additional Url parameters to pass with the http request to get data
 * @param additionalHeaders         Additional headers to pass with the http request to get spec and data
 * @param timeouts                  optional configuration of HTTP timeouts
 * @param authMode                  Optional configuration of webservice authentication. Supported `AuthMode`s are all HttpAuthModes, e.g. BasicAuthMode, OAuthMode, CustomHttpAuthMode.
 *                                  CustomHttpAuthMode can be used to implement a custom authentication protocol, e.g. AzureADClientGrantAuthMode in sdl-azure module.
 * @param followRedirects           if redirects should be followed when creating HTTP-connection. Default is false because of security concerns.
 * @param retries                   number of retries if http request fails. Default is 0 retries.
 * @param pagingLinkJsonPath        If selected operation implements paging and returns content-type application/json, configure a JsonPath expression to extract the link of the next page to query.
 *                                  The JsonPath expression should return a String or a list of Strings as result.
 *                                  If a link for the next page is not found, it is assumed that it is the last page and no further queries will be made.
 *                                  Example JsonPath expression: "$._links[?(@.rel == 'next')].href"
 * @param schemaMatchJsonPath       The response schema of an operation in the OpenApi specification should describe the structure of the entire response body.
 *                                  If that's not the case, define schemaMatchJsonPath to extract the response part that matches the schema.
 *                                  Example JsonPath expression: "$.data"
 * @param maxPagesPerPartition      Only relevant together pagingLinkRegex.
 *                                  Sets maximum number of pages to put into one Spark partition.
 *                                  Default is 10. Remember that a page normally includes multiple records and can already be quite large.
 *                                  This helps to limit memory usage, as Spark will offload partitions to disk if memory is scarce.
 */
case class OpenApiDataObject(override val id: DataObjectId,
                             baseUrl: String,
                             operationId: String,
                             apiDocsUrl: String = defaultApiDocsPath,
                             useFirstOpenApiSpecServer: Boolean = false,
                             responseContentType: String = defaultResponseContentType,
                             authMode: Option[HttpAuthMode] = None,
                             proxy: Option[HttpProxyConfig] = None,
                             followRedirects: Boolean = false,
                             retries: Int = 0,
                             urlParameters: Map[String, String] = Map(),
                             additionalHeaders: Map[String, String] = Map(),
                             timeouts: Option[HttpTimeoutConfig] = None,
                             pagingLinkJsonPath: Option[String] = None,
                             schemaMatchJsonPath: Option[String] = None,
                             maxPagesPerPartition: Int = 10,
                             override val sparkConnectionId: Option[ConnectionId] = None,
                             override val metadata: Option[DataObjectMetadata] = None
                            )(@transient implicit override val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateSparkDataFrame with SmartDataLakeLogger {

  private val mediaType = MediaType.parse(responseContentType).right.get
  assert(pagingLinkJsonPath.isEmpty || mediaType.equalsIgnoreParameters(MediaType.ApplicationJson), "PagingLinkRegex can only be used when responseContentType=application/json")
  private val specUrl = {
    if (apiDocsUrl.startsWith("./")) apiDocsUrl
    else if (SttpUtil.canHandleScheme(apiDocsUrl)) apiDocsUrl
    else if (ResourceUtil.canHandleScheme(new Path(apiDocsUrl))) apiDocsUrl
    else if (apiDocsUrl.matches("^\\w*:")) apiDocsUrl
    else s"$baseUrl/${apiDocsUrl.dropWhile(_ == '/')}"
  }

  // these variables will be initialized in prepare phase
  private var spec: Option[OpenApiSpec] = None
  private var operation: Option[OpenApiOperation] = None
  private var responseContentTypeEvaluated: Option[String] = None
  private var responseSchema: Option[DataType] = None
  private var schema: Option[StructType] = None

  @transient private lazy implicit val httpBackend: SttpBackend[Identity, Any] = createDefaultBackend(proxy, timeouts)

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    implicit val hadoopConf: Configuration = context.hadoopConf
    super.prepare
    spec = Some(OpenApiUtil.getAndParseSpec(specUrl))
    operation = spec.flatMap(_.operations.find(_.operationId == operationId))
    assert(operation.nonEmpty, s"($id) operation $operationId not found")
    val (contentType, responseSchema) = operation.get.responseSchema(responseContentType)
    schema = responseSchema match {
      // supported cases
      case schema: StructType if mediaType.equalsIgnoreParameters(MediaType.ApplicationJson) => Some(schema)
      case schema: ArrayType if schema.elementType.isInstanceOf[StructType] && mediaType.equalsIgnoreParameters(MediaType.ApplicationJson) => Some(schema.elementType.asInstanceOf[StructType])
      case StringType if mediaType.isText => Some(StructType(Seq(StructField("content", StringType))))
      case BinaryType if mediaType.isApplication || mediaType.isImage || mediaType.isAudio || mediaType.isVideo => Some(StructType(Seq(StructField("content", BinaryType))))
      // unsupported cases
      case _: StructType => throw new IllegalStateException(s"($id) Got schema of 'object' type (e.g. Spark StructType), but can only parse responseContentType=application/json for now. Configured responseContentType=$responseContentType.")
      case StringType => throw new IllegalStateException(s"($id) Got schema of String type, but responseContentType is not text. Configured responseContentType=$responseContentType.")
      case BinaryType => throw new IllegalStateException(s"($id) Got schema of Binary type, but responseContentType is not Application|Image|Audio|Video. Configured responseContentType=$responseContentType.")
      case dataType => throw new IllegalStateException(s"($id) Schema of $dataType not supported together with responseContentType=$responseContentType")
    }
    responseContentTypeEvaluated = Some(contentType)
    this.responseSchema = Some(responseSchema)
    logger.info(s"($id) got schema: ${schema.get.simpleString}")
  }

  def getContent(url: String, contentType: String, withUrlParameters: Boolean = true): Array[Byte] = {
    val request = basicRequest
      .applyAuthMode(authMode)
      .optionalReadTimeout(timeouts)
      .get(Uri.unsafeParse(url).addParams(if (withUrlParameters) urlParameters else Map[String, String]()))
      .headers(additionalHeaders)
      .header("Allow", contentType)
      .followRedirects(followRedirects)
      .response(asByteArray)
    SttpUtil.sendRequest(request, s"($id) get", retries)
  }

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    assert(schema.nonEmpty, s"($id) prepare must be called before getDataFrame")
    implicit val session: SparkSession = sparkConnection.sparkSession
    import org.json4s.jackson.JsonMethods.parse
    import session.implicits._

    val df = context.phase match {
      // init phase -> return empty dataframe
      case ExecutionPhase.Init =>
        getEmptyDataFrame(schema.get)
      // exec phase -> read: return data
      case ExecutionPhase.Exec =>
        val targetUrl = s"$baseUrl/${operation.get.path.dropWhile(_ == '/')}"
        if (mediaType.equalsIgnoreParameters(MediaType.ApplicationJson)) {
          def getResponse(url: String, idx: Int): JValue = {
            val response = new String(getContent(url, responseContentTypeEvaluated.get, withUrlParameters = idx == 0))
            if (logger.isDebugEnabled) logger.debug(s"response: $response")
            parse(response)
          }

          var jsonData = if (pagingLinkJsonPath.isDefined) {
            SttpUtil.getPagedResponseIterator(targetUrl, createPagingLinkJsonPathExtractor(pagingLinkJsonPath.get), getResponse)
          } else {
            Iterator(getResponse(targetUrl, 0))
          }
          schemaMatchJsonPath.foreach(p => jsonData = jsonData.map(JsonUtils.evaluateJsonPath(_, p)))
          val sparkRows = responseSchema.get match {
            case _: StructType =>
              jsonData.map(e => JsonUtils.convertObjectToCatalyst(e.asInstanceOf[JObject], schema.get))
            case responseSchema: ArrayType if responseSchema.elementType.isInstanceOf[StructType] && schema.get.isInstanceOf[StructType] =>
              val jsonRows = jsonData.flatMap(_.asInstanceOf[JArray].arr)
              jsonRows.map(e => JsonUtils.convertObjectToCatalyst(e.asInstanceOf[JObject], schema.get))
          }
          DatasetHelper.parallelizeInternalRows(sparkRows, schema.get, maxPagesPerPartition)
        } else {
          responseSchema.get match {
            case StringType =>
              val data = new String(getContent(targetUrl, responseContentTypeEvaluated.get))
              if (logger.isDebugEnabled) logger.debug(s"response: $data")
              Seq(new String(data)).toDF(schema.get.fieldNames: _*)
            case BinaryType =>
              val data = getContent(targetUrl, responseContentTypeEvaluated.get)
              if (logger.isDebugEnabled) logger.debug(s"response: binary length=${data.length}")
              Seq(data).toDF(schema.get.fieldNames: _*)
          }
        }
    }

    df
  }

  private def createPagingLinkJsonPathExtractor(jsonPath: String): JValue => Option[String] = response => {
    try {
      val result = JsonUtils.evaluateJsonPath(response, jsonPath)
      val link = result.values match {
        case x: Seq[String] => x.headOption
        case x: String => Some(x)
        case null => None
        case x => throw new IllegalStateException(s"($id) pagingLinkJsonPathExtractor expects a String or a list of Strings as result, but got $x")
      }
      link.foreach(l => logger.debug(s"next pagingLink found: $l"))
      link
    } catch {
      case e: PathNotFoundException =>
        logger.info(s"($id) Next paging link not found: ${e.getClass.getSimpleName}: ${e.getMessage}")
        None
    }
  }

  override def factory: FromConfigFactory[DataObject] = OpenApiDataObject

}

object OpenApiDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): OpenApiDataObject = {
    extract[OpenApiDataObject](config)
  }
}