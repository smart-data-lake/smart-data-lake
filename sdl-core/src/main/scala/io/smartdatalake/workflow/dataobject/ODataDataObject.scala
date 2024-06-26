package io.smartdatalake.workflow.dataobject

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.secrets.StringOrSecret

import scala.collection.mutable.ArrayBuffer
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.WebserviceMethod.WebserviceMethod
import io.smartdatalake.util.webservice.{ScalaJWebserviceClient, WebserviceException, WebserviceMethod}
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, DataType, StructType, StructField, StringType}
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats}

import java.net.URLEncoder
import java.nio.file.{Files, Paths}
import java.io.{BufferedWriter, File, FileWriter}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.util.{Failure, Success}
import org.apache.hadoop.fs.{Path => HadoopPath}

import scala.reflect.runtime.universe.typeOf

/**
 * InversionOfControl container class
 * This class serves as a hub or a relay for all calls to class creators and to static functions outside of this module.
 * It allows test code to override these calls and to inject Mock- and Spy objects into the running code.
 */
class ODataIOC {

  /**
   * Creates a new instance of the java.io.File class
   * @param path to the file
   * @return java.io.File object
   */
  def newFile(path:String) : java.io.File = {
    new File(path)
  }

  /**
   * Create a new instance of the java.nio.file.Path class
   * @param path
   * @return java.nio.file.Path object
   */
  def newPath(path:String) : java.nio.file.Path = {
    Paths.get(path)
  }

  /**
   * Create a new instance of the java.io.BufferedWriter class
   * @param writer reference to an already existing Writer object
   * @return java.io.BufferedWriter object
   */
  def newBufferedWriter(writer: java.io.Writer): BufferedWriter = {
    new BufferedWriter(writer)
  }

  /**
   * Create a new instance of the java.io.FileWriter class
   * @param file path to the file to write to
   * @return java.io.FileWriter
   */
  def newFileWriter(file: java.io.File) : FileWriter= {
    new FileWriter(file)
  }

  /**
   * Create a new instance of the ODataResponseMemoryBuffer class
   * @param setup buffer configuration
   * @param context pipeline context
   * @return ODataResponseMemoryBuffer
   */
  def newODataResponseMemoryBuffer(setup: ODataResponseBufferSetup, context: ActionPipelineContext) : ODataResponseMemoryBuffer = {
    new ODataResponseMemoryBuffer(setup, context, this)
  }

  /**
   * Create a new instance of the ODataReponseLocalFileBuffer class
   * @param tmpDirName path to the temporary direction to write to
   * @param setup buffer configuration
   * @param context pipeline context
   * @return ODataReponseLocalFileBuffer
   */
  def newODataResponseLocalFileBuffer(tmpDirName: String, setup: ODataResponseBufferSetup, context: ActionPipelineContext) : ODataResponseLocalFileBuffer =
  {
    new ODataResponseLocalFileBuffer(tmpDirName, setup, context, this)
  }

  /**
   * Create a new instance of the ODataResponseDBFSFileBuffer class
   * @param tmpDirName path to the temporary direction to write to
   * @param setup buffer configuration
   * @param context pipeline context
   * @return ODataResponseDBFSFileBuffer
   */
  def newODataResponseDBFSFileBuffer(tmpDirName: String, setup: ODataResponseBufferSetup, context: ActionPipelineContext) : ODataResponseDBFSFileBuffer =
  {
    new ODataResponseDBFSFileBuffer(tmpDirName, setup, context, this)
  }

  /**
   * Create a new instance of a ResponseFileBuffer class, based on the provided parameters
   * @param tmpDirName path to the temporary direction to write to
   * @param setup buffer configuration
   * @param context pipeline context
   * @return either ODataResponseLocalFileBuffer or ODataResponseDBFSFileBuffer
   */
  def newODataResponseFileBufferByType(tmpDirName: String, setup: ODataResponseBufferSetup, context: ActionPipelineContext) : ODataResponseBuffer = {
    var result : ODataResponseBuffer = null

    if (setup != null) {
      setup.tempFileBufferType.getOrElse("").toLowerCase match {
        case "local" => result = newODataResponseLocalFileBuffer(tmpDirName, setup, context)
        case "dbfs"  => result = newODataResponseDBFSFileBuffer(tmpDirName, setup, context)
        case _ => throw ConfigurationException(s"(Unknown FileBufferType '$setup.tempFileBufferType'")
      }
    }
    else {
      throw ConfigurationException("No configuration available to the create FileResponseBuffer")
    }
    result
  }

  /**
   * Create a new instance of the ODataBearerToken class
   * @param token bearer token
   * @param expiresAt expire datetime
   * @return ODataBearerToken
   */
  def newODataBearerToken(token: String, expiresAt:java.time.Instant) : ODataBearerToken = {
    ODataBearerToken(token, expiresAt)
  }

  /**
   * Creates a new instance of the org.apache.hadoop.fs.FileSystem class
   * @param path base path for the file system
   * @param context pipeline context
   * @return org.apache.hadoop.fs.FileSystem
   */
  def newHadoopFsWithConf(path: org.apache.hadoop.fs.Path, context:ActionPipelineContext) : org.apache.hadoop.fs.FileSystem = {
    HdfsUtil.getHadoopFsWithConf(path)(context.hadoopConf)
  }

  /**
   * Create a new instance of the org.apache.hadoop.fs.Path class
   * @param path path as a string
   * @return org.apache.hadoop.fs.Path
   */
  def newHadoopPath(path: String) : HadoopPath = {
    new HadoopPath(path)
  }

  /**
   * Create a new instance of the org.apache.hadoop.fs.Path class
   * @param parent parent directory
   * @param child child directory
   * @return org.apache.hadoop.fs.Path
   */
  def newHadoopPath(parent: String, child: String) : HadoopPath = {
    new HadoopPath(parent, child)
  }

  /**
   * Create a new instance of the org.apache.hadoop.fs.Path class
   * @param parent parent directory
   * @param child child directory
   * @return org.apache.hadoop.fs.Path
   */
  def newHadoopPath(parent: HadoopPath, child: String) : HadoopPath = {
    new HadoopPath(parent, child)
  }

  /**
   * Create a new instance of the io.smartdatalake.util.webservice.ScalaJWebserviceClient class
   * @param url
   * @param headers
   * @param timeouts
   * @param authMode
   * @param proxy
   * @param followRedirects
   * @return io.smartdatalake.util.webservice.ScalaJWebserviceClient
   */
  def newScalaJWebServiceClient(url: String, headers : Map[String, String], timeouts : Option[HttpTimeoutConfig], authMode : Option[io.smartdatalake.definitions.AuthMode], proxy :  Option[HttpProxyConfig], followRedirects: Boolean) : ScalaJWebserviceClient = {
    ScalaJWebserviceClient(url, headers, timeouts, authMode, proxy, followRedirects)
  }

  /**
   * Calls the static method exists of the java.nio.file.Files class
   * @param path path to check
   * @return true if exists
   */
  def fileExists(path:java.nio.file.Path): Boolean = {
    Files.exists(path)
  }

  /**
   * Calls the static method createDirectories of the java.nio.file.Files class
   * @param path path to create
   * @return
   */
  def fileCreateDirectories(path:java.nio.file.Path) : java.nio.file.Path = {
    Files.createDirectories(path)
  }

  /**
   * Calls the static method now of the java.time.Instance class
   * @return now instant
   */
  def getInstantNow : Instant = {
    Instant.now()
  }

  /**
   * Calls the static method writeHadoopFile of the io.smartdatalake.util.hdfs.HdfsUtil class
   * @param path path to write to
   * @param content content to write
   * @param filesystem reference to the file system
   */
  def writeHadoopFile(path: HadoopPath, content: String, filesystem: FileSystem) : Unit = {
    HdfsUtil.writeHadoopFile(path, content)(filesystem)
  }
}


/**
 * [[ODataAuthorization]] contains the coordinates and credentials to gain access to the DataSource
 * @param oauthUrl: URL to the OAuth2 authorization instance like "https://login.microsoftonline.com/{tenant-guid}/oauth2/v2.0/token"
 * @param clientId: Name of the user (supports secrets providers)
 * @param clientPassword: Password of the user (supports secret providers)
 * @param oauthScope: OAuth authorization scope (like https://xxx.crm4.dynamics.com/.default)
 */
case class ODataAuthorization(oauthUrl: String
                              , clientId: StringOrSecret
                              , clientPassword: StringOrSecret
                              , oauthScope: String
                             )

/**
 * [[ODataBearerToken]] contains the current bearer token and a method to check whether the token is still valid.
 * @param token : The token string
 * @param expiresAt : Instant at which the token expires
 */
case class ODataBearerToken(token: String, expiresAt:java.time.Instant) {

  def isExpired : Boolean = {
    expiresAt.isBefore(Instant.now())
  }
}


/**
 * [[DataObject]] of type OData.
 *
 * @param schema : Schema of the expected output in the form of <<array< struct< columnA:string, columnB: integer ... >>
 * @param baseUrl : Base URL of the OData Service like https://xxx.crm4.dynamics.com/api/data/v9.2/
 * @param tableName : Name of the table which needs to be accessed
 * @param sourceFilters : Optional. OData filter string which will be applied to the access operation like "objecttypecode eq 'task' and createdon ge 2024-01-01T00:00:00.000Z"
 * @param timeouts : Optional. Timeout settings of type [[HttpTimeoutConfig]]
 * @param authorization: Optional. Authorization credentials of type [[ODataAuthorization]]
 * @param changeDateColumnName: Optional. Name of the column which will be used to read incrementally. like "modifiedon"
 * @param nRetry: Optional. Number of retries after a failed attempt, default = 1
 * @param responseBufferSetup: Optional. Setup for response buffers of type [[ODataResponseBufferSetup]]
 * @param maxRecordCount: Optional. Maximum number of records to be extracted.
 * @param metadata
 * @param instanceRegistry
 */
case class ODataDataObject(override val id: DataObjectId,
                           override val schema: Option[GenericSchema],
                           baseUrl : String,
                           tableName: String,
                           sourceFilters: Option[String] = None,
                           timeouts: Option[HttpTimeoutConfig] = None,
                           authorization: Option[ODataAuthorization] = None,
                           changeDateColumnName: Option[String] = None,
                           nRetry: Int = 1,
                           responseBufferSetup : Option [ODataResponseBufferSetup] = None,
                           maxRecordCount: Option[Int] = None,
                           override val metadata: Option[DataObjectMetadata] = None
                          )
                          (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateSparkDataFrame with CanCreateIncrementalOutput with SmartDataLakeLogger with UserDefinedSchema {

  private var ioc: ODataIOC = new ODataIOC()
  private var previousState : String = ""
  private var nextState : String = ""
  private var responseBuffer : ODataResponseBuffer = null

  def injectIOC(ioc: ODataIOC) : Unit = {
    this.ioc = ioc
  }


  /**
   * Calls the OData-API for one request.
   *
   * @param url: The url to be called
   * @param method: Either Get or Set
   * @param headers: Additional headers to be sent allong with the request
   * @param body: The body of the message
   * @param mimeType: MIME type of the message
   * @param retry: Number of retries
   * @return the response to the request
   */
  @tailrec
  private def request(url: String
                      , method: WebserviceMethod = WebserviceMethod.Get
                      , headers: Map[String, String] = Map()
                      , body: String = ""
                      , mimeType: String = "application/json"
                      , retry: Int = nRetry
                     ) : Array[Byte] = {
    val webserviceClient = ioc.newScalaJWebServiceClient(url, headers, timeouts, authMode = None, proxy = None, followRedirects = true)
    val webserviceResult = method match {
      case WebserviceMethod.Get =>
        webserviceClient.get()
      case WebserviceMethod.Post =>
        val bodyArray = body.getBytes("UTF8")
        webserviceClient.post(bodyArray, mimeType)
    }

    webserviceResult match {
      case Success(c) =>
        logger.info(s"Success for request $url")
        c
      case Failure(e) =>
        if(retry == 0) {
          logger.error(e.getMessage, e)
          throw new WebserviceException(e.getMessage)
        }
        logger.info(s"Request will be repeated, because the server responded with: ${e.getMessage}. \nRequest retries left: ${retry-1}")
        request(url, method, headers, body, mimeType, retry-1)
    }
  }

  /**
   * Packs the provided map into a url string and encodes this string according to http standards
   * @param content: Content to be packed
   * @return URL string
   */
  private def getFormUrlEncodedBody(content: Map[String, String]) : String = {
    val contentEncoded = content.map{
      case (key, value) => key + "=" + URLEncoder.encode(value, "UTF-8")
    }
    contentEncoded.mkString("&")
  }

  /**
   * Requests a new bearer token from the remote authorization instance
   * @param authorization : authorization parameters
   * @return new [[ODataBearerToken]] instance
   */
  def getBearerToken(authorization: ODataAuthorization) : ODataBearerToken = {
    implicit val formats: Formats = DefaultFormats

    val payload : Map[String, String] = Map(
      "grant_type" -> "client_credentials",
      "client_id" -> authorization.clientId.resolve(),
      "client_secret" -> authorization.clientPassword.resolve(),
      "scope" ->  authorization.oauthScope
    )

    val payloadString = getFormUrlEncodedBody(payload)

    val response = request(authorization.oauthUrl, method=WebserviceMethod.Post, body=payloadString, mimeType="application/x-www-form-urlencoded")
    val responseString : String = new String(response)
    val responseMap : Map[String, Any] = Serialization.read[Map[String, Any]](responseString)

    val token = responseMap.apply("access_token").asInstanceOf[String]
    val expiresInSecs = responseMap.apply("expires_in").asInstanceOf[BigInt].longValue

    val expiresDateTime = Instant.now.plusSeconds(expiresInSecs)
    ioc.newODataBearerToken(token, expiresDateTime)
  }

  /**
   * Creates the required headers for the main requests (containing the bearer token)
   * @param bearerToken : Current instance of the [[ODataBearerToken]]
   * @return Map instance with the headers
   */
  private def getRequestHeader(bearerToken: ODataBearerToken) : Map[String, String] = {
    Map("Authorization" -> s"Bearer ${bearerToken.token}"
      , "Accept" -> "application/json"
      , "Content-Type" -> "application/json; charset=utf-8"
    )
  }

  /**
   * Extracts the column names from the provided schema
   * @param schema : Expected schema
   * @return Sequence of column names
   */
  private def extractColumnNames(schema: StructType): Seq[String] = {
    schema.fields.flatMap {
      field =>
        field.dataType match {
          case structType: StructType =>
            extractColumnNames(structType).map(field.name + "." + _)
          case _ =>
            field.name :: Nil
        }
    }
  }

  /**
   * Scans the received response text for the next link using Regex
   * @param message : Received response
   * @return next link url or "" if no next link was found
   */
  private def extractNextLink(message: String): String = {
    val searchString = "\"@odata.nextLink\":\""
    val posStartKey = message.indexOf(searchString)
    if (posStartKey > 0) {
      val posStartValue = posStartKey + searchString.length
      val posEndValue = message.indexOf("\"", posStartValue)
      val nextLink = message.substring(posStartValue, posEndValue)
      nextLink
    }
    else {
      ""
    }
  }

  /**
   * Converts the proviced [[java.time.Instant]] into a properly formated , Odata-Compatible  string
   * @param instant : Instant to be converted
   * @return String
   */
  private def getODataInstantFilterLiteral(instant: Instant) : String = {
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX").withZone(ZoneId.of("UTC")).format(instant)
  }

  /**
   * Creates the URL to be called by combining all necessary parameters into the url
   * @param columnNames : Names of the expected columns
   * @param extractStart: Start of the program to exclude records that were modified after this start.
   * @return OData request URL
   */
  def getODataURL(columnNames: Seq[String], extractStart: Instant) : String = {

    //Start with the base url and the table name
    var requestUrl = s"$baseUrl$tableName"

    //Using this ArrayBuffer oDataModifiers to collect all required parameters
    val oDataModifiers = ArrayBuffer[String]()

    //Adding a list of all required columns
    if (columnNames.nonEmpty) {
      oDataModifiers.append(s"$$select=${URLEncoder.encode(columnNames.mkString(","), "UTF-8")}")
    }

    //If there are any filters required, add them too
    val filters = getODataURLFilters(extractStart)
    if (filters.isDefined) {
      oDataModifiers.append(filters.get)
    }

    //If there is a maximum record count specified, add it
    if (maxRecordCount.isDefined) {
      oDataModifiers.append(s"$$top=${maxRecordCount.get}")
    }

    //If there are any modifiers collected (see lines above), add these modified to the url string
    if (oDataModifiers.nonEmpty) {
      requestUrl += "?" + oDataModifiers.mkString("&")
    }
    requestUrl
  }

  /**
   * Sub method to collect all required filters
   * @param extractStart : Start of the program to exclude records that were modified after this start.
   * @return
   */
  private def getODataURLFilters(extractStart: Instant) : Option[String] = {
    val filters = ArrayBuffer[String]()

    //If there are any predefined filters configured, treat these filters as one unit and add them
    //to the filters list
    if (sourceFilters.isDefined) {
      filters.append("(" + sourceFilters.get + ")")
    }

    //If there is a changeDateColumnName and a previousState specified, use this column to filter only the records that
    //were modified since the last run (previousState) and the start of this run.
    if (changeDateColumnName.isDefined && previousState != "") {

      val endRange = getODataInstantFilterLiteral(extractStart)
      filters.append(s"${changeDateColumnName.get} lt $endRange")

      val startRange = getODataInstantFilterLiteral(Instant.parse(previousState))
      filters.append(s"${changeDateColumnName.get} ge $startRange")
    }

    //If there are any filters found, combine them with AND into one string and return it
    if (filters.nonEmpty) {
      var filterString = filters.mkString(" and ")
      filterString = URLEncoder.encode(filterString, "UTF-8")
      Option("$filter=" + filterString)
    } else {
      None
    }
  }

  /**
   * Creates the DataFrame of the received responses. This is the main method which triggers the OData-API-Calls.
   * It blocks until all data is received.
   * @param partitionValues
   * @param context
   * @return
   */
  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {
    import org.apache.spark.sql.functions._
    implicit val formats: Formats = DefaultFormats
    val session = context.sparkSession
    import session.implicits._

    //Marking the current time to exclude records which are modified after this instant
    val startTimeStamp = this.ioc.getInstantNow

    val recordSchema = schema.get.convert(typeOf[SparkSubFeed]).asInstanceOf[SparkSchema]
    val arraySchema = ArrayType(recordSchema.inner)

    if(context.phase == ExecutionPhase.Init){
      // In Init phase, return an empty DataFrame
      Seq[String]().toDF("responseString")
        .select(from_json($"responseString", arraySchema).as("response"))
        .select(explode($"response").as("record"))
        .select("record.*")
        .withColumn("sdlb_created_on", current_timestamp())
    } else {
      //DEL Convert the schema string into StructType
      //DEL val schemaType  = ioc.sparkDataTypeFromDDL(schema)

      //DEL Extract the schema of one record
      //DEL val schemaTypeRecords = schemaType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]

      //Extract the names of the columns
      //DEL val columnNames = extractColumnNames(schemaTypeRecords)
      val columnNames = recordSchema.columns

      //Generate the URL for the first API call
      var requestUrl = getODataURL(columnNames, startTimeStamp)

      //Request the bearer token
      var bearerToken = getBearerToken(authorization.get)

      responseBufferSetup.get.setTableName(this.tableName)

      //Initialize the MemoryBuffer
      this.responseBuffer = ioc.newODataResponseMemoryBuffer(responseBufferSetup.get, context)
      var loopCount = 0

      //Commence the looping for each request
      while (requestUrl != "") {
        //Check if the bearer token is still valid and request a new one if not
        if (bearerToken.isExpired) {
          bearerToken = getBearerToken(authorization.get)
        }

        //Generate the request header
        val requestHeader = getRequestHeader(bearerToken)

        //Execute the current request
        val responseBytes = request(requestUrl, headers = requestHeader)

        //Convert the current response into a string
        val responseString = new String(responseBytes, "UTF8")

        //Check if the response buffer should evolve
        this.responseBuffer = this.responseBuffer.switchIfNecessary()

        //Add the response to the response buffer
        this.responseBuffer.addResponse(responseString)

        //Get next URL for the next API call
        requestUrl = extractNextLink(responseString)
        loopCount += 1
      }

      logger.info(s"Loop count $loopCount")

      //DEL create dataframe with the correct schema and add created_at column with the current timestamp
      //DEL val schemaExtended = s"struct< `@odata.context`: string, value: $schema>"
      val responseSchema = StructType(Seq(StructField("@odata.context", StringType), StructField("value", arraySchema)))

      val responsesDf = this.responseBuffer.getDataFrame
        .select(from_json($"responseString", responseSchema).as("response"))
        .select(explode($"response.value").as("record"))
        .select("record.*")
        .withColumn("sdlb_created_on", current_timestamp())

      // put simple nextState logic below
      nextState = startTimeStamp.toString
      // return
      responsesDf
    }
  }

  /**
   * Runs operations after reading from [[DataObject]]
   */
  override private[smartdatalake] def postRead(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
      super.postRead(partitionValues)

      if (this.responseBuffer != null) {
        this.responseBuffer.cleanUp()
      }

    }

  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {
    implicit val formats: Formats = DefaultFormats
    previousState = state.getOrElse("")
  }

  override def getState: Option[String] = {
    implicit val formats: Formats = DefaultFormats
    Some(nextState)
  }

  override def factory: FromConfigFactory[DataObject] = ODataDataObject
}

object ODataDataObject extends FromConfigFactory[DataObject] with SmartDataLakeLogger {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ODataDataObject = {
    extract[ODataDataObject](config)
  }
}
