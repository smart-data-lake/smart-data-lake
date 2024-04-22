package io.smartdatalake.workflow.dataobject

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.secrets.StringOrSecret

import scala.collection.mutable.ArrayBuffer
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.WebserviceMethod.WebserviceMethod
import io.smartdatalake.util.webservice.{ScalaJWebserviceClient, WebserviceException, WebserviceMethod}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructType, ArrayType}
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats}
import java.net.URLEncoder
//import com.databricks.sdk.scala.dbutils.DBUtils
import java.nio.file.{Paths, Files}
import java.io.{File, BufferedWriter, FileWriter}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.util.{Failure, Success}

/**
 * [[ODataAuthorization]] contains the coordinates and credentials to gain access to the DataSource
 * @param oauthUrl: URL to the OAuth2 authorization instance like "https://login.microsoftonline.com/{tenant-guid}/oauth2/v2.0/token"
 * @param clientId: Name of the user (supports secrets providers)
 * @param clientPassword: Password of the user (supports secret providers)
 * @param oauthScope: OAuth autorization scope (like https://xxx.crm4.dynamics.com/.default)
 */
case class ODataAuthorization(oauthUrl: String
                              , clientId: StringOrSecret
                              , clientPassword: StringOrSecret
                              , oauthScope: String
                             )

/**
 * [[ODataResponseBufferSetup]] contains configurations related to the response buffer
 * @param tempFileBufferType : Name of the buffer type when file buffering is required (can either be "local" or "dbfs")
 * @param tempFileDirectoryPath : Path to the temporary direcotry in which the temporary files can be stored
 * @param memoryToFileEvolutionThresholdNumOfChars : Max number of character which can be stored in memory before switching to file buffering. (If set to 0 then no swichting will occur)
 */
case class ODataResponseBufferSetup(
                                tempFileBufferType: Option[String] = None
                                , tempFileDirectoryPath: Option[String] = None
                                , memoryToFileEvolutionThresholdNumOfChars: Option[Int] = None
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
 * [[ODataResponseBuffer]] is the base class for all other response buffer implementations
 * @param setup: The setup object which contains the configuration for this response buffer
 */
abstract class ODataResponseBuffer(setup: Option[ODataResponseBufferSetup]) {

  /**
   * Contains the number of characters already stored in this response buffer
   */
  private var storedCharCount: Int = 0

  /**
   * Adds the provided response to the buffer. This method must be overriden and called by any sub class
   * @param response : The response to be added
   */
  def addResponse(response: String): Unit = {
    storedCharCount += response.length
  }

  def addResponses(responses: Iterable[String]): Unit = {
    val x = responses.map(_ => addResponse(_))
  }

  /**
   * @return the number of characters stored in this buffer
   */
  def getStoredCharacterCount : Int = storedCharCount

  /**
   * Creates and returns a DataFrame referencing the stored responses. Each response is one record.
   * @param context: Context to create the DataFrame
   * @return : The Dataframe referencing the stored responses
   */
  def getDataFrame(context: ActionPipelineContext): DataFrame

  /**
   * Deletes all buffered responses and leaves the buffer empty.
   */
  def cleanUp() : Unit = {
    this.storedCharCount = 0
  }

  /**
   * Evolves this buffer to next stage and copies all already buffered responses, to the new instance.
   * @return The new response buffer instance.
   */
  def evolve() : ODataResponseBuffer
}


/**
 * ResponseBuffer implementation which uses memory to store all responses.
 * @param setup: The setup object which contains the configuration for this response buffer
 */
class ODataResponseMemoryBuffer(setup: Option[ODataResponseBufferSetup]) extends ODataResponseBuffer(setup) {

  /**
   * The buffered responses. Every response corresponds to one element in this array.
   */
  private var responses: ArrayBuffer[String] = ArrayBuffer.empty[String]

  /**
   * Adds a new response to the buffer
   * @param response : The response to be added
   */
  override def addResponse(response: String): Unit = {
    if (responses == null) {
      responses = ArrayBuffer(response)
    }
    else {
      responses.append(response)
    }
    super.addResponse(response)
  }

  /**
   * Creates a DataFrame based on the buffered responses
   * @param context: Context to create the DataFrame
   *  @return : The Dataframe referencing the stored responses
   */
  override def getDataFrame(context: ActionPipelineContext): DataFrame = {
    val session = context.sparkSession
    import session.implicits._
    val dataFrame = responses.toDF("responseString")
    dataFrame
  }

  override def cleanUp(): Unit = {
    responses.clear()
    super.cleanUp()
  }

  /**
   * Returns the array containing the buffered responses
   * @return the array containing the buffered responses
   */
  def getResponseBuffer : ArrayBuffer[String] = {
    responses
  }

  /**
   * Checks a evolution threshold is defined and if it is reached. If so this method creates the new response buffer and
   * initializes it with the currently buffered responses. If no threshold is set or has not been reached yet, this
   * method returns the current instance with no further action.
   *  @return The old or new response buffer instance.
   */
  override def evolve(): ODataResponseBuffer = {
    var result : ODataResponseBuffer = this
    if (setup.isDefined){
      val setupObj = setup.get
      val dirPath = setupObj.tempFileDirectoryPath.getOrElse("")
      val threshold = setupObj.memoryToFileEvolutionThresholdNumOfChars.getOrElse(-1)

      if (dirPath != "" && threshold > 0 && this.getStoredCharacterCount > threshold) {
        result = ODataResponseBufferFactory.createTempFileReponseBuffer(setup)
        result.addResponses(this.getResponseBuffer)
      }
    }
    result
  }
}

/**
 * [[ODataResponseLocalFileBuffer]] is using the local file system to buffer all received responses.
 * @param tmpDirName : Name of sub directory which will be created to only contain files of this instance
 * @param setup: The setup object which contains the configuration for this response buffer
 */
class ODataResponseLocalFileBuffer(tmpDirName: String, setup: Option[ODataResponseBufferSetup]) extends ODataResponseBuffer(setup) {

  private var dirInitialized : Boolean = false
  private var dirPath: Option[String] = None
  private var fileCount: Int = 0

  /**
   * Initializes the temporary directory by making sure, that the path exists and the target directory is empty.
   */
  protected def initTempDir(): Unit = {
    if (!dirInitialized) {
      makeTempDirIfNotExists()
      clearTempDir()
      dirInitialized = true
    }
  }

  /**
   * @return the path to the temporary directory
   */
  private def getDirectoryPath: String = {
    if (this.dirPath.nonEmpty)
      dirPath.get
    else if (setup.isDefined && setup.get.tempFileDirectoryPath.isDefined) {
      dirPath = Option(Paths.get(setup.get.tempFileDirectoryPath.get).resolve(tmpDirName + s"_${Instant.now.getEpochSecond}").toString)
      dirPath.get
    } else
      throw new ConfigurationException("TempFileDirectoryPath not configured")
  }

  /**
   * Deletes all files in the temporary directory
   */
  protected def clearTempDir(): Unit = {
    val file = new File(this.getDirectoryPath)
    val files = file.listFiles()
    files.foreach( f => if (!f.isDirectory) f.delete())
  }

  /**
   * Creates the path and the temporary directories if either does not exists.
   */
  protected def makeTempDirIfNotExists(): Unit = {
    val dirPath = Paths.get(this.getDirectoryPath)
    if (!Files.exists(dirPath)) {
      Files.createDirectories(dirPath)
    }
  }

  /**
   * Write the provided data as a new file into the temporary directory
   * @param fileName: Name of the file
   * @param data: Content of the file
   */
  protected def writeToFile(fileName: String, data: String): Unit = {
    val dirPath : java.nio.file.Path = Paths.get(this.getDirectoryPath)
    val filepath = dirPath.resolve(fileName)
    val file = new File(filepath.toString)
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(data)
    writer.close()
  }


  /**
   * @return name the current temporary file
   */
  private def getFileName() : String = {
    val curTime = DateTimeFormatter.ofPattern("yyyyMMdd'_'HHmmss").withZone(ZoneId.of("UTC")).format(Instant.now())
    s"${curTime}_${this.fileCount}.json"
  }

  /**
   * Adds the provided response to the buffer
   * @param response : The response to be added
   */
  override def addResponse(response: String): Unit = {
    initTempDir()
    writeToFile(getFileName(), response)
    super.addResponse(response)
    this.fileCount += 1
  }

  /**
   * Creates a dataframe which reads all stored temporary files
   * @param context: Context to create the DataFrame
   *  @return : The Dataframe referencing the stored responses
   */
  override def getDataFrame(context: ActionPipelineContext): DataFrame = {
    val session = context.sparkSession
    val dataFrame = session.read.option("wholetext", true).text(this.getDirectoryPath).withColumnRenamed("value", "responseString")
    dataFrame
  }

  /**
   * Deletes all temporary stored files.
   */
  override def cleanUp(): Unit = {
    clearTempDir()
    super.cleanUp()
  }

  /**
   * There is no evolution path from the FileBuffer. This method returns always the current instance
   *  @return This instance
   */
  override def evolve(): ODataResponseBuffer = {
    this
  }
}

/**
 * Factory object to create and evolve the response buffers
 */
object ODataResponseBufferFactory {

  /**
   * Name of the target table
   */
  private var tableName : String = ""

  /**
   * Setter of the target table name
   * @param tableName: Name of the target table
   */
  def setTableName(tableName: String): Unit = {
    this.tableName = tableName
  }

  /**
   * Creates a memory response buffer instance
   * @param setup: Setup for the new buffer
   * @return : an instance of a new [[ODataResponseMemoryBuffer]]
   */
  def createMemoryResponseBuffer(setup: Option[ODataResponseBufferSetup]): ODataResponseBuffer = {
    new ODataResponseMemoryBuffer(setup)
  }

  /**
   * Creates a new instance of a file response buffer (either local of for dbfs)
   * @param setup : Setup for the buffer
   * @return : New instance of [[ODataResponseLocalFileBuffer]]
   */
  def createTempFileReponseBuffer(setup: Option[ODataResponseBufferSetup]) : ODataResponseBuffer = {
    var result : ODataResponseBuffer = null

    if (setup.isDefined) {
      val fileBufferType = setup.get.tempFileBufferType

      fileBufferType.getOrElse("").toLowerCase match {
        case "local" => result = new ODataResponseLocalFileBuffer(tableName, setup)
        //case "dbfs"  => throw RuntimeException("test")
        case _ => throw ConfigurationException(s"(Unknown FileBufferType '$fileBufferType'")
      }
    }
    else {
      throw ConfigurationException("No configuration available to the create FileResponseBuffer")
    }

    result
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
                           schema: String,
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
  extends DataObject with CanCreateSparkDataFrame with CanCreateIncrementalOutput with SmartDataLakeLogger {

  private var previousState : String = ""
  private var nextState : String = ""

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
    val webserviceClient = ScalaJWebserviceClient(url, headers, timeouts, authMode = None, proxy = None, followRedirects = true)
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
  private def getBearerToken(authorization: ODataAuthorization) : ODataBearerToken = {
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
    val expiresInSecs = responseMap.apply("expires_in").asInstanceOf[BigInt].longValue()

    val expiresDateTime = Instant.now.plusSeconds(expiresInSecs)
    ODataBearerToken(token, expiresDateTime)
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
  private def getODataURL(columnNames: Seq[String], extractStart: Instant) : String = {

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

    //If there is a changeDateColumnName specified, use this column to exclude any records that were modified
    //after the start of this program.
    //If there is a previous state, add this previous add as another filter
    if (changeDateColumnName.isDefined) {
      //TODO: ChangeDateColumnName must not be used if this DataObject does not run in incremental mode
      val endRange = getODataInstantFilterLiteral(extractStart)
      filters.append(s"${changeDateColumnName.get} lt $endRange")

      if (previousState != "") {
        val startRange = getODataInstantFilterLiteral(Instant.parse(previousState))
        filters.append(s"${changeDateColumnName.get} ge $startRange")
      }
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
    val startTimeStamp = Instant.now

    if(context.phase == ExecutionPhase.Init){
      // In Init phase, return an empty DataFrame
      Seq[String]().toDF("responseString")
        .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))
        .select(explode($"response").as("record"))
        .select("record.*")
        .withColumn("created_at", current_timestamp())
    } else {
      //Convert the schema string into StructType
      val schemaType  = DataType.fromDDL(schema)

      //Extract the schema of one record
      val schemaTypeRecords = schemaType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]

      //Extract the names of the columns
      val columnNames = extractColumnNames(schemaTypeRecords)

      //Generate the URL for the first API call
      var requestUrl = getODataURL(columnNames, startTimeStamp)

      //Request the bearer token
      var bearerToken = getBearerToken(authorization.get)

      //Initialize the ResposneBufferFactory
      ODataResponseBufferFactory.setTableName(this.tableName)

      //Initialize the MemoryBuffer
      var responseBuffer = ODataResponseBufferFactory.createMemoryResponseBuffer(responseBufferSetup)
      var loopCount = 0

      //Commence the looping for each request
      while (requestUrl != "") {

        //Check if the bearer token is still valid and request a new one if not
        if (bearerToken.isExpired) {
          logger.info("Requesting new Bearer token")
          bearerToken = getBearerToken(authorization.get)
        }

        //Generate the request header
        val requestHeader = getRequestHeader(bearerToken)

        //Execute the current request
        val responseBytes = request(requestUrl, headers = requestHeader)

        //Convert the current response into a string
        val responseString = new String(responseBytes, "UTF8")

        //Add the response to the response buffer
        responseBuffer.addResponse(responseString)

        //Check if the response buffer should evolve
        responseBuffer = responseBuffer.evolve()

        //Get next URL for the next API call
        requestUrl = extractNextLink(responseString)
        loopCount += 1
      }

      logger.info(s"Loop count $loopCount")

      // create dataframe with the correct schema and add created_at column with the current timestamp
      val schemaExtended = s"struct< `@odata.context`: string, value: $schema>"

      val responsesDf = responseBuffer.getDataFrame(context)
        .select(from_json($"responseString", DataType.fromDDL(schemaExtended)).as("response"))
        .select(explode($"response.value").as("record"))
        .select("record.*")
        .withColumn("sdlb_created_on", current_timestamp())

      // put simple nextState logic below
      nextState = startTimeStamp.toString
      // return
      responsesDf
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
