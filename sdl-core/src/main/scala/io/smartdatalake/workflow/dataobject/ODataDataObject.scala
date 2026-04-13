/*
 * sdl-core - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.{ExpressionUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.SparkExpressionUtil
import io.smartdatalake.util.webservice.WebserviceMethod.WebserviceMethod
import io.smartdatalake.util.webservice.{HttpProxyConfig, HttpTimeoutConfig, SttpWebserviceClient, WebserviceMethod}
import io.smartdatalake.workflow.action.executionMode.DataObjectStateIncrementalMode
import io.smartdatalake.workflow.connection.SparkClassicConnection
import io.smartdatalake.workflow.connection.authMode.HttpAuthMode
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed.getSparkSession
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.generic.{CanCreateIncrementalOutput, UserDefinedSchema}
import io.smartdatalake.workflow.dataobject.spark.CanCreateSparkDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, expr, max}
import org.apache.spark.sql.types._
import org.json4s.{DefaultFormats, Formats}

import java.io.{BufferedWriter, File, FileWriter}
import java.net.URLEncoder
import java.nio.file.{Files, Paths}
import java.time.Instant
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.typeOf
import scala.util.{Failure, Success}

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
  def newODataResponseMemoryBuffer(setup: Option[ODataResponseBufferSetup], context: ActionPipelineContext) : ODataResponseMemoryBuffer = {
    new ODataResponseMemoryBuffer(setup, context, this)
  }

  /**
   * Create a new instance of the ODataResponseFileBuffer class
   * @param tmpDirName path to the temporary direction to write to
   * @param setup buffer configuration
   * @param context pipeline context
   * @return ODataResponseDBFSFileBuffer
   */
  def newODataResponseFileBuffer(tmpDirName: String, setup: ODataResponseBufferSetup, context: ActionPipelineContext) : ODataResponseFileBuffer =
  {
    new ODataResponseFileBuffer(tmpDirName, setup, context, this)
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
   * Calls the static method exists of the java.nio.file.Files class
   * @param path path to check
   * @return true if exists
   */
  def fileExists(path:java.nio.file.Path): Boolean = {
    Files.exists(path)
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
 * [[DataObject]] of type OData.
 *
 * @param schema : Schema of the expected output. It can be provided as a string in the form of <<array< struct< columnA:string, columnB: integer ... >>,
 *               as ddl-File, caseClass, java Bean, XSD-File, JSON-Schema-File or Avro-Schema File. For more information, please see the SDLB-docuemntation
 *               on specifying schemas (https://smartdatalake.ch/docs/reference/schema#specifying-schema).
 * @param baseUrl : Base URL of the OData Service like https://xxx.crm4.dynamics.com/api/data/v9.2/
 * @param tableName : Name of the table which needs to be accessed
 * @param sourceFilters : Optional. OData filter string which will be applied to the access operation like "objecttypecode eq 'task' and createdon ge 2024-01-01T00:00:00.000Z"
 * @param timeouts : Optional. Timeout settings of type [[HttpTimeoutConfig]]
 * @param authMode : Optional configuration of webservice authentication. Supported `AuthMode`s are all HttpAuthModes, e.g. BasicAuthMode, OAuthMode, CustomHttpAuthMode.
 * CustomHttpAuthMode can be used to implement a custom authentication protocol, e.g. AzureADClientGrantAuthMode in sdl-azure module.
 * @param authorization : Deprecated, use #authMode instead
 * @param incrementalOutputExpr: Optional. Name of the column which will be used to read incrementally (like "modifiedon"). The column must be part of the schema. If this column is originally of datatype Timestamp in the source, it should be marked as a string in the schema to prevent casting problems.
 * @param nRetry: Optional. Number of retries after a failed attempt, default = 1
 * @param responseBufferSetup: Optional. Setup for response buffers of type [[ODataResponseBufferSetup]]
 * @param maxRecordCount: Optional. Maximum number of records to be extracted.
 */
case class ODataDataObject(override val id: DataObjectId,
                           override val schema: Option[GenericSchema],
                           baseUrl : String,
                           tableName: String,
                           sourceFilters: Option[String] = None,
                           timeouts: Option[HttpTimeoutConfig] = None,
                           proxy: Option[HttpProxyConfig] = None,
                           authMode: Option[HttpAuthMode] = None,
                           @Deprecated @deprecated("Use authMode instead", "2.8.1") authorization: Option[HttpAuthMode] = None,
                           followRedirects: Boolean = false,
                           incrementalOutputExpr: Option[String] = None,
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
   * @param retries : Number of retries if http request fails. Default is 0 retries.
   * @return the response to the request
   */
  private def request(url: String
                      , method: WebserviceMethod = WebserviceMethod.Get
                      , headers: Map[String, String] = Map()
                      , body: String = ""
                      , mimeType: String = "application/json"
                      , retries: Int = 0
                     ) : Array[Byte] = {
    val webserviceClient = SttpWebserviceClient(url = url, additionalHeaders = headers, timeouts = timeouts, authMode = authMode.orElse(authorization), proxy = proxy, followRedirects = followRedirects, retries = retries, sttpBackendOption = None)
    val webserviceResult = method match {
      case WebserviceMethod.Get =>
        webserviceClient.get()
      case WebserviceMethod.Post =>
        val bodyArray = body.getBytes("UTF8")
        webserviceClient.post(bodyArray, mimeType)
    }

    webserviceResult match {
      case Success(c) => c
      case Failure(e) => throw e
    }
  }

  /**
   * Creates the required headers for the main requests
   *
   * @return Map instance with the headers
   */
  private def getRequestHeader: Map[String, String] = {
    Map("Content-Type" -> "application/json; charset=UTF-8", "Accept" -> "application/json")
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
   * Creates the URL to be called by combining all necessary parameters into the url
   * @param columnNames : Names of the expected columns
   * @return OData request URL
   */
  def getODataURL(columnNames: Seq[String], context: ActionPipelineContext) : String = {

    //Start with the base url and the table name
    var requestUrl = s"$baseUrl$tableName"

    //Using this ArrayBuffer oDataModifiers to collect all required parameters
    val oDataModifiers = ArrayBuffer[String]()

    //Adding a list of all required columns
    if (columnNames.nonEmpty) {
      oDataModifiers.append(s"$$select=${URLEncoder.encode(columnNames.mkString(","), "UTF-8")}")
    }

    //If there are any filters required, add them too
    val filters = getODataURLFilters(context)
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
   * @return
   */
  private def getODataURLFilters(context: ActionPipelineContext) : Option[String] = {
    val filters = ArrayBuffer[String]()

    //If there are any predefined filters configured, treat these filters as one unit and add them
    //to the filters list
    if (sourceFilters.isDefined) {
      filters.append("(" + sourceFilters.get + ")")
    }

    //If there is a incrementalOutputExpr and a previousState specified, use this column to filter only the records that
    //were modified since the last run (previousState) and the start of this run.
    if (this.isRunningIncrementally(context) & incrementalOutputExpr.isDefined & previousState != "" & previousState != null) {

      filters.append(s"${incrementalOutputExpr.get} gt $previousState")
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
   * Prepare & test [[DataObject]]'s prerequisits
   *
   * This runs during the "prepare" operation of the DAG.
   */
  override private[smartdatalake] def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    validateConfiguration(context)
  }

  /**
   * Creates the DataFrame of the received responses. This is the main method which triggers the OData-API-Calls.
   * It blocks until all data is received.
   *
   * @param partitionValues
   * @param context
   * @return
   */
  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {
    import org.apache.spark.sql.functions._
    val session = getSparkSession
    import session.implicits._

    val recordSchema = schema.get.convert(typeOf[SparkSubFeed]).asInstanceOf[SparkSchema]
    val arraySchema = ArrayType(recordSchema.inner)

    if(context.phase == ExecutionPhase.Init){
      // In Init phase, return an empty DataFrame
      Seq[String]().toDF("responseString")
        .select(from_json($"responseString", arraySchema).as("response"))
        .select(explode($"response").as("record"))
        .select("record.*")
    } else {
      //Extract the names of the columns
      val columnNames = recordSchema.columns

      //Generate the URL for the first API call
      var requestUrl = getODataURL(columnNames, context)

      responseBufferSetup.foreach{ rbs => rbs.setActionName(this.id.id) }

      //Initialize the MemoryBuffer
      this.responseBuffer = ioc.newODataResponseMemoryBuffer(responseBufferSetup, context)
      var loopCount = 0

      //Commence the looping for each request
      while (requestUrl != "") {

        //Execute the current request
        val responseBytes = request(requestUrl, headers = getRequestHeader, retries = nRetry)

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

      val responseSchema = StructType(Seq(StructField("@odata.context", StringType), StructField("value", arraySchema)))

      val responsesDf = this.responseBuffer.getDataFrame(session)
        .select(from_json($"responseString", responseSchema).as("response"))
        .select(explode($"response.value").as("record"))
        .select("record.*")

      //If the DataObject has a incrementalOutputExpr defined, the following code will determine the nextState
      //value out of the received data. If there is no new data the previousState will also be the nextState.
      if (isRunningIncrementally(context)) {
        val new_nextState = getNextODataState(responsesDf)
        if (new_nextState.isDefined) {
          nextState = new_nextState.get
        }
        else {
          nextState = previousState
        }

      }

      // return
      responsesDf
    }
  }

  def validateConfiguration(context: ActionPipelineContext): Unit = {
    if (isRunningIncrementally(context)) {
      if (incrementalOutputExpr.isEmpty) {
        throw new ConfigurationException("In execution mode DataObjectStateIncrementalMode you must configure an incrementalOutputExpr and also include it in the output schema")
      }

      val recordSchema = schema.get.convert(typeOf[SparkSubFeed]).asInstanceOf[SparkSchema]
      if (!recordSchema.inner.fieldNames.contains(incrementalOutputExpr.get)) {
        throw new ConfigurationException(s"incrementalOutputExpr '${incrementalOutputExpr.get}' must be included in the output schema")
      }
    }
  }

  /*** Determines if the calling action of this DataObject is running the DataObjectStateIncrementalMode
   *
   * @param context the current ActionPipelineContext
   * @return true if incremental, false otherwise
   */
  def isRunningIncrementally(context: ActionPipelineContext): Boolean = {
    val mode = context.currentAction.get.executionMode
    if (mode.isDefined) {
      mode.get.isInstanceOf[DataObjectStateIncrementalMode]
    }
    else {
      false
    }
  }

  /***
   * This method determines the maximum value of the incrementalOutputExpr from the received data and converts it
   * to an OData representation. The resulting string can then be used for the next query
   * @param df The data frame with the received data
   * @return OData representation of the new incrementalOutputExpr value
   */
  def getNextODataState(df: DataFrame): Option[String] = {
    val incExpr = SparkExpressionUtil.resolveExpression(this.incrementalOutputExpr.get, df.schema)
    val incExprDataType = incExpr.dataType

    var work_df = df.select(max(expr(this.incrementalOutputExpr.get)).alias("nextState"))

    incExprDataType match {
      case _: TimestampType => work_df = work_df.withColumn("nextState", date_format(col("nextState"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

      case _: DateType => work_df = work_df.withColumn("nextState", date_format(col("nextState"), "yyyy-MM-dd"))

      case _: StringType => work_df = work_df.withColumn("nextState", col("nextState"))

      case _ => work_df = work_df.withColumn("nextState", col("nextState").cast(StringType))
    }

    val result_record = work_df.collect()

    if (result_record.length > 0) {
      Some(result_record(0).getString(0))
    } else {
      None
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
