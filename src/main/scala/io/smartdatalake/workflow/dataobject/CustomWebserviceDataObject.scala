package io.smartdatalake.workflow.dataobject

import com.typesafe.config.Config
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.AuthMode
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.WebserviceMethod.WebserviceMethod
import io.smartdatalake.util.webservice.{ScalaJCustomWebserviceClient, WebserviceException, WebserviceMethod}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import io.smartdatalake.workflow.dataobject.CustomWebserviceDataObject.extract
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{DefaultFormats, Formats}

import scala.annotation.tailrec
import scala.util.{Failure, Success}
import java.time.Instant

case class HttpTimeoutConfig(connectionTimeoutMs: Int, readTimeoutMs: Int)
case class DepartureQueryParameters(airport: String, begin: Long, end: Long)

case class Departure(icao24: String,
                     firstSeen: String,
                     estDepartureAirport: String,
                     lastSeen: Int,
                     estArrivalAirport: String,
                     callsign: String,
                     estDepartureAirportHorizDistance: Option[Int],
                     estDepartureAirportVertDistance: Option[Int],
                     estArrivalAirportHorizDistance: Option[Int],
                     estArrivalAirportVertDistance: Option[Int],
                     departureAirportCandidatesCount: Int,
                     arrivalAirportCandidatesCount: Int)

/**
 * [[DataObject]] to call webservice and return response as a DataFrame
 */
case class CustomWebserviceDataObject(override val id: DataObjectId,
                                      override val metadata: Option[DataObjectMetadata] = None,
                                      additionalHeaders: Map[String,String] = Map(),
                                      queryParameters: Option[Seq[DepartureQueryParameters]] = None,
                                      timeouts: Option[HttpTimeoutConfig] = None,
                                      authMode: Option[AuthMode] = None,
                                      baseUrl : String,
                                      nRetry: Int = 1)
                                     (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with SmartDataLakeLogger {

  // check whether there are query parameters available from the config
  if(queryParameters == None){
    throw new ConfigurationException(s"($id) no query parameters available")
  }

  // if we have query parameters in the state we will use them from now on
  val currentQueryParameters = queryParameters.get

  @tailrec
  private def request(url: String, method: WebserviceMethod = WebserviceMethod.Get, body: String = "", retry: Int = nRetry) : Array[Byte] = {
    val webserviceClient = ScalaJCustomWebserviceClient(this, Some(url))
    val webserviceResult = method match {
      case WebserviceMethod.Get => webserviceClient.get()
      case WebserviceMethod.Post => webserviceClient.post(body.getBytes, "application/json")
    }
    webserviceResult match {
      case Success(c) =>
        logger.info(s"Success for request ${url}")
        c
      case Failure(e) =>
        if(retry == 0) {
          logger.error(e.getMessage, e)
          throw new WebserviceException(e.getMessage)
        }
        logger.info(s"Request will be repeated, because the server responded with: ${e.getMessage}. \nRequest retries left: ${retry-1}")
        request(url, method, body, retry-1)
    }
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues])(implicit session: SparkSession, context: ActionPipelineContext): DataFrame = {
    import org.apache.spark.sql.functions._
    implicit val formats: Formats = DefaultFormats
    import session.implicits._

    val byte2String = udf((payload: Array[Byte]) => new String(payload))

    // REPLACE BLOCK
    // given the query parameters, generate all requests
    val departureRequests = currentQueryParameters.map(
      param => s"${baseUrl}?airport=${param.airport}&begin=${param.begin}&end=${param.end}"
    )
    // make request
    val departuresResponses = departureRequests.map(request(_))
    // deserialize the result into a sequence of Departure objects
    val departures = departuresResponses.flatMap(res => JsonMethods.parse(new String(res)).extract[Seq[Departure]])
    // create dataframe and add created_at column with the current timestamp
    val departuresDf = departures.toDF
      .withColumn("created_at", current_timestamp())
    // return
    departuresDf
    // REPLACE BLOCK
  }

  override def factory: FromConfigFactory[DataObject] = CustomWebserviceDataObject
}

object CustomWebserviceDataObject extends FromConfigFactory[DataObject] with SmartDataLakeLogger {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomWebserviceDataObject = {
    extract[CustomWebserviceDataObject](config)
  }
}