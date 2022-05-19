package io.smartdatalake.workflow.dataobject

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.AuthMode
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.WebserviceMethod.WebserviceMethod
import io.smartdatalake.util.webservice.{ScalaJWebserviceClient, WebserviceException, WebserviceMethod}
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{DefaultFormats, Formats}

import java.time.Instant
import scala.annotation.tailrec
import scala.util.{Failure, Success}

case class HttpTimeoutConfig(connectionTimeoutMs: Int, readTimeoutMs: Int)
case class DepartureQueryParameters(airport: String, begin: Long, end: Long)

case class State(airport: String, nextBegin: Long)

/**
 * [[DataObject]] to call webservice and return response as a DataFrame
 */
case class CustomWebserviceDataObject(override val id: DataObjectId,
                                      schema: String,
                                      queryParameters: Seq[DepartureQueryParameters],
                                      additionalHeaders: Map[String,String] = Map(),
                                      timeouts: Option[HttpTimeoutConfig] = None,
                                      authMode: Option[AuthMode] = None,
                                      baseUrl : String,
                                      nRetry: Int = 1,
                                      override val metadata: Option[DataObjectMetadata] = None
                                     )
                                     (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateSparkDataFrame with CanCreateIncrementalOutput with SmartDataLakeLogger {

  private var previousState : Seq[State] = Seq()
  private var nextState : Seq[State] = Seq()

  private val now = Instant.now.getEpochSecond

  @tailrec
  private def request(url: String, method: WebserviceMethod = WebserviceMethod.Get, body: String = "", retry: Int = nRetry) : Array[Byte] = {
    val webserviceClient = ScalaJWebserviceClient(url, additionalHeaders, timeouts, authMode, proxy = None, followRedirects = true)
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

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): DataFrame = {
    import org.apache.spark.sql.functions._
    implicit val formats: Formats = DefaultFormats
    val session = context.sparkSession
    import session.implicits._

    val byte2String = udf((payload: Array[Byte]) => new String(payload))

    // if time interval is more than a week, set end config to 4 days after begin
    def checkQueryParameters(queryParameters: Seq[DepartureQueryParameters]) = {
      queryParameters.map{
        param =>
          val diff = param.end - param.begin
          if(diff / (3600*24) >= 7) {
            param.copy(end=param.begin+3600*24*4)
          } else {
            param
          }
      }
    }

    if(context.phase == ExecutionPhase.Init){
      // simply return an empty data frame
      Seq[String]().toDF("responseString")
        .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))
        .select(explode($"response").as("record"))
        .select("record.*")
        .withColumn("created_at", current_timestamp())
    } else {
      // place the new implementation of currentQueryParameters below this line
      // if we have query parameters in the state we will use them from now on
      val currentQueryParameters = if (previousState.isEmpty) checkQueryParameters(queryParameters) else checkQueryParameters(previousState.map{
        x => DepartureQueryParameters(x.airport, x.nextBegin, now)
      })

      // given the query parameters, generate all requests
      val departureRequests = currentQueryParameters.map(
        param => s"${baseUrl}?airport=${param.airport}&begin=${param.begin}&end=${param.end}"
      )
      // make requests
      val departuresResponses = departureRequests.map(request(_))
      // create dataframe with the correct schema and add created_at column with the current timestamp
      val departuresDf = departuresResponses.toDF("responseBinary")
        .withColumn("responseString", byte2String($"responseBinary"))
        .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))
        .select(explode($"response").as("record"))
        .select("record.*")
        .withColumn("created_at", current_timestamp())

      // put simple nextState logic below
      if(previousState.isEmpty){
        nextState = currentQueryParameters.map(params => State(params.airport, params.end))
      } else {
        nextState = previousState.map(params => State(params.airport, now))
      }
      // return
      departuresDf
    }
  }

  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {
    implicit val formats: Formats = DefaultFormats
    previousState = state.map(s => JsonMethods.parse(s).extract[Seq[State]]).getOrElse(Seq())
  }

  override def getState: Option[String] = {
    implicit val formats: Formats = DefaultFormats
    Some(Serialization.write(nextState))
  }

  override def factory: FromConfigFactory[DataObject] = CustomWebserviceDataObject
}

object CustomWebserviceDataObject extends FromConfigFactory[DataObject] with SmartDataLakeLogger {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomWebserviceDataObject = {
    extract[CustomWebserviceDataObject](config)
  }
}
