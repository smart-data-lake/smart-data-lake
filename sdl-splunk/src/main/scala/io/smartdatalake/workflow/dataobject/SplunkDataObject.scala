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

import java.time.Duration.ofMinutes
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ofPattern
import java.time.{Duration, LocalDateTime}

import com.splunk._
import com.typesafe.config.Config
import configs.ConfigReader
import configs.syntax._
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.SplunkConnection
import io.smartdatalake.workflow.dataobject.SplunkFormatter.{fromSplunkStringFormat, toSplunkStringFormat}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import resource._

import scala.annotation.tailrec
import scala.collection.JavaConverters._


/**
 * [[DataObject]] of type Splunk.
 * Provides details to an action to access Splunk logs.
 */
case class SplunkDataObject(override val id: DataObjectId,
                             params: SplunkParams,
                             connectionId: ConnectionId,
                             override val metadata: Option[DataObjectMetadata] = None
                           )(implicit instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with SplunkService {

  /**
   * Connection defines host, port and credentials in central location
   */
  private val connection = getConnection[SplunkConnection](connectionId)

  private implicit val rowSeqEncoder: Encoder[Seq[Row]] = Encoders.kryo[Seq[Row]]
  private implicit val queryTimeIntervalEncoder: Encoder[QueryTimeInterval] = Encoders.kryo[QueryTimeInterval]

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit spark: SparkSession, context: ActionPipelineContext): DataFrame = {
    readFromSplunk(params)
  }

  override def prepare(implicit session: SparkSession): Unit = try {
    connection.test()
  } catch {
    case ex: Throwable => throw ConnectionTestException(s"($id) Can not connect. Error: ${ex.getMessage}", ex)
  }

  private def readFromSplunk(params: SplunkParams)(implicit spark: SparkSession): DataFrame = {
    val queryTimeIntervals = splitQueryTimes(params.queryFrom, params.queryTo, params.queryTimeInterval).repartition(params.parallelRequests)
    val searchResultRdd = queryTimeIntervals.map(interval => readRowsFromSplunk(interval, params)).as[Seq[Row]].rdd
    val searchResultRddFlattened = searchResultRdd.flatMap(identity)
    val searchResultDf = spark.createDataFrame(searchResultRddFlattened, params.schema)
    searchResultDf
  }

  private def readRowsFromSplunk(interval: QueryTimeInterval, params: SplunkParams): Seq[Row] = {
    val splunk = connection.connectToSplunk
    try {
      val queryValidated = validateQuery(params.query)
      val searchArgs = createJobExportArgs(interval.from, interval.to)
      val searchResult = readFromSplunk(queryValidated, searchArgs, splunk)
      val searchResultRows = transformToRows(searchResult, params.schema)
      searchResultRows
    } finally {
      if (splunk != null) {
        splunk.logout()
      }
    }
  }

  private def transformToRows(searchResults: Seq[Map[String, String]], schema: StructType): Seq[Row] = {
    searchResults.map(item => {
      if (schema.length == 1) { // no schema or only a one-column schema has been defined -> concatenate all values to a string
        List(item.toString)
      } else { // only pick defined column values
        schema.fieldNames.foldRight(List[String]())((name, acc) => item.getOrElse(name, "") :: acc)
      }
    }).map(Row.fromSeq(_))
  }

  private[dataobject] def splitQueryTimes(from: LocalDateTime,
                                     to: LocalDateTime,
                                     interval: Duration)(implicit spark: SparkSession): Dataset[QueryTimeInterval] = {
    @tailrec
    def splitQueryTimesAccum(from: LocalDateTime,
                             to: LocalDateTime,
                             accum: Seq[QueryTimeInterval]): Seq[QueryTimeInterval] = {
      if (from.isAfter(to)) {
        accum
      } else if (from.plus(interval).isAfter(to) || from.plus(interval).isEqual(to)) {
        accum :+ QueryTimeInterval(from, to)
      } else {
        splitQueryTimesAccum(from.plus(interval), to, accum :+ QueryTimeInterval(from, from.plus(interval)))
      }
    }

    import spark.implicits._
    val queryTimes = splitQueryTimesAccum(from, to, Seq.empty)
    queryTimes.toDS
  }

  private def validateQuery(query: String): String = {
    query match {
      case q: String if !q.contains("index=") => throw new IllegalArgumentException("Splunk queries should define the index the search should work on.")
      case q: String if !q.startsWith("search") => "search " + query
      case _ => query
    }
  }

  override def factory: FromConfigFactory[DataObject] = SplunkDataObject
}

object SplunkDataObject extends FromConfigFactory[DataObject] {

  private val SF_TIME_FORMAT = "yyyy-MM-dd HH:mm"

  /**
   * Parse a [[LocalDateTime]] from a [[SplunkDataObject]] SDL config.
   *
   * @param value a string specifying the [[LocalDateTime]] in the expected format.
   * @return  a new [[LocalDateTime]] instance representing the specified value.
   */
  def parseConfigDateTime(value: String): LocalDateTime = {
    val valueStripped = value.stripPrefix("'").stripSuffix("'")
    LocalDateTime.parse(valueStripped, ofPattern(SF_TIME_FORMAT))
  }

  /**
   * Parse a [[Duration]] from a [[SplunkDataObject]] SDL config.
   *
   * @param value an integer specifying the [[Duration]] in the expected format.
   * @return  a new [[Duration]] instance representing the specified value.
   */
  def parseConfigDuration(value: Int): Duration = {
    ofMinutes(value)
  }

  /**
   * A [[ConfigReader]] that reads [[SplunkParams]] values.
   *
   * SplunkParams have special semantics for Duration which are covered with this reader.
   */
  implicit val splunkParamsReader: ConfigReader[SplunkParams] = ConfigReader.fromConfigTry { c =>
    SplunkParams.fromConfig(c)
  }

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SplunkDataObject = {
    extract[SplunkDataObject](config)
  }
}

case class QueryTimeInterval(from: LocalDateTime, to: LocalDateTime)

object SplunkFormatter {

  val SPLUNK_DATETIME_FORMATTER: DateTimeFormatter = ofPattern("yyyy-MM-dd'T'HH:mm:ss")

  def toSplunkStringFormat(value: LocalDateTime): String = SPLUNK_DATETIME_FORMATTER.format(value)

  def fromSplunkStringFormat(value: String): LocalDateTime = LocalDateTime.parse(value, SPLUNK_DATETIME_FORMATTER)

}

private[smartdatalake] trait SplunkService extends SmartDataLakeLogger {

  def createJobExportArgs(queryFrom: LocalDateTime, queryTo: LocalDateTime): JobExportArgs = {
    val args = new JobExportArgs
    args.setEarliestTime(toSplunkStringFormat(queryFrom)) //Inclusive
    args.setLatestTime(toSplunkStringFormat(queryTo)) //Exclusive
    args.setSearchMode(JobExportArgs.SearchMode.NORMAL)
    args.setOutputMode(JobExportArgs.OutputMode.JSON)
    args.setOutputTimeFormat("%Y-%m-%d %H:%M:%S.%3N %:z")
    args
  }

  private def getJobExportArg(key: String, args: JobExportArgs): String = args.getOrDefault(key, "unknown").toString

  def readFromSplunk(query: String, searchArgs: JobExportArgs, splunk: Service): Seq[Map[String, String]] = {
    val startTime = System.currentTimeMillis()
    val searchResults = managed(splunk.export(query, searchArgs)) acquireAndGet { export =>
      val reader = new MultiResultsReaderJson(export)
      val results = reader.iterator.asScala.flatMap(_.iterator().asScala.map(_.asScala.toMap)).toArray // toArray copies the result to an array before closing
      reader.close()
      results
    }
    val endTime = System.currentTimeMillis()
    val duration = (endTime - startTime) / 1e3 // millis -> seconds
    logger.info(s"Reading #${searchResults.length} rows of splunk data took $duration s for query " +
      s"from [${fromSplunkStringFormat(getJobExportArg("earliest_time", searchArgs))}] " +
      s"to [${fromSplunkStringFormat(getJobExportArg("latest_time", searchArgs))}]")
    searchResults
  }
}

case class SplunkParams(
                         query: String,
                         queryFrom: LocalDateTime,
                         queryTo: LocalDateTime,
                         queryTimeInterval: Duration = ofMinutes(10),
                         columnNames: Seq[String] = Seq("_raw", "_time"),
                         parallelRequests: Int = 2
                       ) {
  val schema: StructType = StructType(columnNames.toArray.map(name => StructField(name, StringType, nullable = true)).toList)
}

object SplunkParams {
  def fromConfig(config: Config): SplunkParams = {
    implicit val splunkLocalDateTimeReader: ConfigReader[LocalDateTime] = ConfigReader.fromTry { (c, p) =>
      SplunkDataObject.parseConfigDateTime(c.getString(p))
    }
    implicit val splunkDurationReader: ConfigReader[Duration] = ConfigReader.fromTry { (c, p) =>
      SplunkDataObject.parseConfigDuration(c.getInt(p))
    }
    config.extract[SplunkParams].value
  }
}
