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

import java.time.{Duration, LocalDateTime}

import com.splunk.{JobExportArgs, Service}
import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.definitions.BasicAuthMode
import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.workflow.connection.{SplunkConnection, SplunkConnectionService}
import org.apache.spark.sql.Row
import org.scalatest.Assertions

class SplunkDataObjectTest extends DataObjectTestSuite {

  val queryRawColumnNames: String = "\"_raw\", \"_time\""

  test("splunk queries without table mappings should yield a dataframe having only _raw and _time fields") {
    // prepare
    val splunkDO = createDO(SplunkStubQueries.queryRaw, queryRawColumnNames)

    // system under test
    val sut = createSutWithStubs(splunkDO)
    Assertions

    // run
    val resultDf = sut.getDataFrame()

    // check
    val result = resultDf.collect()
    resultDf.schema.fieldNames should contain theSameElementsAs Seq("_raw", "_time")
    result should not be empty
    val result_1 = result(0)
    result_1 should not be None
    val reference_result_1 = Row.fromSeq(SplunkStubQueries.responseRaw.head.values.toSeq)
    result_1 shouldEqual reference_result_1
  }

  private val queryWithTableMappingColumnNames = "\"flight_from\", \"flight_to\", \"startLocation\", \"endLocation\", \"dateOfTravel\", \"timeOfTravel\", \"_time\", \"long_from\", \"lat_from\", \"long_to\", \"lat_to\", \"trips_gfid\", \"language\""

  test("splunk queries with a table mapping should yield a dataframe having all defined fields in the schema") {
    // prepare
    val splunkDO = createDO(SplunkStubQueries.queryWithTableMapping, queryWithTableMappingColumnNames)

    // system under test
    val sut = createSutWithStubs(splunkDO)

    // run
    val resultDf = sut.getDataFrame()
    val result = resultDf.collect()

    // check
    resultDf.schema.fieldNames should have size 13
    result should not be empty
    val result_1 = resultDf.schema.fieldNames.zip(result(0).toSeq).toMap
    result_1 should not be None
    val reference_result_1 = SplunkStubQueries.responseWithTableMapping.head
    result_1 shouldEqual reference_result_1
  }

  test("splitting query times should yield intervals of the defined size") {
    // prepare
    val now = LocalDateTime.of(2019, 7, 10, 11, 42)
    val tomorrow = now.plusDays(1)
    val fifteenMinutes = Duration.ofMinutes(15)

    // system under test
    val sut = createDO()

    sut.splitQueryTimes(now, now.minusMinutes(1), fifteenMinutes).collect() shouldBe empty
    sut.splitQueryTimes(now, now, fifteenMinutes).collect() should contain theSameElementsAs Seq(QueryTimeInterval(now, now))

    val oneDayDifference = sut.splitQueryTimes(now, tomorrow, fifteenMinutes).collect()
    oneDayDifference should have size 96

    sut.params.parallelRequests shouldEqual 10
  }

  private def createDO(query: String = "TEST_PURPOSES_ONLY", columnNames: String = "TEST_PURPOSES_ONLY") = {
    instanceRegistry.register(SplunkConnection( "con1", "splunk.test.com", 8888, BasicAuthMode("CLEAR#REPLACEME", "CLEAR#REPLACEME")))
    val config = ConfigFactory.parseString(
      s"""
         |{
         | id = src1
         | type = splunk
         | connectionId = con1
         | params {
         |   query = "$query"
         |   query-from = "'2019-07-09 00:00'"
         |   query-to = "2019-07-09 00:01"
         |   query-time-interval = 10
         |   column-names = [$columnNames]
         |   parallel-requests = 10
         | }
         |}
         """.stripMargin)
    SplunkDataObject.fromConfig(config, instanceRegistry)
  }

  private def createSutWithStubs(ais: SplunkDataObject): SplunkDataObject with SplunkStub = {
    val con = instanceRegistry.get[SplunkConnection](ConnectionId("con1"))
    val conStub = new SplunkConnection("con1sut", con.host, con.port, con.authMode) with SplunkConnectionStub
    instanceRegistry.register(conStub)
    new SplunkDataObject( "src1", ais.params, "con1sut") with SplunkStub
  }
}

object SplunkStubQueries {

  val queryRaw: String = """search index=p_hafas REQUESTPARAMS \"STATISTICS\" FS12345 OR FS1234_Serv | sort -_time"""
  val responseRaw: Seq[Map[String, String]] = Seq(Map(
    ("_raw", "Tue Jul  9 00:00:59 2019 (5399) [FS1234_Serv] STATISTICS: clientProduct=SDL&REQUESTPARAMS=&startLocation=S1#STA#N:LosAngeles#ID:8502306#X:8449896#Y:47169151#RI:1|&endLocation=Z1#STA#N:Helsinki#ID:8501656#X:7626412#Y:46379790#RI:1|&dateOfTravel=11072019&timeOfTravel=8:11&&requestType=XMLCONRECONSTRUCTION&clientIp=---&language=d&"),
    ("_time", "2019-07-09 00:00:59.000 +02:00")
  ))

  val queryWithTableMapping: String = """
    !search index=p_hafas REQUESTPARAMS \"STATISTICS\" FS12345 OR FS1234_Serv
    ! | rex field=_raw \"startLocation=S1#STA#N:(?P<startLocation>[^\\#]+).+endLocation=Z1#STA#N:(?P<endLocation>[^\\#]+).+dateOfTravel=(?P<dateOfTravel>[0-9]{8})*.+timeOfTravel=(?P<timeOfTravel>(([0-9]?\\d|2[0-9]):([0-9]?\\d|2[0-9])))\"
    ! | table flight_from flight_to startLocation viaLocation endLocation dateOfTravel timeOfTravel _time long_from lat_from long_to lat_to trips_gfid language
    ! | sort -_time
    !""".stripMargin('!').replaceAll(System.lineSeparator, "")

  val responseWithTableMapping: Seq[Map[String, String]] = Seq(Map(
    ("long_from", "8473097"),
    ("long_to", "8540193"),
    ("flight_from", "LAX"),
    ("endLocation", "Helsinki, Airport"),
    ("timeOfTravel", "0:00"),
    ("dateOfTravel", "09072019"),
    ("lat_to", "47378168"),
    ("lat_from", "47437101"),
    ("_time", "2019-07-09 00:00:59.000 +02:00"),
    ("startLocation", "Los Angeles, Airport"),
    ("flight_to", "HEL"),
    ("trips_gfid", ""),
    ("language", "")
  ))

  def escapeJava(str: String): String = str.replace("""\""", """\\""").replace(""""""", """\"""")

}

// instead of using mockito et. al. we're using a "poor man's stub approach" by creating a self-baked trait.
trait SplunkStub extends SplunkService {
  override def readFromSplunk(query: String, searchArgs: JobExportArgs, splunk: Service): Seq[Map[String, String]] = {
    query match {
      case q if SplunkStubQueries.escapeJava(q).equals(SplunkStubQueries.queryRaw) => SplunkStubQueries.responseRaw
      case q if SplunkStubQueries.escapeJava(q).equals(SplunkStubQueries.queryWithTableMapping) => SplunkStubQueries.responseWithTableMapping
      case _ => super.readFromSplunk(query, searchArgs, splunk) // otherwise, invoke the actual implementation
    }
  }
}

trait SplunkConnectionStub extends SplunkConnectionService {
  override def connectToSplunk: Service = null
}
