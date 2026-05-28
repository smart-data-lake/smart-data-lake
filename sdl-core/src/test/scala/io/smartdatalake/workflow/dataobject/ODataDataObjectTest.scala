/*
 * Smart Data Lake Builder - Build your data lake the smart way.
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

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.{WireMock => w}
import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.{DataObjectTestSuite, TestUtil}
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.webservice.{HttpRequestError, HttpTimeoutConfig}
import io.smartdatalake.workflow.ExecutionPhase
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.executionMode.{DataObjectStateIncrementalMode, ProcessAllMode}
import io.smartdatalake.workflow.connection.authMode.OAuthMode
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.mockito.{Mockito => m}

import java.nio.file.Files
import java.time.Instant

class ODataDataObjectUnitTest extends DataObjectTestSuite {

  test("getODataURL basic") {
    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry)
      .copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
    )

    val result = sut.getODataURL(List("ColumnA", "ColumnB"), actionPipelineContext)

    assert(result == "http://localhost:8080/dataapi/api/data/v9.2/testSource?$select=ColumnA%2CColumnB")
  }

  test("getODataURL with state") {
    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
      , incrementalOutputExpr = Some("lastModified")
    )

    sut.setState(Some("PREVIOUSSTATE"))
    val result = sut.getODataURL(List("ColumnA", "ColumnB"), actionPipelineContext)

    assert(result == "http://localhost:8080/dataapi/api/data/v9.2/testSource?$select=ColumnA%2CColumnB&$filter=lastModified+gt+PREVIOUSSTATE")
  }

  test("getODataURL with state and source filter") {
    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
      , incrementalOutputExpr = Some("lastModified")
      , sourceFilters = Some("type eq TEST")
    )

    sut.setState(Some("4242424242"))
    val result = sut.getODataURL(List("ColumnA", "ColumnB"), actionPipelineContext)

    assert(result == "http://localhost:8080/dataapi/api/data/v9.2/testSource?$select=ColumnA%2CColumnB&$filter=%28type+eq+TEST%29+and+lastModified+gt+4242424242")
  }

  test("getODataURL with maxrecordcount") {
    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
      , maxRecordCount = Some(9999)
    )

    val result = sut.getODataURL(List("ColumnA", "ColumnB"), actionPipelineContext)

    assert(result == "http://localhost:8080/dataapi/api/data/v9.2/testSource?$select=ColumnA%2CColumnB&$top=9999")
  }

  test("getSparkDataFrame in init phase") {
    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry)
      .copy(phase = ExecutionPhase.Init, currentAction = Some(action_mock))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
    )

    val result = sut.getSparkDataFrame(Seq.empty)(actionPipelineContext)

    val resultSchema = result.schema

    val columnAIdx = resultSchema.fieldIndex("ColumnA")
    val columnBIdx = resultSchema.fieldIndex("ColumnB")

    val columnAType = resultSchema.fields(columnAIdx)
    val columnBType = resultSchema.fields(columnBIdx)

    assert(columnAType.name == "ColumnA")
    assert(columnAType.dataType.typeName == "string")

    assert(columnBType.name == "ColumnB")
    assert(columnBType.dataType.typeName == "integer")
  }

  test("validateConfiguration - non-incremental mode") {
    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    sut.validateConfiguration(actionPipelineContext)
  }

  test("validateConfiguration - non-incremental mode with incrementalOutputExpr") {
    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
      , incrementalOutputExpr = Some("FOOBAR")
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    sut.validateConfiguration(actionPipelineContext)
  }

  test("validateConfiguration - incremental mode with correct setup") {
    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType), StructField("IncColumn", StringType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
      , incrementalOutputExpr = Some("IncColumn")
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    sut.validateConfiguration(actionPipelineContext)
  }

  test("validateConfiguration - incremental mode with no incColumn") {
    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType), StructField("IncColumn", StringType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
      , incrementalOutputExpr = None
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    assertThrows[ConfigurationException] {
      sut.validateConfiguration(actionPipelineContext)
    }
  }

  test("validateConfiguration - incremental mode with no incColumn in schema") {
    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
      , incrementalOutputExpr = Some("incColumn")
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    assertThrows[ConfigurationException] {
      sut.validateConfiguration(actionPipelineContext)
    }
  }
}

class ODataDataObjectComponentTest extends DataObjectTestSuite {

  val port = 8080
  val httpsPort = 8443
  val host = "127.0.0.1"
  var server: WireMockServer = _

  override def additionalBefore(): Unit = {
    server = TestUtil.startWebservice(host, port, httpsPort)
  }

  after {
    server.stop()
  }

  test("Simple Test without special options and only two records") {
    val response1 = """{"@odata.context": "FOOBAR CONTEXT", "value": [{"@odata.id":"ODATAID1", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_1A", "ColumnB":1}, {"@odata.id":"ODATAID2", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_2A", "ColumnB":2}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withQueryParam("$select", w.equalTo("ColumnA,ColumnB"))
      .willReturn(w.aResponse().withBody(response1))
    )

    val sut = ODataDataObject(
        id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val resultDf = sut.getSparkDataFrame(Seq.empty)(actionPipelineContext)
    val resultData = resultDf.collect()

    assert(resultData.length == 2)

    val record1 = resultData(0)
    assert(record1.getString(0) == "FOOBAR_1A")
    assert(record1.getInt(1) == 1)

    val record2 = resultData(1)
    assert(record2.getString(0) == "FOOBAR_2A")
    assert(record2.getInt(1) == 2)
  }

  test("Simple Test with state") {

    val auth_response = """{"token_type":"Bearer", "access_token":"ACCESS_TOKEN_FOO_BAR", "expires_in":4242}"""

    w.stubFor(w.post(w.urlEqualTo("/tenantid/oauth2/v2.0/token"))
      .withRequestBody(w.equalTo("grant_type=client_credentials&client_id=FooBarID&client_secret=FooBarPWD&scope=Scope"))
      .willReturn(w.aResponse().withBody(auth_response))
    )

    val response1 = """{"@odata.context": "FOOBAR CONTEXT", "value": [{"@odata.id":"ODATAID1", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_1A", "ColumnB":1}, {"@odata.id":"ODATAID2", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_2A", "ColumnB":2}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withHeader("Authorization", w.equalTo(s"Bearer ACCESS_TOKEN_FOO_BAR"))
      .withQueryParam("$select", w.equalTo("ColumnA,ColumnB"))
      .willReturn(w.aResponse().withBody(response1))
    )

    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = None
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val resultDf = sut.getSparkDataFrame(Seq.empty)(actionPipelineContext)
    val resultData = resultDf.collect()

    assert(resultData.length == 2)

    val record1 = resultData(0)
    assert(record1.getString(0) == "FOOBAR_1A")
    assert(record1.getInt(1) == 1)

    val record2 = resultData(1)
    assert(record2.getString(0) == "FOOBAR_2A")
    assert(record2.getInt(1) == 2)
  }

  test("Simple Test without authMode") {

    val response1 = """{"@odata.context": "FOOBAR CONTEXT", "value": [{"@odata.id":"ODATAID1", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_1A", "ColumnB":1, "modifiedOn":"2024-06-10T10:03:40.000Z"}, {"@odata.id":"ODATAID2", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_2A", "ColumnB":2, "modifiedOn":"2024-06-10T10:03:44.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withQueryParam("$select", w.equalTo("ColumnA,ColumnB,modifiedOn"))
      .withQueryParam("$filter", w.equalTo("modifiedOn gt 2024-06-10T08:00:00.000Z"))
      .willReturn(w.aResponse().withBody(response1))
    )

    val ioc_spy = m.spy(new ODataIOC())

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType), StructField("modifiedOn", StringType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
      , incrementalOutputExpr = Some("modifiedOn")
    )
    sut.injectIOC(ioc_spy)
    sut.setState(Some("2024-06-10T08:00:00.000Z"))

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val resultDf = sut.getSparkDataFrame(Seq.empty)(actionPipelineContext)
    val resultData = resultDf.collect()

    assert(resultData.length == 2)

    val record1 = resultData(0)
    assert(record1.getString(0) == "FOOBAR_1A")
    assert(record1.getInt(1) == 1)
    assert(record1.getString(2) == "2024-06-10T10:03:40.000Z")

    val record2 = resultData(1)
    assert(record2.getString(0) == "FOOBAR_2A")
    assert(record2.getInt(1) == 2)
    assert(record2.getString(2) == "2024-06-10T10:03:44.000Z")

    val newState = sut.getState
    assert(newState.get == "2024-06-10T10:03:44.000Z")
  }

  test("With three pages with memory buffer") {

    val response1 = """{"@odata.context": "FOOBAR CONTEXT", "@odata.nextLink":"http://localhost:8080/dataapi/api/data/v9.2/testSource?$page=2", "value": [{"@odata.id":"ODATAID1", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_1A", "ColumnB":1, "modifiedOn":"2024-06-10T10:03:45.000Z"}, {"@odata.id":"ODATAID2", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_2A", "ColumnB":2, "modifiedOn":"2024-06-10T10:03:46.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withQueryParam("$select", w.equalTo("ColumnA,ColumnB,modifiedOn"))
      .withQueryParam("$filter", w.equalTo("modifiedOn gt 2024-06-10T10:03:44.000Z"))
      .willReturn(w.aResponse().withBody(response1))
    )

    val response2 = """{"@odata.context": "FOOBAR CONTEXT", "@odata.nextLink":"http://localhost:8080/dataapi/api/data/v9.2/testSource?$page=3", "value": [{"@odata.id":"ODATAID3", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_3A", "ColumnB":3, "modifiedOn":"2024-06-10T10:03:47.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withQueryParam("$page", w.equalTo("2"))
      .willReturn(w.aResponse().withBody(response2))
    )

    val response3 = """{"@odata.context": "FOOBAR CONTEXT", "value": [{"@odata.id":"ODATAID4", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_4A", "ColumnB":4, "modifiedOn":"2024-06-10T10:03:48.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withQueryParam("$page", w.equalTo("3"))
      .willReturn(w.aResponse().withBody(response3))
    )

    val ioc_spy = m.spy(new ODataIOC())

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType), StructField("modifiedOn", StringType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
      , incrementalOutputExpr = Some("modifiedOn")
    )
    sut.injectIOC(ioc_spy)
    sut.setState(Some("2024-06-10T10:03:44.000Z"))

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val resultDf = sut.getSparkDataFrame(Seq.empty)(actionPipelineContext)
    val resultData = resultDf.collect()

    assert(resultData.length == 4)

    val record1 = resultData(0)
    assert(record1.getString(0) == "FOOBAR_1A")
    assert(record1.getInt(1) == 1)
    assert(record1.getString(2) == "2024-06-10T10:03:45.000Z")

    val record2 = resultData(1)
    assert(record2.getString(0) == "FOOBAR_2A")
    assert(record2.getInt(1) == 2)
    assert(record2.getString(2) == "2024-06-10T10:03:46.000Z")

    val record3 = resultData(2)
    assert(record3.getString(0) == "FOOBAR_3A")
    assert(record3.getInt(1) == 3)
    assert(record3.getString(2) == "2024-06-10T10:03:47.000Z")

    val record4 = resultData(3)
    assert(record4.getString(0) == "FOOBAR_4A")
    assert(record4.getInt(1) == 4)
    assert(record4.getString(2) == "2024-06-10T10:03:48.000Z")

    val newState = sut.getState
    assert(newState.get == "2024-06-10T10:03:48.000Z")
  }

  test("With three pages with temp file buffer") {

    val response1 = """{"@odata.context": "FOOBAR CONTEXT", "@odata.nextLink":"http://localhost:8080/dataapi/api/data/v9.2/testSource?$page=2", "value": [{"@odata.id":"ODATAID1", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_1A", "ColumnB":1, "modifiedOn":"2024-06-10T10:03:45.000Z"}, {"@odata.id":"ODATAID2", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_2A", "ColumnB":2, "modifiedOn":"2024-06-10T10:03:46.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withQueryParam("$select", w.equalTo("ColumnA,ColumnB,modifiedOn"))
      .withQueryParam("$filter", w.equalTo("modifiedOn gt 2024-06-10T10:03:44.000Z"))
      .willReturn(w.aResponse().withBody(response1))
    )

    val response2 = """{"@odata.context": "FOOBAR CONTEXT", "@odata.nextLink":"http://localhost:8080/dataapi/api/data/v9.2/testSource?$page=3", "value": [{"@odata.id":"ODATAID3", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_3A", "ColumnB":3, "modifiedOn":"2024-06-10T10:03:47.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withQueryParam("$page", w.equalTo("2"))
      .willReturn(w.aResponse().withBody(response2))
    )

    val response3 = """{"@odata.context": "FOOBAR CONTEXT", "value": [{"@odata.id":"ODATAID4", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_4A", "ColumnB":4, "modifiedOn":"2024-06-10T10:03:48.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withQueryParam("$page", w.equalTo("3"))
      .willReturn(w.aResponse().withBody(response3))
    )

    val ioc_spy = m.spy(new ODataIOC())
    val now = Instant.parse("2024-06-09T23:00:00Z")
    m.doReturn(now, Seq.empty.toIndexedSeq: _*).when(ioc_spy).getInstantNow

    val temp_dir_base = Files.createTempDirectory("odatatest_filebuffer").toFile
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some(temp_dir_base.getAbsolutePath), memoryToFileSwitchThresholdNumOfChars = Some(20))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType), StructField("modifiedOn", StringType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
      , incrementalOutputExpr = Some("modifiedOn")
    )
    sut.injectIOC(ioc_spy)
    sut.setState(Some("2024-06-10T10:03:44.000Z"))

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val resultDf = sut.getSparkDataFrame(Seq.empty)(actionPipelineContext)
    val resultData = resultDf.collect()

    assert(resultData.length == 4)

    val record1 = resultData(0)
    assert(record1.getString(0) == "FOOBAR_1A")
    assert(record1.getInt(1) == 1)
    assert(record1.getString(2) == "2024-06-10T10:03:45.000Z")

    val record2 = resultData(1)
    assert(record2.getString(0) == "FOOBAR_2A")
    assert(record2.getInt(1) == 2)
    assert(record2.getString(2) == "2024-06-10T10:03:46.000Z")

    val record3 = resultData(2)
    assert(record3.getString(0) == "FOOBAR_3A")
    assert(record3.getInt(1) == 3)
    assert(record3.getString(2) == "2024-06-10T10:03:47.000Z")

    val record4 = resultData(3)
    assert(record4.getString(0) == "FOOBAR_4A")
    assert(record4.getInt(1) == 4)
    assert(record4.getString(2) == "2024-06-10T10:03:48.000Z")

    val newState = sut.getState
    assert(newState.get == "2024-06-10T10:03:48.000Z")

    val numOfTempFiles1 = temp_dir_base.listFiles().length
    assert(numOfTempFiles1 == 1)

    sut.postRead(null)

    val numOfTempFiles2 = temp_dir_base.listFiles().length
    assert(numOfTempFiles2 == 0)

    temp_dir_base.delete()
  }

  test("With connection problems and retry success") {
    val auth_response = """{"token_type":"Bearer", "access_token":"ACCESS_TOKEN_FOO_BAR", "expires_in":4242}"""

    w.stubFor(w.post(w.urlEqualTo("/tenantid/oauth2/v2.0/token"))
      .inScenario("FailTheFirstTime")
      .willReturn(w.aResponse().withFault(com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER))
      .willSetStateTo("Step2")
    )

    w.stubFor(w.post(w.urlEqualTo("/tenantid/oauth2/v2.0/token"))
      .inScenario("FailTheFirstTime")
      .whenScenarioStateIs("Step2")
      .withRequestBody(w.equalTo("grant_type=client_credentials&client_id=FooBarID&client_secret=FooBarPWD&scope=Scope"))
      .willReturn(w.aResponse().withBody(auth_response))
    )

    val response1 = """{"@odata.context": "FOOBAR CONTEXT", "value": [{"@odata.id":"ODATAID1", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_1A", "ColumnB":1}, {"@odata.id":"ODATAID2", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_2A", "ColumnB":2}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withHeader("Authorization", w.equalTo(s"Bearer ACCESS_TOKEN_FOO_BAR"))
      .withQueryParam("$select", w.equalTo("ColumnA,ColumnB"))
      .willReturn(w.aResponse().withBody(response1))
    )

    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"),
      timeouts = Some(HttpTimeoutConfig(connectionTimeoutMs = 500, readTimeoutMs = 500)))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = None
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))


    val resultDf = sut.getSparkDataFrame(Seq.empty)(actionPipelineContext)
    val resultData = resultDf.collect()

    assert(resultData.length == 2)

    val record1 = resultData(0)
    assert(record1.getString(0) == "FOOBAR_1A")
    assert(record1.getInt(1) == 1)

    val record2 = resultData(1)
    assert(record2.getString(0) == "FOOBAR_2A")
    assert(record2.getInt(1) == 2)
  }

  test("With connection problems and no retry success") {

    w.stubFor(w.post(w.urlEqualTo("/tenantid/oauth2/v2.0/token"))
      .willReturn(w.aResponse().withFault(com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER))
    )

    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"),
      timeouts = Some(HttpTimeoutConfig(connectionTimeoutMs = 500, readTimeoutMs = 500)))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    intercept[Exception](sut.getSparkDataFrame(Seq.empty)(actionPipelineContext))
  }

  test("Regression test - Missing incremental column in schema") {

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("annotationid", StringType), StructField("_objectid_value", StringType), StructField("createdon", StringType), StructField("documentbody", StringType)))))
      , baseUrl = "NOT RELEVANT"
      , tableName = "annotations"
      , authMode = None
      , timeouts = None
      , responseBufferSetup = None
      , incrementalOutputExpr = Some("modifiedon")
      , sourceFilters = Some("objecttypecode eq 'msdyn_transcript'")
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Init, currentAction = Some(action_mock))

    assertThrows[ConfigurationException] {
      sut.prepare(actionPipelineContext)
    }
  }

  test("Test with error message on remote errors") {

    w.stubFor(w.post(w.urlEqualTo("/tenantid/oauth2/v2.0/token"))
      .willReturn(w.aResponse().withStatus(400).withBody("FoobarErrorMessage"))
    )

    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authMode = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = None
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty.toIndexedSeq: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val error = intercept[HttpRequestError](sut.getSparkDataFrame(Seq.empty)(actionPipelineContext))
    assert(error.err == "FoobarErrorMessage")
    assert(error.code == 400)
  }
}
