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

package io.smartdatalake.workflow.dataobject

import com.github.tomakehurst.wiremock.client.{WireMock => w}
import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.testutils.{DataObjectTestSuite, TestUtil}
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.webservice.WebserviceException
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.executionMode.{DataObjectStateIncrementalMode, ProcessAllMode}
import io.smartdatalake.workflow.connection.authMode.OAuthMode
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.mockito.ArgumentMatchers.{any, isNull, eq => eqTo}
import org.mockito.{Mockito => m}

import java.io.File
import java.nio.file.Files
import java.time.Instant
import scala.collection.mutable.ArrayBuffer

class ODataResponseMemoryBufferTest extends DataObjectTestSuite {

  def init_ioc(): ODataIOC = {
    org.mockito.Mockito.mock(classOf[ODataIOC])
  }

  def init_sut(ioc: ODataIOC = new ODataIOC(), threshold: Int = 9999, tableName: String = null): ODataResponseMemoryBuffer = {
    val context = this.contextExec
    val setup = ODataResponseBufferSetup(Some("TEMPFILEPATH"), Some(threshold))

    if (tableName != null) {
      setup.setActionName(tableName)
    }

    new ODataResponseMemoryBuffer(setup, context, ioc)
  }

  test("ODataResponseMemoryBuffer - addResponse") {
    val sut = init_sut()

    sut.addResponse("TEST1")
    assert(sut.getResponseBuffer == ArrayBuffer("TEST1"))
    assert(sut.getStoredCharacterCount == 5)

    sut.addResponse("TEST2")
    assert(sut.getResponseBuffer == ArrayBuffer("TEST1", "TEST2"))
    assert(sut.getStoredCharacterCount == 10)
  }

  test("ODataResponseMemoryBuffer - addResponses") {
    val sut = init_sut()

    sut.addResponses(Array("TEST1", "TEST2"))
    assert(sut.getResponseBuffer == ArrayBuffer("TEST1", "TEST2"))
    assert(sut.getStoredCharacterCount == 10)
  }

  test("ODataResponseMemoryBuffer - getDataFrame") {
    val sut = init_sut()

    sut.addResponse("TEST1")
    sut.addResponse("TEST2")
    sut.addResponse("TEST3")

    val df = sut.getDataFrame
    val df_data = df.collect()

    assert(df.schema.fieldNames sameElements Array("responseString"))

    val rec1 = df_data(0)
    assert(rec1.getString(0) == "TEST1")

    val rec2 = df_data(1)
    assert(rec2.getString(0) == "TEST2")

    val rec3 = df_data(2)
    assert(rec3.getString(0) == "TEST3")
  }


  test("ODataResponseMemoryBuffer - cleanUp") {
    val sut = init_sut()
    sut.addResponse("TEST1")

    sut.cleanUp()

    assert(sut.getResponseBuffer == ArrayBuffer[String]())
    assert(sut.getStoredCharacterCount == 0)
  }

  test("ODataResponseMemoryBuffer - switchIfNecessary - still under threshold") {
    //val bufferMock = org.mockito.Mockito.mock(classOf[ODataResponseBuffer])
    //m.doReturn(bufferMock).when(ioc.newODataResponseFileBufferByType(org.mockito.ArgumentMatchers.any[String])
    //(ioc.newODataResponseFileBufferByType _).when().returns(bufferMock)
    //val newBuffer = mockito.Mockito.mock(classOf[ODataResponseDBFSFileBuffer])
    //ODataResponseBufferFactory.injectTestInstance(newBuffer)

    val ioc = init_ioc()
    val sut = init_sut(ioc)
    sut.addResponse("TEST")

    val result = sut.switchIfNecessary()

    assert(result == sut)
    m.verify(ioc, m.never()).newODataResponseFileBuffer(any[String], any[ODataResponseBufferSetup], any[ActionPipelineContext])
  }

  test("ODataResponseMemoryBuffer - switchIfNecessary - new buffer") {
    val context = m.mock(classOf[ActionPipelineContext])
    val setup = ODataResponseBufferSetup(Some("TEMPFILEPATH"), Some(3))
    setup.setActionName("TABLE")

    val ioc = init_ioc()
    val newBuffer = m.mock(classOf[ODataResponseFileBuffer])
    m.when(ioc.newODataResponseFileBuffer("TABLE", setup, context)).thenReturn(newBuffer)
    val sut = new ODataResponseMemoryBuffer(setup, context, ioc)

    sut.addResponse("TEST")
    val result = sut.switchIfNecessary()

    assert(result == newBuffer)
    m.verify(ioc, m.times(1)).newODataResponseFileBuffer("TABLE", setup, context)
  }


  test("ODataResponseMemoryBuffer - switchIfNecessary - above threshold but no path") {
    val context = m.mock(classOf[ActionPipelineContext])
    val setup = ODataResponseBufferSetup(None, Some(3))
    setup.setActionName("TABLE")

    val ioc = init_ioc()
    val sut = new ODataResponseMemoryBuffer(setup, context, ioc)

    sut.addResponse("TEST")
    val result = sut.switchIfNecessary()

    assert(result == sut)
    m.verify(ioc, m.never()).newODataResponseFileBuffer(any[String], any[ODataResponseBufferSetup], any[ActionPipelineContext])
  }

  test("ODataResponseLocalFileBuffer - getDirectoryPath") {
  }
}

class ODataResponseFileBufferTest extends DataObjectTestSuite {

  def init_ioc_mock(): ODataIOC = {

    val instant = Instant.ofEpochSecond(1726124260)

    val mock_ioc = org.mockito.Mockito.mock(classOf[ODataIOC])
    m.doReturn(instant, Seq.empty: _*).when(mock_ioc).getInstantNow

    mock_ioc

  }

  def init_context(): ActionPipelineContext = {
    m.mock(classOf[ActionPipelineContext])
  }

  def init_sut_spy(ioc:ODataIOC,  bufferType: String = "BUFFERTYPE", path: String = "PATH", limit: Int = 3, context: Option[ActionPipelineContext] = None, fileSystem: Option[org.apache.hadoop.fs.FileSystem] = None): ODataResponseFileBuffer = {
    val setup = ODataResponseBufferSetup(Some("PATH"), Some(3))

    val filesystem_mock = fileSystem.getOrElse(m.mock(classOf[org.apache.hadoop.fs.FileSystem]))

    m.doReturn(filesystem_mock, Seq.empty: _*).when(ioc).newHadoopFsWithConf(any[org.apache.hadoop.fs.Path], any[ActionPipelineContext])

    val sut = new ODataResponseFileBuffer("TMPDIR", setup, context.getOrElse(init_context()), ioc)
    m.spy(sut)
  }

  test("ODataResponseFileBufferTest - initTemporaryDirectory") {
    val ioc = init_ioc_mock()
    val sut = init_sut_spy(ioc)

    m.doNothing().when(sut).clearTemporaryDirectory()
    m.doNothing().when(sut).makeTempDirIfNotExists()

    sut.initTemporaryDirectory()
    m.verify(sut, m.times(1)).clearTemporaryDirectory()
    m.verify(sut, m.times(1)).makeTempDirIfNotExists()

    sut.initTemporaryDirectory()
    m.verify(sut, m.times(1)).clearTemporaryDirectory()
    m.verify(sut, m.times(1)).makeTempDirIfNotExists()
  }

  test("ODataResponseFileBufferTest - makeTempDirIfNotExists") {
    val ioc = init_ioc_mock()
    val filesystem = m.mock(classOf[org.apache.hadoop.fs.FileSystem])
    val mock_path = m.mock(classOf[org.apache.hadoop.fs.Path])
    m.doReturn(mock_path, Seq.empty: _*).when(ioc).newHadoopPath(any[String])
    val sut = init_sut_spy(ioc, fileSystem = Some(filesystem))

    assert(sut.getFileSystem == filesystem)

    sut.makeTempDirIfNotExists()

    m.verify(filesystem, m.times(1)).mkdirs(isNull[org.apache.hadoop.fs.Path])
  }

  test("ODataResponseFileBufferTest - cleanUp") {
    val ioc = init_ioc_mock()
    val sut = init_sut_spy(ioc)

    m.doNothing().when(sut).clearTemporaryDirectory()

    sut.cleanUp()

    m.verify(sut, m.times(1)).clearTemporaryDirectory()
  }

  test("ODataResponseFileBufferTest - clearTemporaryDirectory - when exists") {
    val ioc = init_ioc_mock()
    val sut = init_sut_spy(ioc)
    val filesystem = sut.getFileSystem

    m.doReturn(true, Seq.empty: _*).when(filesystem).exists(any[org.apache.hadoop.fs.Path])

    sut.clearTemporaryDirectory()

    m.verify(filesystem, m.times(1)).exists(any[org.apache.hadoop.fs.Path])
    m.verify(filesystem, m.times(1)).delete(any[org.apache.hadoop.fs.Path], eqTo(true))
  }

  test("ODataResponseFileBufferTest - clearTemporaryDirectory - when not exists") {
    val ioc = init_ioc_mock()
    val sut = init_sut_spy(ioc)
    val filesystem = sut.getFileSystem

    m.doReturn(false, Seq.empty: _*).when(filesystem).exists(any[org.apache.hadoop.fs.Path])

    sut.clearTemporaryDirectory()

    m.verify(filesystem, m.times(1)).exists(any[org.apache.hadoop.fs.Path])
    m.verify(filesystem, m.times(0)).delete(any[org.apache.hadoop.fs.Path], any[Boolean])
  }

  test("ODataResponseFileBufferTest - writeToFile") {
    val ioc = init_ioc_mock()
    val sut = init_sut_spy(ioc)
    val filesystem = sut.getFileSystem
    val path_mock = m.mock(classOf[org.apache.hadoop.fs.Path])

    m.doNothing().when(sut).initTemporaryDirectory()
    m.doReturn(path_mock, Seq.empty: _*).when(ioc).newHadoopPath(any[org.apache.hadoop.fs.Path], eqTo("FILENAME"))


    sut.writeToFile("FILENAME", "CONTENT")

    m.verify(sut, m.times(1)).initTemporaryDirectory()
    m.verify(ioc, m.times(1)).writeHadoopFile(eqTo(path_mock), eqTo("CONTENT"), eqTo(filesystem))
  }

  test("ODataResponseFileBufferTest - generateFileName") {
    val ioc = init_ioc_mock()
    val sut = init_sut_spy(ioc)

    m.doReturn(42, Seq.empty: _*).when(sut).getResponseCount

    val result = sut.generateFileName()

    assert(result == "42.json")
  }

  test("ODataResponseFileBufferTest - addResponse") {
    val ioc = init_ioc_mock()
    val sut = init_sut_spy(ioc)

    m.doReturn("FILENAME", Seq.empty: _*).when(sut).generateFileName()
    m.doNothing().when(sut).writeToFile(any[String], any[String])

    sut.addResponse("RESPONSE")

    m.verify(sut, m.times(1)).writeToFile("FILENAME", "RESPONSE")
  }

  test("ODataResponseFileBufferTest - getDataFrame") {
    val ioc = init_ioc_mock()
    val context = init_context()
    val session = m.mock(classOf[SparkSession])
    val reader = m.mock(classOf[DataFrameReader])
    val dataframe = m.mock(classOf[org.apache.spark.sql.DataFrame])
    val path = m.mock(classOf[org.apache.hadoop.fs.Path])

    m.doReturn(session, Seq.empty: _*).when(context).sparkSession
    m.doReturn(reader, Seq.empty: _*).when(session).read
    m.doReturn(reader, Seq.empty: _*).when(reader).option(any[String], any[Boolean])

    m.doReturn("PATH", Seq.empty: _*).when(path).toString
    m.doReturn(path, Seq.empty: _*).when(ioc).newHadoopPath(any[String], any[String])
    m.doReturn(dataframe, Seq.empty: _*).when(reader).text(any[String])
    m.doReturn(dataframe, Seq.empty: _*).when(dataframe).withColumnRenamed("value", "responseString")

    val sut = init_sut_spy(ioc, context = Some(context))
    val result = sut.getDataFrame

    assert(result == dataframe)
  }

  test("ODataResponseFileBufferTest - switchIfNecessary") {
    val ioc = init_ioc_mock()
    val sut = init_sut_spy(ioc)

    val result = sut.switchIfNecessary()

    assert(result == sut)
  }
}

class ODataDataObjectUnitTest extends DataObjectTestSuite {

  test("getODataURL basic") {
    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
    )

    val result = sut.getODataURL(List("ColumnA", "ColumnB"), actionPipelineContext)

    assert(result == "http://localhost:8080/dataapi/api/data/v9.2/testSource?$select=ColumnA%2CColumnB")
  }

  test("getODataURL with state") {
    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
      , incrementalOutputExpr = Some("lastModified")
    )

    sut.setState(Some("PREVIOUSSTATE"))
    val result = sut.getODataURL(List("ColumnA", "ColumnB"), actionPipelineContext)

    assert(result == "http://localhost:8080/dataapi/api/data/v9.2/testSource?$select=ColumnA%2CColumnB&$filter=lastModified+gt+PREVIOUSSTATE")
  }

  test("getODataURL with state and source filter") {
    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
      , incrementalOutputExpr = Some("lastModified")
      , sourceFilters = Some("type eq TEST")
    )

    sut.setState(Some("4242424242"))
    val result = sut.getODataURL(List("ColumnA", "ColumnB"), actionPipelineContext)

    assert(result == "http://localhost:8080/dataapi/api/data/v9.2/testSource?$select=ColumnA%2CColumnB&$filter=%28type+eq+TEST%29+and+lastModified+gt+4242424242")
  }

  test("getODataURL with maxrecordcount") {
    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
      , maxRecordCount = Some(9999)
    )

    val result = sut.getODataURL(List("ColumnA", "ColumnB"), actionPipelineContext)

    assert(result == "http://localhost:8080/dataapi/api/data/v9.2/testSource?$select=ColumnA%2CColumnB&$top=9999")
  }

  test("getSparkDataFrame in init phase") {
    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Init, currentAction = Some(action_mock))


    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
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
    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))
    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    sut.validateConfiguration(actionPipelineContext)
  }

  test("validateConfiguration - non-incremental mode with incrementalOutputExpr") {
    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))
    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
      , incrementalOutputExpr = Some("FOOBAR")
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    sut.validateConfiguration(actionPipelineContext)
  }

  test("validateConfiguration - incremental mode with correct setup") {
    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))
    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType), StructField("IncColumn", StringType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
      , incrementalOutputExpr = Some("IncColumn")
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    sut.validateConfiguration(actionPipelineContext)
  }

  test("validateConfiguration - incremental mode with no incColumn") {
    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))
    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType), StructField("IncColumn", StringType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
      , incrementalOutputExpr = None
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    assertThrows[ConfigurationException] {
      sut.validateConfiguration(actionPipelineContext)
    }
  }

  test("validateConfiguration - incremental mode with no incColumn in schema") {
    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))
    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
      , incrementalOutputExpr = Some("incColumn")
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    assertThrows[ConfigurationException] {
      sut.validateConfiguration(actionPipelineContext)
    }
  }
}

class ODataDataObjectComponentTest extends DataObjectTestSuite {


  test("Simple Test without special options and only two records") {
    val port = 8080
    val httpsPort = 8443
    val host = "127.0.0.1"
    val server = TestUtil.startWebservice(host, port, httpsPort)
    val auth_response = """{"access_token":"ACCESS_TOKEN_FOO_BAR", "expires_in":4242}"""

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
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))
    
    val sut = ODataDataObject(
        id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    val resultDf = sut.getSparkDataFrame(Seq.empty)(actionPipelineContext)
    val resultData = resultDf.collect()

    assert(resultData.length == 2)

    val record1 = resultData(0)
    assert(record1.getString(0) == "FOOBAR_1A")
    assert(record1.getInt(1) == 1)

    val record2 = resultData(1)
    assert(record2.getString(0) == "FOOBAR_2A")
    assert(record2.getInt(1) == 2)

    server.stop()
  }

  test("With state") {
    val port = 8080
    val httpsPort = 8443
    val host = "127.0.0.1"
    val server = TestUtil.startWebservice(host, port, httpsPort)
    val auth_response = """{"access_token":"ACCESS_TOKEN_FOO_BAR", "expires_in":4242}"""

    w.stubFor(w.post(w.urlEqualTo("/tenantid/oauth2/v2.0/token"))
      .withRequestBody(w.equalTo("grant_type=client_credentials&client_id=FooBarID&client_secret=FooBarPWD&scope=Scope"))
      .willReturn(w.aResponse().withBody(auth_response))
    )

    val response1 = """{"@odata.context": "FOOBAR CONTEXT", "value": [{"@odata.id":"ODATAID1", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_1A", "ColumnB":1, "modifiedOn":"2024-06-10T10:03:40.000Z"}, {"@odata.id":"ODATAID2", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_2A", "ColumnB":2, "modifiedOn":"2024-06-10T10:03:44.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withHeader("Authorization", w.equalTo(s"Bearer ACCESS_TOKEN_FOO_BAR"))
      .withQueryParam("$select", w.equalTo("ColumnA,ColumnB,modifiedOn"))
      .withQueryParam("$filter", w.equalTo("modifiedOn gt 2024-06-10T08:00:00.000Z"))
      .willReturn(w.aResponse().withBody(response1))
    )

    val ioc_spy = m.spy(new ODataIOC())

    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType), StructField("modifiedOn", StringType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
      , incrementalOutputExpr = Some("modifiedOn")
    )
    sut.injectIOC(ioc_spy)
    sut.setState(Some("2024-06-10T08:00:00.000Z"))

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

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

    server.stop()
  }

  test("With three pages with memory buffer") {
    val port = 8080
    val httpsPort = 8443
    val host = "127.0.0.1"
    val server = TestUtil.startWebservice(host, port, httpsPort)
    val auth_response = """{"access_token":"ACCESS_TOKEN_FOO_BAR", "expires_in":4242}"""

    w.stubFor(w.post(w.urlEqualTo("/tenantid/oauth2/v2.0/token"))
      .withRequestBody(w.equalTo("grant_type=client_credentials&client_id=FooBarID&client_secret=FooBarPWD&scope=Scope"))
      .willReturn(w.aResponse().withBody(auth_response))
    )

    val response1 = """{"@odata.context": "FOOBAR CONTEXT", "@odata.nextLink":"http://localhost:8080/dataapi/api/data/v9.2/testSource?$page=2", "value": [{"@odata.id":"ODATAID1", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_1A", "ColumnB":1, "modifiedOn":"2024-06-10T10:03:45.000Z"}, {"@odata.id":"ODATAID2", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_2A", "ColumnB":2, "modifiedOn":"2024-06-10T10:03:46.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withHeader("Authorization", w.equalTo(s"Bearer ACCESS_TOKEN_FOO_BAR"))
      .withQueryParam("$select", w.equalTo("ColumnA,ColumnB,modifiedOn"))
      .withQueryParam("$filter", w.equalTo("modifiedOn gt 2024-06-10T10:03:44.000Z"))
      .willReturn(w.aResponse().withBody(response1))
    )

    val response2 = """{"@odata.context": "FOOBAR CONTEXT", "@odata.nextLink":"http://localhost:8080/dataapi/api/data/v9.2/testSource?$page=3", "value": [{"@odata.id":"ODATAID3", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_3A", "ColumnB":3, "modifiedOn":"2024-06-10T10:03:47.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withHeader("Authorization", w.equalTo(s"Bearer ACCESS_TOKEN_FOO_BAR"))
      .withQueryParam("$page", w.equalTo("2"))
      .willReturn(w.aResponse().withBody(response2))
    )

    val response3 = """{"@odata.context": "FOOBAR CONTEXT", "value": [{"@odata.id":"ODATAID4", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_4A", "ColumnB":4, "modifiedOn":"2024-06-10T10:03:48.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withHeader("Authorization", w.equalTo(s"Bearer ACCESS_TOKEN_FOO_BAR"))
      .withQueryParam("$page", w.equalTo("3"))
      .willReturn(w.aResponse().withBody(response3))
    )

    val ioc_spy = m.spy(new ODataIOC())

    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType), StructField("modifiedOn", StringType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
      , incrementalOutputExpr = Some("modifiedOn")
    )
    sut.injectIOC(ioc_spy)
    sut.setState(Some("2024-06-10T10:03:44.000Z"))

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

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

    server.stop()
  }

  test("With three pages with temp file buffer") {
    val port = 8080
    val httpsPort = 8443
    val host = "127.0.0.1"
    val server = TestUtil.startWebservice(host, port, httpsPort)
    val auth_response = """{"access_token":"ACCESS_TOKEN_FOO_BAR", "expires_in":4242}"""

    w.stubFor(w.post(w.urlEqualTo("/tenantid/oauth2/v2.0/token"))
      .withRequestBody(w.equalTo("grant_type=client_credentials&client_id=FooBarID&client_secret=FooBarPWD&scope=Scope"))
      .willReturn(w.aResponse().withBody(auth_response))
    )

    val response1 = """{"@odata.context": "FOOBAR CONTEXT", "@odata.nextLink":"http://localhost:8080/dataapi/api/data/v9.2/testSource?$page=2", "value": [{"@odata.id":"ODATAID1", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_1A", "ColumnB":1, "modifiedOn":"2024-06-10T10:03:45.000Z"}, {"@odata.id":"ODATAID2", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_2A", "ColumnB":2, "modifiedOn":"2024-06-10T10:03:46.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withHeader("Authorization", w.equalTo(s"Bearer ACCESS_TOKEN_FOO_BAR"))
      .withQueryParam("$select", w.equalTo("ColumnA,ColumnB,modifiedOn"))
      .withQueryParam("$filter", w.equalTo("modifiedOn gt 2024-06-10T10:03:44.000Z"))
      .willReturn(w.aResponse().withBody(response1))
    )

    val response2 = """{"@odata.context": "FOOBAR CONTEXT", "@odata.nextLink":"http://localhost:8080/dataapi/api/data/v9.2/testSource?$page=3", "value": [{"@odata.id":"ODATAID3", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_3A", "ColumnB":3, "modifiedOn":"2024-06-10T10:03:47.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withHeader("Authorization", w.equalTo(s"Bearer ACCESS_TOKEN_FOO_BAR"))
      .withQueryParam("$page", w.equalTo("2"))
      .willReturn(w.aResponse().withBody(response2))
    )

    val response3 = """{"@odata.context": "FOOBAR CONTEXT", "value": [{"@odata.id":"ODATAID4", "@odata.etag":"ODATA_ETAG", "@odata.editLink":"ODATA_EDITLINK", "ColumnA":"FOOBAR_4A", "ColumnB":4, "modifiedOn":"2024-06-10T10:03:48.000Z"}]}"""
    w.stubFor(w.get(w.urlMatching("/dataapi/api/data/v9.2/testSource.*"))
      .withHeader("Accept", w.equalTo("application/json"))
      .withHeader("Content-Type", w.equalTo("application/json; charset=UTF-8"))
      .withHeader("Authorization", w.equalTo("Bearer ACCESS_TOKEN_FOO_BAR"))
      .withQueryParam("$page", w.equalTo("3"))
      .willReturn(w.aResponse().withBody(response3))
    )

    val ioc_spy = m.spy(new ODataIOC())
    val now = Instant.parse("2024-06-09T23:00:00Z")
    m.doReturn(now, Seq.empty: _*).when(ioc_spy).getInstantNow

    val temp_dir_base = Files.createTempDirectory("odatatest_filebuffer").toFile
    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some(temp_dir_base.getAbsolutePath), memoryToFileSwitchThresholdNumOfChars = Some(20))
    val temp_dir = new File(temp_dir_base, "test-dataobject_1717974000")

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType), StructField("modifiedOn", StringType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
      , incrementalOutputExpr = Some("modifiedOn")
    )
    sut.injectIOC(ioc_spy)
    sut.setState(Some("2024-06-10T10:03:44.000Z"))

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

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
    server.stop()
  }

  test("With connection problems and retry success") {
    val port = 8080
    val httpsPort = 8443
    val host = "127.0.0.1"
    val server = TestUtil.startWebservice(host, port, httpsPort)
    val auth_response = """{"access_token":"ACCESS_TOKEN_FOO_BAR", "expires_in":4242}"""

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

    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))


    val resultDf = sut.getSparkDataFrame(Seq.empty)(actionPipelineContext)
    val resultData = resultDf.collect()

    assert(resultData.length == 2)

    val record1 = resultData(0)
    assert(record1.getString(0) == "FOOBAR_1A")
    assert(record1.getInt(1) == 1)

    val record2 = resultData(1)
    assert(record2.getString(0) == "FOOBAR_2A")
    assert(record2.getInt(1) == 2)

    server.stop()
  }

  test("With connection problems and no retry success") {
    val port = 8080
    val httpsPort = 8443
    val host = "127.0.0.1"
    val server = TestUtil.startWebservice(host, port, httpsPort)

    w.stubFor(w.post(w.urlEqualTo("/tenantid/oauth2/v2.0/token"))
      .willReturn(w.aResponse().withFault(com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER))
    )

    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    //val context_mock = m.mock(classOf[ActionPipelineContext])
    //m.doReturn(this.session,Seq.empty: _*).when(context_mock).sparkSession
    var exceptionCaught = false

    try {
      sut.getSparkDataFrame(Seq.empty)(actionPipelineContext)
    }
    catch
    {
      case x: Exception => exceptionCaught = true
    }
    finally {
      server.stop()
    }

    assert(exceptionCaught)
  }

  test("Regression test - Missing incremental column in schema") {

    val mock_auto = m.mock(classOf[OAuthMode])
    val mock_buffer_setup = m.mock(classOf[ODataResponseBufferSetup])

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("annotationid", StringType), StructField("_objectid_value", StringType), StructField("createdon", StringType), StructField("documentbody", StringType)))))
      , baseUrl = "NOT RELEVANT"
      , tableName = "annotations"
      , authorization = Some(mock_auto)
      , timeouts = None
      , responseBufferSetup = Some(mock_buffer_setup)
      , incrementalOutputExpr = Some("modifiedon")
      , sourceFilters = Some("objecttypecode eq 'msdyn_transcript'")
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(DataObjectStateIncrementalMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Init, currentAction = Some(action_mock))

    assertThrows[ConfigurationException] {
      sut.prepare(actionPipelineContext)
    }
  }

  test("Test with error message on remote errors") {
    val port = 8080
    val httpsPort = 8443
    val host = "127.0.0.1"
    val server = TestUtil.startWebservice(host, port, httpsPort)

    w.stubFor(w.post(w.urlEqualTo("/tenantid/oauth2/v2.0/token"))
      .willReturn(w.aResponse().withStatus(400).withBody("FoobarErrorMessage"))
    )

    val auth_setup = OAuthMode(StringOrSecret("http://localhost:8080/tenantid/oauth2/v2.0/token"), StringOrSecret("FooBarID"), StringOrSecret("FooBarPWD"), StringOrSecret("Scope"))
    val buffer_setup = ODataResponseBufferSetup(tempFileDirectoryPath = Some("C:\\temp\\"), memoryToFileSwitchThresholdNumOfChars = Some(1000))

    val sut = ODataDataObject(
      id = DataObjectId("test-dataobject")
      , schema = Some(SparkSchema(StructType(Seq(StructField("ColumnA", StringType), StructField("ColumnB", IntegerType)))))
      , baseUrl = "http://localhost:8080/dataapi/api/data/v9.2/"
      , tableName = "testSource"
      , authorization = Some(auth_setup)
      , timeouts = None
      , responseBufferSetup = Some(buffer_setup)
    )

    val action_mock = m.mock(classOf[CopyAction])
    m.doReturn(Some(ProcessAllMode()),Seq.empty: _*).when(action_mock).executionMode
    val actionPipelineContext = TestUtil.getDefaultActionPipelineContext(this.session).copy(phase = ExecutionPhase.Exec, currentAction = Some(action_mock))

    var exceptionCaught : Exception = null

    try {
      sut.getSparkDataFrame(Seq.empty)(actionPipelineContext)
    }
    catch
    {
      case x: Exception => exceptionCaught = x
    }
    finally {
      server.stop()
    }

    assert(exceptionCaught.isInstanceOf[WebserviceException])

    val webServiceExceptionCaught = exceptionCaught.asInstanceOf[WebserviceException]
    assert(webServiceExceptionCaught.responseBody.get == "FoobarErrorMessage")
    assert(webServiceExceptionCaught.getMessage == "Webservice Request failed with error <400> - FoobarErrorMessage")
  }
}
