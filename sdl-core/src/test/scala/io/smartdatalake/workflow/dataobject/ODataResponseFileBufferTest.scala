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

import io.smartdatalake.testutils.DataObjectTestSuite
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.mockito.ArgumentMatchers.{any, isNull, eq => eqTo}
import org.mockito.{Mockito => m}

import java.time.Instant

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

  def init_sut_spy(ioc: ODataIOC, bufferType: String = "BUFFERTYPE", path: String = "PATH", limit: Int = 3, context: Option[ActionPipelineContext] = None, fileSystem: Option[org.apache.hadoop.fs.FileSystem] = None): ODataResponseFileBuffer = {
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
