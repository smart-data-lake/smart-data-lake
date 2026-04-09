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
import org.mockito.ArgumentMatchers.any
import org.mockito.{Mockito => m}
import org.scalatest.BeforeAndAfter

import scala.collection.mutable.ArrayBuffer

class ODataResponseMemoryBufferTest extends DataObjectTestSuite with BeforeAndAfter {

  def init_ioc(): ODataIOC = {
    org.mockito.Mockito.mock(classOf[ODataIOC])
  }

  def init_sut(ioc: ODataIOC = new ODataIOC(), threshold: Int = 9999, tableName: String = null): ODataResponseMemoryBuffer = {
    val context = this.contextExec
    val setup = Some(ODataResponseBufferSetup(Some("TEMPFILEPATH"), Some(threshold)))

    if (tableName != null) {
      setup.get.setActionName(tableName)
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

  // TODO: dont know how to fix
  ignore("ODataResponseMemoryBuffer - getDataFrame") {
    val sut = init_sut()

    sut.addResponse("TEST1")
    sut.addResponse("TEST2")
    sut.addResponse("TEST3")

    /*
    val df = sut.getDataFrame
    val df_data = df.collect()

    assert(df.schema.fieldNames sameElements Array("responseString"))

    val rec1 = df_data(0)
    assert(rec1.getString(0) == "TEST1")

    val rec2 = df_data(1)
    assert(rec2.getString(0) == "TEST2")

    val rec3 = df_data(2)
    assert(rec3.getString(0) == "TEST3")
    */
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
    val setup = Some(ODataResponseBufferSetup(Some("TEMPFILEPATH"), Some(3)))
    setup.get.setActionName("TABLE")

    val ioc = init_ioc()
    val newBuffer = m.mock(classOf[ODataResponseFileBuffer])
    m.when(ioc.newODataResponseFileBuffer("TABLE", setup.get, context)).thenReturn(newBuffer)
    val sut = new ODataResponseMemoryBuffer(setup, context, ioc)

    sut.addResponse("TEST")
    val result = sut.switchIfNecessary()

    assert(result == newBuffer)
    m.verify(ioc, m.times(1)).newODataResponseFileBuffer("TABLE", setup.get, context)
  }


  test("ODataResponseMemoryBuffer - switchIfNecessary - above threshold but no path") {
    val context = m.mock(classOf[ActionPipelineContext])
    val setup = Some(ODataResponseBufferSetup(None, Some(3)))
    setup.get.setActionName("TABLE")

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
