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
package io.smartdatalake.app

import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.webservice.SttpWebserviceClient
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject._
import org.apache.spark.sql.SparkSession
import org.eclipse.jetty.websocket.api.{Session, WebSocketAdapter}
import org.eclipse.jetty.websocket.client.WebSocketClient
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.net.URI
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success}

/**
 * This tests use configuration test/resources/configstatusinfo/application.conf
 */
class SmartDataLakeBuilderStatusInfoTest extends AnyFunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  private val sdlb = DefaultSmartDataLakeBuilder

  before {
    sdlb.instanceRegistry.clear()
  }

  // Note that this test produces a StackOverflowError in the Log with JDK11. The test succeeds nevertheless. Details see below.
  test("sdlb run with statusinfoserver: Test connectivity of REST API and Websocket") {

    val feedName = "test"
    // setup input DataObject
    val srcDO = CsvFileDataObject("src1", "target/src1")(sdlb.instanceRegistry)
    val dfSrc1 = Seq("testData").toDF("testColumn")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc1), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = Seq(
      getClass.getResource("/configstatusinfo/application.conf").getPath)
    )
    //Run SDLB
    sdlb.run(sdlConfig)

    //Create Client Websocket that tries to establish connection with SDLB Job
    val receivedMessages: ListBuffer[String] = ListBuffer()
    // The socket that receives events
    class UnitTestSocket extends WebSocketAdapter with SmartDataLakeLogger {
      override def onWebSocketConnect(sess: Session): Unit = {}

      override def onWebSocketText(message: String): Unit = {
        receivedMessages += message
      }

      override def onWebSocketClose(statusCode: Int, reason: String): Unit = {}

      override def onWebSocketError(cause: Throwable): Unit = {}
    }
    val client = new WebSocketClient
    client.start()
    val session = client.connect(new UnitTestSocket, URI.create("ws://localhost:4440/ws/")).get


    //Verify Rest API context endpoint is reachable and returns correct results
    val webserviceDOContext = WebserviceFileDataObject("dummy", url = s"http://localhost:4440/api/v1/context/")(sdlb.instanceRegistry)
    val webserviceClientContext = SttpWebserviceClient(webserviceDOContext)
    webserviceClientContext.get() match {
      case Failure(exception) =>
        throw exception
      case Success(value) =>
        val str = new String(value, StandardCharsets.UTF_8)
        assert(str.contains("\"feedSel\":\"test\""))
    }

    //Verify Rest API state endpoint is reachable and returns correct results
    //Known Issue: if you run this test with Java 11.0.4, you may see a Stackoverflow error.
    //The problem arises after the second call to the webservice (no matter what the call is)
    // If you encounter it, either: Comment out one of the Calls to the webservice, or use a different JDK version to run the test.
    val webserviceDOState = WebserviceFileDataObject("dummy", url = s"http://localhost:4440/api/v1/state/")(sdlb.instanceRegistry)
    val webserviceClientState = SttpWebserviceClient(webserviceDOState)
    webserviceClientState.get() match {
      case Failure(exception) =>
        throw exception
      case Success(value) =>
        val str = new String(value, StandardCharsets.UTF_8)
        assert(str.contains("\"actionsState\":{\"Action~a\":{\"executionId\":{\"runId\":1,\"attemptId\":1}"))
    }
    //Verify a client websocket can connect
    assert(receivedMessages.head.contains("Hello from io.smartdatalake.communication.statusinfo.websocket.StatusInfoSocket"))

    session.close()
    client.stop()
  }
}

