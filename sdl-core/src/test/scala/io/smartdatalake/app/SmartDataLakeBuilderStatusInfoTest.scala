/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions._
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action._
import io.smartdatalake.workflow.dataobject._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.eclipse.jetty.websocket.api.{Session, WebSocketAdapter}
import org.eclipse.jetty.websocket.client.WebSocketClient
import org.scalatest.concurrent.ConductorFixture
import org.scalatest.{BeforeAndAfter, fixture}
import org.scalatest.time.{Millis, Seconds, Span}

import java.net.URI
import java.nio.file.Files
import java.util.concurrent.Future
import scala.collection.mutable.ListBuffer

/**
 * This tests use configuration test/resources/application.conf
 */
class SmartDataLakeBuilderStatusInfoTest extends fixture.FunSuite with BeforeAndAfter with ConductorFixture {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(scaled(Span(50, Seconds)), scaled(Span(15, Millis)))

  import session.implicits._

  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  val statePath = "target/stateTest/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))

  test("sdlb run with statusinfoserver: Test connectivity of REST API and Websocket") {
    conductor =>

      // init sdlb
      val appName = "sdlb-runId"
      val feedName = "test"

      HdfsUtil.deleteFiles(new Path(statePath), false)
      val sdlb = new DefaultSmartDataLakeBuilder()
      implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry
      implicit val actionPipelineContext: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

      // setup DataObjects
      val srcDO = CsvFileDataObject("src1", tempPath + s"inputDataObject")
      instanceRegistry.register(srcDO)
      val tgt1DO = CsvFileDataObject("tgt1", tempPath + s"outputDataObject")
      instanceRegistry.register(tgt1DO)

      // fill src table with data
      val dfSrc1 = Seq("testData").toDF("testColumn")
      srcDO.writeDataFrame(dfSrc1, Seq())


      val action1 = CopyAction("a", srcDO.id, tgt1DO.id, metadata = Some(ActionMetadata(feed = Some(feedName))))
      instanceRegistry.register(action1.copy())
      Environment._globalConfig = GlobalConfig(statusInfo = Some(StatusInfoConfig(4440, stopOnEnd = false)))
      val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, applicationName = Some(appName), statePath = Some(statePath))


      //Run SDLB
      //   threadNamed("sdlb-run") {
      sdlb.run(sdlConfig)
      //     }
      val receivedMessages: ListBuffer[String] = ListBuffer()
      //   threadNamed("websocket") {
      //Create Client Websocket that tries to establish connection with SDLB Job

      // The socket that receives events
      class UnitTestSocket() extends WebSocketAdapter with SmartDataLakeLogger {
        override def onWebSocketConnect(sess: Session): Unit = {}

        override def onWebSocketText(message: String): Unit = {
          receivedMessages += message
        }

        override def onWebSocketClose(statusCode: Int, reason: String): Unit = {}

        override def onWebSocketError(cause: Throwable): Unit = {}
      }
      val client = new WebSocketClient
      val uri = URI.create("ws://localhost:4440/ws/")
      client.start()
      val socket = new UnitTestSocket
      val fut: Future[Session] = client.connect(socket, uri)
      Thread.sleep(6000)

      var connectionTries = 0
      var connectionEstablished = false
      while (!connectionEstablished && connectionTries < 1000) {
        try {
          fut.get
          connectionEstablished = true
          println("Connected")
        } catch {
          case e: Exception => {
            println("Try " + connectionTries + " failed with Exception : " + e)
            connectionTries += 1
            Thread.sleep(1000)
          }
        }
      }
    //  }
    //      whenFinished{
    //Verify a client websocket can connect
    //    assert(receivedMessages.head.contains("Hello from io.smartdatalake.statusinfo.websocket.StatusInfoSocket"))
    // }


    /*
        //Verify Rest API is reachable
        val webserviceDOContext = WebserviceFileDataObject("dummy", url = s"http://localhost:4440/api/v1/context/")
        val webserviceClientContext = ScalaJWebserviceClient(webserviceDOContext)
        webserviceClientContext.get() match {
          case Failure(exception) =>
            throw exception
          case Success(value) =>
            val str = new String(value, StandardCharsets.UTF_8)
            assert(str.contains("\"feedSel\":\"test\""))
        }
    */

  }
}



