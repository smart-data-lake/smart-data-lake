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

package io.smartdatalake.app

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigSyntax}
import io.smartdatalake.communication.agent.{AgentClient, JettyAgentServer, StorageAgentServer}
import io.smartdatalake.config.ConfigParser.{getActionConfigMap, getConnectionConfigMap, getDataObjectConfigMap, parseConfigObjectWithId}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, AgentId, ConnectionId, DataObjectId}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ExecutionPhase
import io.smartdatalake.workflow.action.{Action, ProxyAction}
import io.smartdatalake.workflow.agent.JettyAgent
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import scala.concurrent.Future
import scala.util.{Failure, Success}

class SmartDataLakeBuilderAgentTest extends AnyFunSuite with BeforeAndAfter with SmartDataLakeLogger {

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  private val sdlb = DefaultSmartDataLakeBuilder
  implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry

  before {
    instanceRegistry.clear()
    instanceRegistry.register(TestUtil.defaultSparkConnection)
  }

  test("Test Config Parsing") {
    val feedName = "test"

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = Seq("cp:/configAgents/agent-main.conf"))

    sdlb.loadConfigIntoInstanceRegistry(sdlConfig, session.sparkContext.hadoopConfiguration)

    val actionToSend = sdlb.instanceRegistry.getActions.filter(_.id.id == "remote-to-cloud-jetty-agent").head.asInstanceOf[ProxyAction].wrappedAction

    val sdlMessage = AgentClient.prepareHoconInstructions(actionToSend, Nil, JettyAgent(AgentId("dummyId"), "dummyUrl", sdlb.instanceRegistry.getConnections.map(connection => connection.id.id -> connection).toMap), ExecutionPhase.Exec)
    val configFromString = ConfigFactory.parseString(sdlMessage.agentInstruction.get.hoconConfig, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

    val dataObjects: Map[DataObjectId, DataObject] = getDataObjectConfigMap(configFromString)
      .map { case (id, config) => (DataObjectId(id), parseConfigObjectWithId[DataObject](id, config)) }

    val actions: Map[ActionId, Action] = getActionConfigMap(configFromString)
      .map { case (id, config) => (ActionId(id), parseConfigObjectWithId[Action](id, config)) }

    val connections: Map[ConnectionId, Connection] = getConnectionConfigMap(configFromString)
      .map { case (id, config) => (ConnectionId(id), parseConfigObjectWithId[Connection](id, config)) }

    //Contents of the action and objects generated out of the serialized hocon string should match the contents of /configAgents/application-jettyagent.conf
    assert(dataObjects.contains("remote-file") && dataObjects.contains("cloud-file1") && connections.contains("agent-src") && actions.contains("remote-to-cloud-jetty-agent"))
  }

  test("SDLB run with JettyAgent: Test starting remote action via Agent") {

    val tempDir = Files.createTempDirectory("jetty-agent")

    // setup input DataObject and data
    val srcDO = CsvFileDataObject("src1", tempDir.resolve("agent_src/remote_file").toString)(sdlb.instanceRegistry)
    val dfSrc1 = Seq("testData").toDF("testColumn")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc1), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    // setup remote SDLB agent
    val remoteSDLB = new SmartDataLakeBuilder {}
    val server = JettyAgentServer(remoteSDLB, LocalJettyAgentSmartDataLakeBuilderConfig(configuration = Seq("cp:/configAgents/agent-remote-server.conf"), configurationValueOverwrite = Map("env.tempDir" -> tempDir.toString)))
    server.start()

    // run SDLB Main Instance
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "test-(jetty|main)", configuration = Seq("cp:/configAgents/agent-main.conf"), configurationValueOverwrite = Map("env.tempDir" -> tempDir.toString))
    sdlb.run(sdlConfig)

    // remoteSDLB should have executed exactly one action: the remoteAction
    assert(remoteSDLB.instanceRegistry.getActions.size == 1)
    val remoteAction = remoteSDLB.instanceRegistry.getActions.head
    assert(remoteAction.id.id == "remote-to-cloud-jetty-agent")
    assert(remoteAction.outputs.head.id.id == "cloud-file1")

    // Main Instance of SDLB was able to execute action cloud-to-cloud by using data provided from the Agent
    assert(tempDir.resolve("cloud_staging/cloud_file2").toFile.exists())
  }

  test("SDLB run with StorageAgent: Test starting remote action via Agent") {

    val tempDir = Files.createTempDirectory("storage-agent")

    // setup input DataObject and data
    val srcDO = CsvFileDataObject("src1", tempDir.resolve("agent_src/remote_file").toString)(sdlb.instanceRegistry)
    val dfSrc1 = Seq("testData").toDF("testColumn")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc1), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    // setup remote SDLB agent, needs to run in separate thread
    val remoteSDLB = new SmartDataLakeBuilder {}
    val server = new StorageAgentServer(remoteSDLB, LocalStorageAgentSmartDataLakeBuilderConfig(configuration = Seq("cp:/configAgents/agent-remote-server.conf"), path = tempDir.resolve("storage-agent1").toString, pollIntervalSec = 1, configurationValueOverwrite = Map("env.tempDir" -> tempDir.toString)))
    var doPoll = true
    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      while (doPoll) {
        server.pollForInstructions()
      }
    }.onComplete {
      case Success(_) => logger.info(s"pollForInstructions done")
      case Failure(ex) => throw ex
    }

    // run SDLB Main Instance
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "test-(storage|main)", configuration = Seq("cp:/configAgents/agent-main.conf"), configurationValueOverwrite = Map("env.tempDir" -> tempDir.toString))
    sdlb.run(sdlConfig)
    doPoll = false

    // remoteSDLB should have executed exactly one action: the remoteAction
    assert(remoteSDLB.instanceRegistry.getActions.size == 1)
    val remoteAction = remoteSDLB.instanceRegistry.getActions.head
    assert(remoteAction.id.id == "remote-to-cloud-storage-agent")
    assert(remoteAction.outputs.head.id.id == "cloud-file1")

    // Main Instance of SDLB was able to execute action cloud-to-cloud by using data provided from the Agent
    assert(tempDir.resolve("cloud_staging/cloud_file2").toFile.exists())
  }

}