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

import io.smartdatalake.communication.agent._
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
import io.smartdatalake.workflow.dataobject._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Tests communication with AzureRelayAgent.
 * Because it only works with a connection to Azure Relay Service it is considered as an Integration Test.
 * Note that you need to specify the Environment Variable SharedAccessKey for it to work
 */
object SmartDataLakeBuilderAzureRelayAgentIT extends App {

  protected implicit val session: SparkSession = TestUtil.sessionWithoutHive

  import session.implicits._

  val feedName = "test"
  FileUtils.deleteDirectory(Paths.get(System.getProperty("user.dir"), "target/relay_agent_dummy_connection").toFile)
  FileUtils.deleteDirectory(Paths.get(System.getProperty("user.dir"), "target/relay_dummy_cloud_connection").toFile)
  val sdlb = new DefaultSmartDataLakeBuilder()

  // setup input DataObject
  val srcDO = CsvFileDataObject("src1", "target/relay_agent_dummy_connection/remote-file")(sdlb.instanceRegistry)
  val dfSrc1 = Seq("testData").toDF("testColumn")
  srcDO.writeDataFrame(SparkDataFrame(dfSrc1), Seq())(TestUtil.getDefaultActionPipelineContextWithoutHive(sdlb.instanceRegistry))

  val azureRelayUrl = "Endpoint=sb://relay-tbb-test.servicebus.windows.net/;EntityPath=relay-tbb-test-connection;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey="
  val agentConfig = LocalAzureRelayAgentSmartDataLakeBuilderConfig(feedSel = feedName, configuration = None, azureRelayURL = Some(azureRelayUrl))
  val remoteSDLB = new DefaultSmartDataLakeBuilder()
  //Make sure this string matches the config from the file application-azureRelayAgent.conf
  val agentController: AgentServerController = AgentServerController(remoteSDLB.instanceRegistry, remoteSDLB)
  val agentServerThread =
    Future {
      AzureRelayAgentServer.start(agentConfig, agentController)
    }

  val configFileResource = getClass.getResource("/configAgents/application-azureRelayAgent.conf")
  require(configFileResource != null, "Please make sure the file application-azureRelayAgent.conf is included in the resources when running this test." +
    "In IntelliJ, you can do this with the option Modify Classpath")
  Thread.sleep(5000)
  val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = Some(Seq(configFileResource.getPath))
  )
  //Run SDLB Main Instance
  sdlb.run(sdlConfig)

  //remoteSDLB should have executed exactly one action: the remoteAction
  assert(remoteSDLB.instanceRegistry.getActions.size == 1)
  val remoteAction = remoteSDLB.instanceRegistry.getActions.head
  assert(remoteAction.id.id == "remote-to-cloud")
  assert(remoteAction.outputs.head.id.id == "cloud-file1")

  //Main Instance of SDLB was not using remoteFile connection from connection list
  assert(!Paths.get(System.getProperty("user.dir"), "target", "relay_dummy_connection").toFile.exists())

  //Main Instance of SDLB was able to execute action cloud-to-cloud by using data provided from the Agent
  assert(Paths.get(System.getProperty("user.dir"), "target", "relay_dummy_cloud_connection", "cloud-file2").toFile.exists())
}



