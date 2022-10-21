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
import io.smartdatalake.communication.agent.{AgentClient, AgentController, AgentServer, AgentServerConfig}
import io.smartdatalake.config.ConfigParser.{getActionConfigMap, getConnectionConfigMap, getDataObjectConfigMap, parseConfigObjectWithId}
import io.smartdatalake.config.SdlConfigObject.{ActionId, AgentId, ConnectionId, DataObjectId}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.agent.AgentImpl
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Paths

/**
 * This tests use configuration test/resources/application.conf
 */
class SmartDataLakeBuilderRemoteTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog

  import session.implicits._

  test("Test Config Parsing") {
    val feedName = "test"
    val sdlb = new DefaultSmartDataLakeBuilder()

    val srcDO1 = SparkSubFeed(SparkDataFrame(
      Seq(("testData"))
        .toDF("testColumn")
    ), DataObjectId("src1"), Nil)

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = Some(Seq(
      getClass.getResource("/configremote/application.conf").getPath))
    )
    //Run simlutation of SDLB to parse config file and populate instanceregistry
    sdlb.startSimulationWithConfigFile(sdlConfig, Seq(srcDO1))(session)

    implicit val instanceRegistry = sdlb.instanceRegistry

    val sdlMessage = AgentClient.prepareHoconInstructions(sdlb.instanceRegistry.getActions.head, Nil, AgentImpl(AgentId("dummyId"), "dummyUrl", sdlb.instanceRegistry.getConnections.map(connection => (connection.id.id -> connection)).toMap))
    val configFromString = ConfigFactory.parseString(sdlMessage.agentInstruction.get.hoconConfig, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

    val dataObjects: Map[DataObjectId, DataObject] = getDataObjectConfigMap(configFromString)
      .map { case (id, config) => (DataObjectId(id), parseConfigObjectWithId[DataObject](id, config)) }

    val actions: Map[ActionId, Action] = getActionConfigMap(configFromString)
      .map { case (id, config) => (ActionId(id), parseConfigObjectWithId[Action](id, config)) }

    val connections: Map[ConnectionId, Connection] = getConnectionConfigMap(configFromString)
      .map { case (id, config) => (ConnectionId(id), parseConfigObjectWithId[Connection](id, config)) }

    //Contents of the action and objects generated out of the serialized hocon string should match the contents of /configremote/application.conf
    assert(dataObjects.contains("src1") && dataObjects.contains("tgt1") && connections.contains("localSql") && actions.contains("a"))
  }
  test("sdlb run with agent: Test starting remote action from sdlb to agentserver") {

    val feedName = "test"

    val sdlb = new DefaultSmartDataLakeBuilder()
    // setup input DataObject
    val srcDO = CsvFileDataObject("src1", "target/src1")(sdlb.instanceRegistry)
    val dfSrc1 = Seq("testData").toDF("testColumn")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc1), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))


    val agentConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = None)

    val remoteSDLB = new DefaultSmartDataLakeBuilder()
    val agentController: AgentController = AgentController(sdlb.instanceRegistry, remoteSDLB)
    AgentServer.start(AgentServerConfig(sdlConfig = agentConfig), agentController)

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = Some(Seq(
      getClass.getResource("/configremote/application.conf").getPath))
    )
    //Run SDLB Main Instance
    sdlb.run(sdlConfig)

    //When main instance is done, remote SDLB should have created tgt file
    val remoteAction = sdlb.instanceRegistry.getActions.head
    assert(remoteAction.id.id == "a")
    assert(remoteAction.outputs.head.id.id == "tgt1")
    assert(Paths.get(System.getProperty("user.dir"), "target/agent_dummy_connection", "tgt1").toFile.exists())

    println("blubv")
  }
}



