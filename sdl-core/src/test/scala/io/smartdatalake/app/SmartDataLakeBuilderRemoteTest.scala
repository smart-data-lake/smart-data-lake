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
import io.smartdatalake.communication.agent.{AgentClient, AgentServer, AgentServerConfig, AgentServerController}
import io.smartdatalake.config.ConfigParser.{getActionConfigMap, getConnectionConfigMap, getDataObjectConfigMap, parseConfigObjectWithId}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, AgentId, ConnectionId, DataObjectId}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.ExecutionPhase
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.agent.AgentImpl
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.nio.file.Paths
import scala.util.Try

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
      Seq("testData")
        .toDF("testColumn")
    ), DataObjectId("src1"), Nil)

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = Some(Seq(
      getClass.getResource("/configremote/application.conf").getPath))
    )
    //Run simlutation of SDLB to parse config file and populate instanceregistry
    sdlb.startSimulationWithConfigFile(sdlConfig, Seq(srcDO1))(session)

    implicit val instanceRegistry: InstanceRegistry = sdlb.instanceRegistry

    val sdlMessage = AgentClient.prepareHoconInstructions(sdlb.instanceRegistry.getActions.head, Nil, AgentImpl(AgentId("dummyId"), "dummyUrl", sdlb.instanceRegistry.getConnections.map(connection => connection.id.id -> connection).toMap), ExecutionPhase.Exec)
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
    FileUtils.deleteDirectory(Paths.get(System.getProperty("user.dir"), "target/agent_dummy_connection").toFile)
    FileUtils.deleteDirectory(Paths.get(System.getProperty("user.dir"), "target/dummy_cloud_connection").toFile)
    val sdlb = new DefaultSmartDataLakeBuilder()
    // setup input DataObject
    val srcDO = CsvFileDataObject("src1", "target/agent_dummy_connection/remote-file")(sdlb.instanceRegistry)
    val dfSrc1 = Seq("testData").toDF("testColumn")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc1), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))


    val agentConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = None)

    val remoteSDLB = new DefaultSmartDataLakeBuilder()
    val agentController: AgentServerController = AgentServerController(remoteSDLB.instanceRegistry, remoteSDLB)
    AgentServer.start(AgentServerConfig(sdlConfig = agentConfig), agentController)

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = Some(Seq(
      getClass.getResource("/configremote/application.conf").getPath))
    )
    //Run SDLB Main Instance
    sdlb.run(sdlConfig)

    //remoteSDLB should have executed exactly one action: the remoteAction
    assert(remoteSDLB.instanceRegistry.getActions.size == 1)
    val remoteAction = remoteSDLB.instanceRegistry.getActions.head
    assert(remoteAction.id.id == "remote-to-cloud")
    assert(remoteAction.outputs.head.id.id == "cloud-file1")

    //Main Instance of SDLB was not using remoteFile connection from connection list
    assert(!Paths.get(System.getProperty("user.dir"), "target","dummy_connection").toFile.exists())

    //Main Instance of SDLB was able to execute action cloud-to-cloud by using data provided from the Agent
    assert(Paths.get(System.getProperty("user.dir"), "target", "dummy_cloud_connection", "cloud-file2").toFile.exists())
  }
}



