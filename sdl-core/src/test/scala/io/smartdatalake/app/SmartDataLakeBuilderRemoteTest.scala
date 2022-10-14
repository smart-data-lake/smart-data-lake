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
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.action.Action
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

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

    val hoconString = AgentClient.prepareHoconInstructions(sdlb.instanceRegistry.getActions.head, sdlb.instanceRegistry.getConnections)
    val configFromString = ConfigFactory.parseString(hoconString, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

    val dataObjects: Map[DataObjectId, DataObject] = getDataObjectConfigMap(configFromString)
      .map { case (id, config) => (DataObjectId(id), parseConfigObjectWithId[DataObject](id, config)) }

    val actions: Map[ActionId, Action] = getActionConfigMap(configFromString)
      .map { case (id, config) => (ActionId(id), parseConfigObjectWithId[Action](id, config)) }

    val connections: Map[ConnectionId, Connection] = getConnectionConfigMap(configFromString)
      .map { case (id, config) => (ConnectionId(id), parseConfigObjectWithId[Connection](id, config)) }

    //Contents of the action and objects generated out of the serialized hocon string should match the contents of /configremote/application.conf
    assert(dataObjects.contains("src1") && dataObjects.contains("tgt1") && connections.contains("localSql"))
  }
  test("sdlb run with agent: Test starting remote action from sdlb to agentserver") {

    val feedName = "test"

    val sdlb = new DefaultSmartDataLakeBuilder()
    // setup input DataObject
    val srcDO = CsvFileDataObject("src1", "target/src1")(sdlb.instanceRegistry)
    val dfSrc1 = Seq("testData").toDF("testColumn")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc1), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = Some(Seq(
      getClass.getResource("/configremote/application.conf").getPath))
    )

    val sdlb2 = new DefaultSmartDataLakeBuilder()

    val sdl2Config = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = None)
    val agentController: AgentController = AgentController(new InstanceRegistry, sdlb2)
    AgentServer.start(AgentServerConfig(sdlConfig = sdl2Config), agentController)

    //Run SDLB
    sdlb.run(sdlConfig)


  }
}



