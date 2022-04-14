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
import io.smartdatalake.communication.agent.{AgentController, AgentServer, AgentServerConfig, SerializedConfig}
import io.smartdatalake.config.ConfigParser.{getActionConfigMap, getDataObjectConfigMap, parseConfigObjectWithId}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.action.{Action, CopyAction}
import io.smartdatalake.workflow.dataframe.spark.SparkDataFrame
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
    // setup input DataObject
    val srcDO = CsvFileDataObject("src1", "target/src1")(sdlb.instanceRegistry)
    val dfSrc1 = Seq("testData").toDF("testColumn")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc1), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = feedName, configuration = Some(Seq(
      getClass.getResource("/configremote/application.conf").getPath))
    )
    //Run SDLB
    sdlb.run(sdlConfig)

    val actionToSerialize = sdlb.instanceRegistry.getActions.head.asInstanceOf[CopyAction]

    val serializedConfig = SerializedConfig(inputDataObjects = actionToSerialize.inputs.map(_._config.get)
      , outputDataObjects = actionToSerialize.outputs.map(_._config.get), action = actionToSerialize._config.get)

    //Serialize the Action and it s input and output dataobjects to HOCON Format
    val hoconString = serializedConfig.asHoconString
    implicit val instanceRegistry = new InstanceRegistry

    val configFromString = ConfigFactory.parseString(hoconString, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))

    val dataObjects: Map[DataObjectId, DataObject] = getDataObjectConfigMap(configFromString)
      .map { case (id, config) => (DataObjectId(id), parseConfigObjectWithId[DataObject](id, config)) }
    instanceRegistry.register(dataObjects)

    val actions: Map[ActionId, Action] = getActionConfigMap(configFromString)
      .map { case (id, config) => (ActionId(id), parseConfigObjectWithId[Action](id, config)) }
    instanceRegistry.register(actions)

    //Contents of the action and objects should match the contents of /configremote/application.conf
    assert(dataObjects.contains("src1") && dataObjects.contains("tgt1") && actions.contains("a"))
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



