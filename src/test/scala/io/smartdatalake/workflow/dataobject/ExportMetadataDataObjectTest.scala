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
package io.smartdatalake.workflow.dataobject

import com.typesafe.config.{Config, ConfigFactory}
import io.smartdatalake.config.{TestAction, TestConnection, TestDataObject}
import io.smartdatalake.workflow.action.ActionMetadata

class ExportMetadataDataObjectTest extends DataObjectTestSuite {

  test("Test DataObjects Export (from classpath)") {
    val con1 = TestConnection(id = "con1")
    instanceRegistry.register(con1)
    val metaData = DataObjectMetadata(name = Some("Test DataObject"), Some("For Testing"))
    val testDo = TestDataObject(id = "do1", arg1 = "Foo", args = List("Bar"), metadata = Some(metaData), connectionId = Some("con1"))
    instanceRegistry.register(testDo)

    val config: Config = ConfigFactory.parseString("id = dataObjects-exporter")

    val dataObjectsExporter: DataObjectsExporterDataObject = DataObjectsExporterDataObject.fromConfig(config, instanceRegistry)
    val df = dataObjectsExporter.getDataFrame()
    df.select("id").head().get(0) should be (testDo.id.id)
    df.select("name").head().get(0) should be (metaData.name.get)
    df.select("description").head().get(0) should be (metaData.description.get)
    df.select("connectionId").head().get(0) should be (testDo.connectionId.get.id)
  }

  test("Test DataObjects Export (from config option)") {
    val configLocation = getClass.getResource("/config/config.conf").toURI.toString
    val config = ConfigFactory.parseString(s"""
          | id = dataObjects-exporter
          | config = "$configLocation"
         """.stripMargin)

    val dataObjectsExporter = DataObjectsExporterDataObject.fromConfig(config, instanceRegistry)
    val df = dataObjectsExporter.getDataFrame()
    df.select("id").head().get(0) should be ("testDataObjectFromConfig")
    df.select("name").head().get(0) should be ("Test DataObject From Config")
    df.select("description").head().get(0) should be ("Loaded from a Test Config")
  }

  test("Test Actions Export (from classpath)") {
    val con1 = TestConnection(id = "con1")
    instanceRegistry.register(con1)
    val do1 = TestDataObject(id = "do1", arg1 = "Foo", args = List("Bar"))
    instanceRegistry.register(do1)
    val do2 = TestDataObject(id = "do2", arg1 = "Bar", args = List("Baz"), connectionId = Some("con1"))
    instanceRegistry.register(do2)

    val metaData = ActionMetadata(name = Some("Test Action"), Some("For Testing"), Some("test feed"))
    val testAction = TestAction("testAction", "do1", "do2", None, None, Some(metaData))
    instanceRegistry.register(testAction)

    val config = ConfigFactory.parseString("id = actions-exporter")

    val actionsExporter = ActionsExporterDataObject.fromConfig(config, instanceRegistry)
    val df = actionsExporter.getDataFrame()
    df.select("id").head().get(0) should be (testAction.id.id)
    df.select("name").head().get(0) should be (metaData.name.get)
    df.select("description").head().get(0) should be (metaData.description.get)
    df.select("feed").head().get(0) should be (metaData.feed.get)
  }

  test("Test Actions Export (from config option)") {
    val configLocation = getClass.getResource("/config/subdirectory/foo.json").toURI.toString
    val config = ConfigFactory.parseString(s"""
          | id = action-exporter
          | config = "$configLocation"
         """.stripMargin).resolve()

    val actionsExporter = ActionsExporterDataObject.fromConfig(config, instanceRegistry)
    val df = actionsExporter.getDataFrame()
    df.select("id").head().get(0) should be ("testActionFromConfig")
    df.select("name").head().get(0) should be ("Test Action From Config")
    df.select("description").head().get(0) should be ("Loaded from a Test Config")
    df.select("feed").head().get(0) should be ("testfeed2")
  }
}
