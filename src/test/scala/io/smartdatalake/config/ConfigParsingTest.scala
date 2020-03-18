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
package io.smartdatalake.config

import com.typesafe.config.ConfigFactory
import configs.{Configs, Result}
import io.smartdatalake.definitions.PartitionDiffMode
import io.smartdatalake.workflow.action.{Action, FileTransferAction}
import io.smartdatalake.workflow.dataobject.{CsvFileDataObject, DataObject, RawFileDataObject}
import org.scalatest.{FlatSpec, Matchers}

class ConfigParsingTest extends FlatSpec with Matchers {

  "SdlConfig" must "parse a configuration" in {
    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        |   do1 = {
        |     type = RawFileDataObject
        |     path = /my/path
        |     partitions = []
        |   }
        |   do2 = {
        |     type = RawFileDataObject
        |     path = /my/path2
        |     partitions = []
        |   }
        |   do3 = {
        |     type = CsvFileDataObject
        |     path = /my/path3
        |     protocol = local-file
        |     csvOptions = {
        |       delimiter = ","
        |       escape = "\\"
        |       header = "true"
        |       quote = "\""
        |     }
        |   }
        |}
        |actions = {
        | a1 = {
        |   type = FileTransferAction
        |   inputId = do3
        |   outputId = do1
        | }
        |}
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)

    val dataObjects = registry.instances.values.filter(_.isInstanceOf[DataObject])

    val do1 = RawFileDataObject(
      id = "do1",
      path = "/my/path",
      partitions = Seq.empty
    )

    val do2 = RawFileDataObject(
      id = "do2",
      path = "/my/path2",
      partitions = Seq.empty
    )

    val do3 = CsvFileDataObject(
      id = "do3",
      path = "/my/path3",
      csvOptions = Map(
        "delimiter" -> ",",
        "escape" -> "\\",
        "header" -> "true",
        "quote" -> "\""
      )
    )

    dataObjects should contain allOf(do1, do2, do3)

    val actions = registry.instances.values.filter(_.isInstanceOf[Action])
    actions should contain only FileTransferAction(
      id = "a1",
      inputId = do3.id,
      outputId = do1.id
    )
  }

  it must "correctly parse an empty DataObject map" in {

    val config = ConfigFactory.parseString("dataObjects = {}")

    val registry = ConfigParser.parse(config)

    registry.instances shouldBe empty
  }

  it must "correctly parse a DataObject and Connection map with a single element" in {

    val config = ConfigFactory.parseString(
      """
        |connections = {
        |   tcon = {
        |     type = io.smartdatalake.config.TestConnection
        |   }
        |}
        |dataObjects = {
        |   tdo = {
        |     type = io.smartdatalake.config.TestDataObject
        |     arg1 = foo
        |     args = [bar, "!"]
        |     connectionId = tcon
        |   }
        |}
        |""".stripMargin)

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)

    registry.getDataObjects should have size 1
    registry.getDataObjects.head shouldBe a [SdlConfigObject]
    registry.getDataObjects.head shouldBe a [DataObject]
    registry.getDataObjects should contain only TestDataObject(id = "tdo", arg1 = "foo", args = List("bar", "!"), connectionId = Some("tcon"))
    registry.getConnections should contain only TestConnection(id = "tcon")
  }

  it must "correctly parse a DataObject map with multiple elements" in {

    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        |   tdo1 = {
        |     type = io.smartdatalake.config.TestDataObject
        |     arg1 = foo
        |     args = [bar, "!"]
        |   }
        |   tdo2 = {
        |     type = io.smartdatalake.config.TestDataObject
        |     arg1 = goo
        |     args = [bar]
        |   }
        |}
        |""".stripMargin)

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)

    registry.instances should have size 2
    registry.instances.values should contain allOf(
      TestDataObject(id = "tdo1", arg1 = "foo", args = List("bar", "!")),
      TestDataObject(id = "tdo2", arg1 = "goo", args = List("bar"))
    )
  }

  it must "correctly parse an empty Action map" in {

    val config = ConfigFactory.parseString("actions = {}")

    val registry = ConfigParser.parse(config)

    registry.instances shouldBe empty
  }

  it must "correctly parse an Action map with a single element" in {
    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        |   tdo1 = {
        |     id = tdo1
        |     type = io.smartdatalake.config.TestDataObject
        |     arg1 = foo
        |     args = []
        |   }
        |   tdo2 = {
        |     id = tdo2
        |     type = io.smartdatalake.config.TestDataObject
        |     arg1 = bar
        |     args = []
        |   }
        |}
        |
        |actions = {
        |   ta1 = {
        |     type = io.smartdatalake.config.TestAction
        |     inputId = tdo1
        |     outputId = tdo2
        |   }
        |}
        |""".stripMargin)

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)

    val actions = registry.instances.values.filter(_.isInstanceOf[Action])

    actions should have size 1
    actions.head shouldBe a [SdlConfigObject]
    actions.head shouldBe a [Action]

    val expected = TestAction(
        id = "ta1",
        arg1 = None,
        inputId = "tdo1",
        outputId = "tdo2"
    )
    actions should contain only expected
    expected.input shouldBe TestDataObject(id = "tdo1", arg1 = "foo", args=List.empty)
    expected.output shouldBe TestDataObject(id = "tdo2", arg1 = "bar", args=List.empty)
  }

  it must "correctly parse an Action map with multiple elements" in {

    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        | tdo1 = {
        |   type = io.smartdatalake.config.TestDataObject
        |   arg1 = foo
        |   args = [bar, "!"]
        | }
        | tdo2 = {
        |   type = io.smartdatalake.config.TestDataObject
        |   arg1 = goo
        |   args = [bar]
        | }
        |}
        |
        |actions = {
        |   ta1 = {
        |     type = io.smartdatalake.config.TestAction
        |     inputId = tdo1
        |     outputId = tdo2
        |   }
        |   ta2 = {
        |     type = io.smartdatalake.config.TestAction
        |     inputId = tdo2
        |     outputId = tdo1
        |   }
        |}
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)

    val actions = registry.instances.values.filter(_.isInstanceOf[TestAction])

    actions should have size 2
    actions should contain allOf(
      TestAction(id = "ta1", arg1 = None, inputId = "tdo1", outputId = "tdo2"),
      TestAction(id = "ta2", arg1 = None, inputId = "tdo2", outputId = "tdo1")
    )

    val dataObjects = registry.instances.values.filter(_.isInstanceOf[TestDataObject])
    dataObjects should contain allOf(actions.head.asInstanceOf[TestAction].input, actions.head.asInstanceOf[TestAction].output)
    dataObjects should contain allOf(actions.tail.head.asInstanceOf[TestAction].input, actions.tail.head.asInstanceOf[TestAction].output)
  }

  "TestDataObject" should "be parsable" in {
    implicit val registry: InstanceRegistry = new InstanceRegistry()
    val config = ConfigFactory.parseString(
      """
        |tdo = {
        | id = tdo
        | type = io.smartdatalake.config.TestDataObject
        | arg1 = "first"
        | args = [one, two]
        |}
        |
        |""".stripMargin).resolve

    val testDataObject = Configs[TestDataObject].get(config, "tdo")
    testDataObject.isSuccess shouldBe true
    testDataObject.value shouldEqual TestDataObject(id = "tdo", arg1 = "first", args = List("one", "two"))
  }

  "TestAction" should "be parsable" in {

    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        | tdo1 = {
        |   type = io.smartdatalake.config.TestDataObject
        |   arg1 = foo
        |   args = [bar, "!"]
        | }
        | tdo2 = {
        |   type = io.smartdatalake.config.TestDataObject
        |   arg1 = goo
        |   args = [bar]
        | }
        |}
        |
        |a = {
        | id = a
        | type = io.smartdatalake.config.TestAction
        | inputId = tdo1
        | outputId = tdo2
        | executionMode = {
        |  type = PartitionDiffMode
        |  partitionColNb = 2
        | }
        |}
        |
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)

    val testAction = Configs[TestAction].get(config, "a")
    testAction shouldEqual Result.Success(TestAction(id = "a", arg1 = None, inputId = "tdo1", outputId = "tdo2", executionMode = Some(PartitionDiffMode(partitionColNb = Some(2)))))
  }

  "local substitution" should "be processed" in {
    val config = ConfigFactory.parseString(
      """{
        | id = 10
        | name = abc
        | path = "test~{id}/~{name}"
        |}""".stripMargin
    )
    val configSubstituted = ConfigParser.localSubstitution(config, "path")
    configSubstituted.getString("path") shouldEqual "test10/abc"
  }
}
