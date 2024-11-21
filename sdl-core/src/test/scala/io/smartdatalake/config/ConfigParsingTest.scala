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

import com.typesafe.config.{ConfigException, ConfigFactory}
import io.smartdatalake.config.ConfigParser.localSubstitution
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.objects.{TestAction, TestConnection, TestDataObject, TestHousekeepingMode}
import io.smartdatalake.definitions.{Environment, SDLSaveMode}
import io.smartdatalake.workflow.action.executionMode.PartitionDiffMode
import io.smartdatalake.workflow.action.{Action, FileTransferAction}
import io.smartdatalake.workflow.dataobject.{CsvFileDataObject, DataObject, DataObjectMetadata, RawFileDataObject}
import org.apache.spark.sql.types.StructType
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
        |     saveMode = Append
        |   }
        |   do3 = {
        |     type = CsvFileDataObject
        |     path = /my/path3
        |     csvOptions = {
        |       delimiter = ","
        |       escape = "\\"
        |       header = "true"
        |       quote = "\""
        |     }
        |     saveMode = OverwritePreserveDirectories
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
      partitions = Seq.empty,
      saveMode = SDLSaveMode.Append
    )

    val do3 = CsvFileDataObject(
      id = "do3",
      path = "/my/path3",
      csvOptions = Map(
        "delimiter" -> ",",
        "escape" -> "\\",
        "header" -> "true",
        "quote" -> "\""
      ),
      saveMode = SDLSaveMode.OverwritePreserveDirectories
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

    registry.instances.keys
  }

  it must "correctly parse a DataObject and Connection map with a single element" in {

    val config = ConfigFactory.parseString(
      """
        |connections = {
        |   tcon = {
        |     type = io.smartdatalake.config.objects.TestConnection
        |   }
        |}
        |dataObjects = {
        |   tdo = {
        |     type = io.smartdatalake.config.objects.TestDataObject
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
        |     type = io.smartdatalake.config.objects.TestDataObject
        |     arg1 = foo
        |     args = [bar, "!"]
        |   }
        |   tdo2 = {
        |     type = io.smartdatalake.config.objects.TestDataObject
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

    registry.instances should have size 0
  }

  it must "correctly parse an Action map with a single element" in {
    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        |   tdo1 = {
        |     id = tdo1
        |     type = io.smartdatalake.config.objects.TestDataObject
        |     arg1 = foo
        |     args = []
        |   }
        |   tdo2 = {
        |     id = tdo2
        |     type = io.smartdatalake.config.objects.TestDataObject
        |     arg1 = bar
        |     args = []
        |   }
        |}
        |
        |actions = {
        |   ta1 = {
        |     type = io.smartdatalake.config.objects.TestAction
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
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = foo
        |   args = [bar, "!"]
        | }
        | tdo2 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = goo
        |   args = [bar]
        | }
        |}
        |
        |actions = {
        |   ta1 = {
        |     type = io.smartdatalake.config.objects.TestAction
        |     inputId = tdo1
        |     outputId = tdo2
        |   }
        |   ta2 = {
        |     type = io.smartdatalake.config.objects.TestAction
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

  "Action" should "throw ConfigurationException on wrong DataObject type" in {
    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        |   tdo1 = {
        |     type = io.smartdatalake.config.objects.TestDataObject
        |     arg1 = foo
        |     args = []
        |   }
        |   tdo2 = {
        |     type = CsvFileDataObject
        |     path = ./csv
        |   }
        |}
        |actions = {
        |   ta1 = {
        |     type = io.smartdatalake.config.objects.TestAction
        |     inputId = tdo1
        |     outputId = tdo2
        |   }
        |}
        |""".stripMargin)

    val ex = intercept[ConfigurationException](ConfigParser.parse(config))
    assert(ex.message.contains("does not implement expected DataObject type"))
    assert(Option(ex.getCause).isEmpty)
  }

  "Action" should "throw ConfigurationException on wrong transformation type" in {
    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        |   tdo1 = {
        |     type = io.smartdatalake.config.objects.TestDataObject
        |     arg1 = foo
        |     args = []
        |   }
        |   tdo2 = {
        |     type = io.smartdatalake.config.objects.TestDataObject
        |     arg1 = foo
        |     args = []
        |   }
        |}
        |actions = {
        |   ta1 = {
        |     type = io.smartdatalake.config.objects.TestAction
        |     inputId = tdo1
        |     outputId = tdo2
        |     transformers = [{
        |       type = ScalaClassSparkDfsTransformer
        |       className = blabla
        |     }]
        |   }
        |}
        |""".stripMargin)

    val ex = intercept[ConfigurationException](ConfigParser.parse(config))
    assert(ex.message.contains("does not implement expected interface"))
  }


  "Action" should "throw ConfigurationException on wrong transformation type (full class name)" in {
    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        |   tdo1 = {
        |     type = io.smartdatalake.config.objects.TestDataObject
        |     arg1 = foo
        |     args = []
        |   }
        |   tdo2 = {
        |     type = io.smartdatalake.config.objects.TestDataObject
        |     arg1 = foo
        |     args = []
        |   }
        |}
        |actions = {
        |   ta1 = {
        |     type = io.smartdatalake.config.objects.TestAction
        |     inputId = tdo1
        |     outputId = tdo2
        |     transformers = [{
        |       type = io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfsTransformer
        |       className = blabla
        |     }]
        |   }
        |}
        |""".stripMargin)

    val ex = intercept[ConfigurationException](ConfigParser.parse(config))
    assert(ex.message.contains("does not implement expected interface"))
  }

  "TestDataObject" should "be parsable" in {
    implicit val registry: InstanceRegistry = new InstanceRegistry()
    val config = ConfigFactory.parseString(
      """
        |tdo = {
        | id = tdo
        | arg1 = "first"
        | args = [one, two]
        |}
        |
        |""".stripMargin).resolve

    val testDataObject = TestDataObject.fromConfig(config.getConfig("tdo"))
    testDataObject shouldEqual TestDataObject(id = "tdo", arg1 = "first", args = List("one", "two"))
  }

  "TestAction with ExecutionMode" should "be parsable" in {
    val dataObjectsConfig = ConfigFactory.parseString(
      """
        |dataObjects = {
        | tdo1 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = foo
        |   args = [bar, "!"]
        | }
        | tdo2 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = goo
        |   args = [bar]
        | }
        |}
      """.stripMargin).resolve
    val config = ConfigFactory.parseString(
      """
        |a = {
        | id = a
        | inputId = tdo1
        | output-id = tdo2
        | executionMode = {
        |  type = PartitionDiffMode
        |  partitionColNb = 2
        | }
        |}
        |
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(dataObjectsConfig)
    val testAction = TestAction.fromConfig(config.getConfig("a"))
    val expected = TestAction(id = "a", arg1 = None, inputId = "tdo1", outputId = "tdo2", executionMode = Some(PartitionDiffMode(partitionColNb = Some(2))))
    testAction shouldEqual expected
  }

  "TestAction with HousekeepingMode" should "be parsable" in {
    val dataObjectsConfig = ConfigFactory.parseString(
      """
        |dataObjects = {
        | tdo1 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = foo
        |   args = [bar, "!"]
        | }
        | tdo2 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = goo
        |   args = [bar]
        | }
        |}
      """.stripMargin).resolve
    val config = ConfigFactory.parseString(
      """
        |a = {
        | id = a
        | inputId = tdo1
        | output-id = tdo2
        | housekeepingMode= {
        |   type = io.smartdatalake.config.objects.TestHousekeepingMode
        |   arg1 = foo
        | }
        |}
        |
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(dataObjectsConfig)
    val testAction = TestAction.fromConfig(config.getConfig("a"))
    val expected = TestAction(id = "a", arg1 = None, inputId = "tdo1", outputId = "tdo2", housekeepingMode = Some(TestHousekeepingMode(arg1 = Some("foo"))))
    testAction shouldEqual expected
  }

  "TestDataObject" should "throw ConfigurationException on wrong connection type" in {
    implicit val registry: InstanceRegistry = new InstanceRegistry()
    val config = ConfigFactory.parseString(
      """
        | connections {
        |   c1 = {
        |     type = HadoopFileConnection
        |     pathPrefix = ./test
        |   }
        | }
        | dataObjects {
        |   tdo = {
        |     type = io.smartdatalake.config.objects.TestDataObject
        |     connectionId = c1
        |     arg1 = "first"
        |     args = [one, two]
        |   }
        | }
    """.stripMargin).resolve()

    val ex = intercept[ConfigurationException](ConfigParser.parse(config))
    assert(ex.message.contains("does not implement expected connection type"))
    assert(Option(ex.getCause).isEmpty)
  }


  "Parser" should "fail if entry is not of type object" in {
    val dataObjectsConfig = ConfigFactory.parseString(
      """
        |dataObjects = {
        | test = config-error
        |}
      """.stripMargin).resolve

    intercept[ConfigurationException](ConfigParser.parse(dataObjectsConfig))
  }

  "TestAction" should "fail on superfluous key" in {
    val dataObjectsConfig = ConfigFactory.parseString(
      """
        |dataObjects = {
        | tdo1 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = foo
        |   args = [bar, "!"]
        | }
        | tdo2 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = goo
        |   args = [bar]
        | }
        |}
      """.stripMargin).resolve
    val config = ConfigFactory.parseString(
      """
        |a = {
        | id = a
        | inputId = tdo1
        | outputId = tdo2
        | test = test
        | executionMode = {
        |  type = PartitionDiffMode
        |  partitionColNb = 2
        | }
        |}
        |
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(dataObjectsConfig)
    intercept[ConfigException](TestAction.fromConfig(config.getConfig("a")))
  }

  "TestAction" should "fail on superfluous key in optional case class" in {
    val dataObjectsConfig = ConfigFactory.parseString(
      """
        |dataObjects = {
        | tdo1 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = foo
        |   args = [bar, "!"]
        | }
        | tdo2 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = goo
        |   args = [bar]
        | }
        |}
      """.stripMargin).resolve
    val config = ConfigFactory.parseString(
      """
        |a = {
        | id = a
        | inputId = tdo1
        | outputId = tdo2
        | test = test
        | executionMode = {
        |  type = PartitionDiffMode
        |  partitionColNb = 2
        | }
        | metadata {
        |  test = test // doesnt exist
        | }
        |}
        |
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(dataObjectsConfig)
    intercept[ConfigException](TestAction.fromConfig(config.getConfig("a")))
  }

  "TestAction" should "fail on superfluous key in optional sealed trait" in {
    val dataObjectsConfig = ConfigFactory.parseString(
      """
        |dataObjects = {
        | tdo1 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = foo
        |   args = [bar, "!"]
        | }
        | tdo2 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = goo
        |   args = [bar]
        | }
        |}
      """.stripMargin).resolve
    val config = ConfigFactory.parseString(
      """
        |a = {
        | id = a
        | inputId = tdo1
        | outputId = tdo2
        | executionMode = {
        |  type = PartitionDiffMode
        |  partitionColNb = 2
        |  test = test // doesnt exist
        | }
        |}
        |
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(dataObjectsConfig)
    intercept[ConfigException](TestAction.fromConfig(config.getConfig("a")))
  }

  "TestAction" should "fail on unknown type of optional sealed trait" in {
    val dataObjectsConfig = ConfigFactory.parseString(
      """
        |dataObjects = {
        | tdo1 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = foo
        |   args = [bar, "!"]
        | }
        | tdo2 = {
        |   type = io.smartdatalake.config.objects.TestDataObject
        |   arg1 = goo
        |   args = [bar]
        | }
        |}
      """.stripMargin).resolve
    val config = ConfigFactory.parseString(
      """
        |a = {
        | id = a
        | inputId = tdo1
        | outputId = tdo2
        | executionMode = {
        |  type = UnknownMode
        |  partitionColNb = 2
        | }
        |}
        |
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(dataObjectsConfig)
    intercept[ConfigException](TestAction.fromConfig(config.getConfig("a")))
  }

  "single local substitution" should "be processed" in {
    val config = ConfigFactory.parseString(
      """{
        | id = 10
        | name = abc
        | path = "test~{id}/~{name}"
        |}""".stripMargin
    )

    val configSubstituted1 = ConfigParser.localSubstitution(config, "path")
    configSubstituted1.getString("path") shouldEqual "test10/abc"
  }

  "all local substitution" should "be processed" in {
    val config = ConfigFactory.parseString(
      """{
        | id = 10
        | name = abc
        | path = "test~{id}/~{name}"
        | pre-read-sql = "test~{id}/~{name}"
        | postWriteSql = "test~{id}/~{name}"
        | table {
        |  name = "test~{id}/~{name}"
        | }
        |}""".stripMargin
    )

    val refinedConfig = Environment.configPathsForLocalSubstitution.foldLeft(config){
      case (config, path) => localSubstitution(config, path)
    }
    refinedConfig.getString("path") shouldEqual "test10/abc"
    refinedConfig.getString("pre-read-sql") shouldEqual "test10/abc"
    refinedConfig.getString("postWriteSql") shouldEqual "test10/abc"
    refinedConfig.getString("table.name") shouldEqual "test10/abc"
  }
}


case class MyTestDataObject( id: DataObjectId,
                             schemaMin: Option[StructType] = None,
                           arg1: String,
                           args: Seq[String],
                           connectionId: Option[ConnectionId] = None,
                           metadata: Option[DataObjectMetadata] = None)
                         ( implicit val instanceRegistry: InstanceRegistry)