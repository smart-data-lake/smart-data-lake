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
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.definitions.{DateColumnType, KeycloakClientSecretAuthMode, SDLSaveMode}
import io.smartdatalake.testutils.custom.TestCustomDfCreator
import io.smartdatalake.util.misc.{AclDef, AclElement}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfCreatorConfig
import io.smartdatalake.workflow.connection.JdbcTableConnection
import io.smartdatalake.workflow.dataobject.{Table, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe.{MethodSymbol, typeOf}

class DataObjectImplTests extends FlatSpec with Matchers {

  "AvroFileDataObject" should "be parsable" in {

    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        | 123 = {
        |   type = AvroFileDataObject
        |   path = /path/to/foo
        |   partitions = []
        |   saveMode = Append
        |   acl = {
        |     permission="rwxr-x---"
        |     acls = [
        |       {
        |         aclType="group"
        |         name="test"
        |         permission="r-x"
        |       }
        |     ]
        |   }
        |   metadata = {name = test, description = "i am a test"}
        | }
        |}
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    registry.instances.values.head shouldBe AvroFileDataObject(
      id = "123",
      path = "/path/to/foo",
      partitions = Seq.empty,
      saveMode = SDLSaveMode.Append,
      acl = Some(AclDef(permission = "rwxr-x---", acls = Seq(AclElement(aclType = "group", name = "test", permission = "r-x")))),
      metadata = Some(DataObjectMetadata(name = Some("test"), description = Some("i am a test")))
    )
  }

  "AccessTableDataObject" should "be parsable" in {
    val config = ConfigFactory.parseString(
      """
        |tableConf = {
        |  db = testDB
        |  name = test
        |  query = "dummy query"
        |  primaryKey = [id1, id2]
        |}
        |
        |dataObjects = {123 = {
        | type = AccessTableDataObject
        | path = /path/to/foo
        | table = ${tableConf}
        |}}
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    registry.instances.values.head shouldBe AccessTableDataObject(
      id = "123",
      path = "/path/to/foo",
      table = Table(
        db = Some("testDB"),
        name = "test",
        query = Some("dummy query"),
        primaryKey = Some(Seq("id1", "id2"))
      )
    )
  }

  "CsvFileDataObject" should "be parsable" in {
    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        | 123 = {
        |   type = CsvFileDataObject
        |   path = /path/to/foo
        |   schema = "first STRING, last STRING"
        |   csvOptions = {
        |     header = false
        |   }
        |   partitions = ["dt", "type"]
        |   saveMode = Append
        | },
        | 124 = {
        |   type = CsvFileDataObject
        |   path = /path/to/foo2
        |   csvOptions = {
        |     delimiter = ","
        |     escape = "\\"
        |     header = "true"
        |     quote = "\""
        |   }
        |   dateColumnType = string
        | }
        |}
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    val dos = registry.instances.values
    dos should contain allOf (
      CsvFileDataObject(
        id = "123",
        path = "/path/to/foo",
        csvOptions = Map("header" -> "false"),
        schema = Some(SparkSchema(StructType(Array(
          StructField("first", StringType, nullable = true),
          StructField("last", StringType, nullable = true)
        )))),
        partitions = Seq("dt", "type"),
        saveMode = SDLSaveMode.Append
      ),
      CsvFileDataObject(
        id = "124",
        path = "/path/to/foo2",
        csvOptions = Map("delimiter" -> ",", "escape" -> "\\", "header" -> "true", "quote" -> "\""),
        dateColumnType = DateColumnType.String
      )
    )
  }

  "ExcelFileDataObject" should "be parsable" in {
    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        | 123 = {
        |   type = ExcelFileDataObject
        |   path = /path/to/foo
        |   excelOptions = {
        |     sheetName = "testSheet"
        |   }
        | }
        |}
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    registry.instances.values.head shouldBe ExcelFileDataObject(
      id = "123",
      path = "/path/to/foo",
      ExcelOptions(
        sheetName = Some("testSheet")
      )
    )
  }

  "JdbcTableDataObject" should "be parsable" in {
    val config = ConfigFactory.parseString(
      """
        |connections = {
        | jdbc1 = {
        |   type = JdbcTableConnection
        |   url = "jdbc://example.test"
        |   driver = com.example.Driver
        | }
        |}
        |dataObjects = {
        | 123 = {
        |   type = JdbcTableDataObject
        |   connectionId = jdbc1
        |   table = {
        |     db = testDB
        |     name = test
        |     query = "dummy query"
        |     primaryKey = [id1, id2]
        |   }
        |   jdbcFetchSize = 5
        | }
        |}
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    val registry2: InstanceRegistry = new InstanceRegistry()
    val jdbcCon = JdbcTableConnection( "jdbc1", url = "jdbc://example.test", driver = "com.example.Driver" )
    registry2.register(jdbcCon)
    registry.instances(DataObjectId("123")) shouldBe JdbcTableDataObject(
      id = "123",
      preWriteSql = None,
      postWriteSql = None,
      connectionId = "jdbc1",
      table = Table(
        db = Some("testDB"),
        name = "test",
        query = Some("dummy query"),
        primaryKey = Some(Seq("id1", "id2"))
      ),
      jdbcFetchSize = 5
    )(registry2)
  }

  "JsonFileDataObject" should "be parsable" in {
    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        | 123 = {
        |   type = JsonFileDataObject
        |   path = /path/to/foo
        |   saveMode = Overwrite
        |   jsonOptions = {
        |     multiLine = false
        |     foo = bar
        |   }
        |   stringify = false
        | }
        |}
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    registry.instances.values.head shouldBe JsonFileDataObject(
      id = "123",
      path = "/path/to/foo",
      jsonOptions = Some(Map("multiLine" -> "false", "foo" -> "bar")),
      partitions = Seq.empty,
      saveMode = SDLSaveMode.Overwrite
    )
  }

  "ParquetFileDataObject" should "be parsable" in {
    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        | 123 = {
        |   type = ParquetFileDataObject
        |   path = /path/to/foo
        |   partitions = [one, two]
        | }
        |}
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    registry.instances.values.head shouldBe ParquetFileDataObject(
      id = "123",
      path = "/path/to/foo",
      partitions = Seq("one", "two")
    )
  }

  "RawFileDataObject" should "be parsable" in {
    val config = ConfigFactory.parseString(
      """
        |dataObjects = {
        | 123 = {
        |   type = RawFileDataObject
        |   path = /path/to/foo
        |   partitions = [one, two]
        | }
        |}
        |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    registry.instances.values.head shouldBe RawFileDataObject(
      id = "123",
      path = "/path/to/foo",
      partitions = Seq("one", "two")
    )
  }

  "CustomDfDataObject" should "be parsable" in {
    val testCreatorConfig = CustomDfCreatorConfig(
      className = Some(classOf[TestCustomDfCreator].getName),
      options = Some(Map("test" -> "foo"))
    )

    val config = ConfigFactory.parseString(
      """
         |dataObjects = {
         | 123 = {
         |   type = CustomDfDataObject
         |   creator {
         |     class-name = io.smartdatalake.testutils.custom.TestCustomDfCreator
         |     options = {
         |       test = foo
         |     }
         |   }
         | }
         |}
         |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    registry.instances.values.head shouldBe CustomDfDataObject(
      id = "123",
      creator = testCreatorConfig
    )
  }

  "WebserviceFileDataObject" should "be parsable" in {
    val config = ConfigFactory.parseString(
      """
         |dataObjects = {
         | 123 = {
         |  type = WebserviceFileDataObject
         |  url = "http://test"
         |  auth-mode = {
         |    type = KeycloakClientSecretAuthMode
         |    ssoServer = server
         |    ssoRealm = realm
         |    ssoGrantType = client_token
         |    clientIdVariable = "CLEAR#foo"
         |    clientSecretVariable = "CLEAR#secret"
         |  }
         | }
         |}
         |""".stripMargin).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    registry.instances.values.head shouldBe WebserviceFileDataObject(
      id = "123",
      url = "http://test",
      authMode = Some(KeycloakClientSecretAuthMode(ssoServer = "server", ssoRealm = "realm", ssoGrantType = "client_token", clientIdVariable = "CLEAR#foo", clientSecretVariable = "CLEAR#secret"))
    )
  }

  "DataObject" should "throw nice error when wrong Connection type" in {

    val config = ConfigFactory.parseString(
    """
      |connections = {
      | con1 = {
      |  type = HiveTableConnection
      |   pathPrefix = "file://c:/temp"
      |   db = default
      | }
      |}
      |dataObjects = {
      | 123 = {
      |  type = CsvFileDataObject
      |  path = foo
      |  connectionId = con1
      |  csv-options {
      |   header = true
      |  }
      | }
      |}
      |""".stripMargin).resolve

    val thrown = the [ConfigException] thrownBy ConfigParser.parse(config)

    thrown.getMessage should include ("123")
    thrown.getMessage should include ("con1")
  }
}
