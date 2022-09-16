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

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import io.smartdatalake.config.SdlConfigObject._
import io.smartdatalake.workflow.action
import io.smartdatalake.workflow.action.TestDfTransformer
import io.smartdatalake.workflow.action.generic.transformer.{DataValidationTransformer, DfTransformerWrapperDfsTransformer, FilterTransformer, RowLevelValidationRule, SQLDfsTransformer, WhitelistTransformer}
import io.smartdatalake.workflow.action.script.CmdScript
import io.smartdatalake.workflow.action.spark.customlogic.CustomFileTransformerConfig
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfTransformer
import org.scalatest.{FlatSpec, Matchers}


private[smartdatalake] class ActionImplTests extends FlatSpec with Matchers {

  val dataObjectConfig: Config = ConfigFactory.parseString(
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
      | tdo3 = {
      |   type = CsvFileDataObject
      |   csv-options {
      |     header = true
      |   }
      |   path = foo1
      | }
      | tdo4 = {
      |   type = CsvFileDataObject
      |   csv-options {
      |     header = true
      |   }
      |   path = foo2
      | }
      |}
      |""".stripMargin).resolve

  "CopyAction" should "be parsable" in {

    val customTransformerConfig = ScalaClassSparkDfTransformer(
      className = classOf[TestDfTransformer].getName
    )

    val config = ConfigFactory.parseString(
      """
        |actions = {
        | 123 = {
        |   type = CopyAction
        |   inputId = tdo1
        |   outputId = tdo2
        |   delete-data-after-read = false
        |   transformers = [
        |     { type = ScalaClassSparkDfTransformer, class-name = io.smartdatalake.workflow.action.TestDfTransformer }
        |     { type = WhitelistTransformer, columnWhitelist = [col1, col2] }
        |     { type = FilterTransformer, filterClause = "1 = 1" }
        |     { type = DataValidationTransformer, rules = [{ type = RowLevelValidationRule, condition = "a is not null" }]}
        |   ]
        | }
        |}
        |""".stripMargin).withFallback(dataObjectConfig).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    registry.getActions.head shouldBe action.CopyAction(
      id = "123",
      inputId = "tdo1",
      outputId = "tdo2",
      transformers = Seq(
        customTransformerConfig,
        WhitelistTransformer(columnWhitelist = Seq("col1", "col2")),
        FilterTransformer(filterClause = "1 = 1"),
        DataValidationTransformer(rules = Seq(RowLevelValidationRule("a is not null")))
      )
    )
  }

  "CustomDataFrameAction" should "be parsable" in {

    val config = ConfigFactory.parseString(
      """
        |actions = {
        | 123 = {
        |   type = CustomDataFrameAction
        |   inputIds = [tdo1]
        |   outputIds = [tdo2]
        |   transformers = [{
        |     type = SQLDfsTransformer
        |     code = {test = "select * from test"}
        |   }, {
        |     type = DfTransformerWrapperDfsTransformer
        |     subFeedsToApply = [test]
        |     transformer = {
        |       type = FilterTransformer
        |       filterClause = "1 = 1"
        |     }
        |   }]
        | }
        |}
        |""".stripMargin).withFallback(dataObjectConfig).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)
    registry.getActions.head shouldBe action.CustomDataFrameAction(
      id = "123",
      inputIds = Seq("tdo1"),
      outputIds = Seq("tdo2"),
      transformers = Seq(
        SQLDfsTransformer(code = Map("test" -> "select * from test")),
        DfTransformerWrapperDfsTransformer(subFeedsToApply = Seq("test"), transformer = FilterTransformer(filterClause = "1 = 1"))
      )
    )
  }

  "CustomFileAction" should "be parsable" in {

    val config = ConfigFactory.parseString(
      """
        |actions = {
        | 123 = {
        |   type = CustomFileAction
        |   inputId = tdo3
        |   outputId = tdo4
        |   transformer = {
        |     class-name = io.smartdatalake.config.objects.TestFileTransformer
        |   }
        |   breakFileRefLineage = true
        | }
        |}
        |""".stripMargin).withFallback(dataObjectConfig).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)

    registry.getActions.head shouldBe action.CustomFileAction(
      id = "123",
      inputId = "tdo3",
      outputId = "tdo4",
      breakFileRefLineage = true,
      transformer = CustomFileTransformerConfig(
        className = Some("io.smartdatalake.config.objects.TestFileTransformer")
      )
    )
  }

  "CustomScriptAction" should "be parsable" in {

    val config = ConfigFactory.parseString(
      """
        |actions = {
        | 123 = {
        |   type = CustomScriptAction
        |   inputIds = [tdo1]
        |   outputIds = [tdo2]
        |   scripts = [{
        |     type = CmdScript
        |     winCmd = test
        |     linuxCmd = test
        |   }]
        | }
        |}
        |""".stripMargin).withFallback(dataObjectConfig).resolve

    implicit val registry: InstanceRegistry = ConfigParser.parse(config)

    registry.getActions.head shouldBe action.CustomScriptAction(
      id = "123",
      inputIds = Seq("tdo1"),
      outputIds = Seq("tdo2"),
      scripts = Seq(CmdScript(winCmd = Some("test"), linuxCmd = Some("test")))
    )
  }

  "Action" should "throw nice error when wrong DataObject type" in {

    val config = ConfigFactory.parseString(
      """
        |actions = {
        | 123 = {
        |   type = CustomFileAction
        |   inputId = tdo1
        |   outputId = tdo1
        |   transformer = {
        |     class-name = io.smartdatalake.config.objects.TestFileTransformer
        |   }
        | }
        |}
        |""".stripMargin).withFallback(dataObjectConfig).resolve

    val thrown = the [ConfigException] thrownBy  ConfigParser.parse(config)

    thrown.getMessage should include ("123")
    thrown.getMessage should include ("tdo1")
  }
}
