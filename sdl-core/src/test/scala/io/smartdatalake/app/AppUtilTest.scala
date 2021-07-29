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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.objects.TestAction
import io.smartdatalake.workflow.action.customlogic.CustomDfsTransformerConfig
import io.smartdatalake.workflow.action.{Action, ActionMetadata, CopyAction, CustomSparkAction}
import io.smartdatalake.workflow.dataobject.{CsvFileDataObject, DataObjectMetadata, HiveTableDataObject}
import org.scalatest.FunSuite

class AppUtilTest extends FunSuite {

  test("mask secrets when logging spark conf") {
    val logTxt = AppUtil.createMaskedSecretsKVLog("secret.key", "+yQs4uO+taUi27+baM5D1/ishD8wDBuxj6+so0uk")
    assert(logTxt == "secret.key=...")
  }

  test("dont mask normal values when logging spark conf") {
    val logTxt = AppUtil.createMaskedSecretsKVLog("test", "abc")
    assert(logTxt == "test=abc")
  }

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  private val do1 = CsvFileDataObject("do1", "/dummy", metadata = Some(DataObjectMetadata(name = Some("dataObject1"), layer = Some("L1"))))
  private val do2 = CsvFileDataObject("do2", "/dummy", metadata = Some(DataObjectMetadata(name = Some("dataObject2"), layer = Some("L1"))))
  private val do3 = CsvFileDataObject("do3", "/dummy", metadata = Some(DataObjectMetadata(name = Some("dataObject3"), layer = Some("L2"))))
  private val do4 = CsvFileDataObject("do4", "/dummy", metadata = Some(DataObjectMetadata(name = Some("dataObject4"), layer = Some("L1"))))
  private val do5 = CsvFileDataObject("do5", "/dummy", metadata = Some(DataObjectMetadata(name = Some("dataObject5"), layer = Some("L2"))))
  private val do6 = CsvFileDataObject("do6", "/dummy", metadata = Some(DataObjectMetadata(name = Some("dataObject6"), layer = Some("L3"))))
  instanceRegistry.register(Seq(do1,do2,do3,do4,do5,do6))
  private val actionA = CopyAction("a", "do1", "do2", metadata = Some(ActionMetadata(name = Some("actionA"), feed = Some("test1"))))
  private val actionB = CopyAction("b", "do1", "do3", metadata = Some(ActionMetadata(name = Some("actionB"), feed = Some("test2"))))
  private val actionC = CustomSparkAction("c", Seq("do2","do3","do4"), Seq("do5"), transformer = CustomDfsTransformerConfig(sqlCode = Some(Map(do5.id -> "select * from do2")))
    , metadata = Some(ActionMetadata(name = Some("actionC"), feed = Some("test1")))
  )
  private val actionD = CopyAction("d", "do5", "do6", metadata = Some(ActionMetadata(name = Some("actionD"), feed = Some("test1"))))
  private val actions1: Set[Action] = Set(actionA, actionB, actionC, actionD)

  test("filter action list by feedName") {
    assert(AppUtil.filterActionList("test1", actions1) == Set(actionA, actionC, actionD)) // default prefix is "feeds:"
    assert(AppUtil.filterActionList("feeds:test1", actions1) == Set(actionA, actionC, actionD))
    assert(AppUtil.filterActionList("feeds:test.*", actions1) == Set(actionA, actionB, actionC, actionD))
    assert(AppUtil.filterActionList("feeds:test2", actions1) == Set(actionB))
    assert(AppUtil.filterActionList("test1;", actions1) == Set(actionA, actionC, actionD)) // empty expression at the end
    assert(AppUtil.filterActionList("|test1", actions1) == Set(actionA, actionC, actionD)) // operation without prefix
    assert(AppUtil.filterActionList("|feeds:test1", actions1) == Set(actionA, actionC, actionD)) // operation and prefix
  }

  test("filter action list by name") {
    assert(AppUtil.filterActionList("names:action[AB]", actions1) == Set(actionA, actionB))
  }

  test("filter action list by id") {
    assert(AppUtil.filterActionList("ids:[AB]", actions1) == Set(actionA, actionB))
  }

  test("filter action list by layer") {
    assert(AppUtil.filterActionList("layers:L3", actions1) == Set(actionD))
  }

  test("filter action list startFromActionIds") {
    assert(AppUtil.filterActionList("startFromActionIds:a", actions1) == Set(actionA, actionC, actionD))
  }

  test("filter action list endWithActionIds") {
    assert(AppUtil.filterActionList("endWithActionIds:c", actions1) == Set(actionA, actionB, actionC))
  }

  test("filter action list startFromDataObjectIds") {
    assert(AppUtil.filterActionList("startFromDataObjectIds:do1", actions1) == Set(actionA, actionB, actionC, actionD))
    assert(AppUtil.filterActionList("startFromDataObjectIds:do3", actions1) == Set(actionC, actionD))
  }

  test("filter action list endWithDataObjectIds") {
    assert(AppUtil.filterActionList("endWithDataObjectIds:do5", actions1) == Set(actionA, actionB, actionC))
  }

  test("filter action list with complex selector") {
    assert(AppUtil.filterActionList("endWithDataObjectIds:do5;feeds:test1", actions1) == Set(actionA, actionB, actionC, actionD)) // union is default
    assert(AppUtil.filterActionList("endWithDataObjectIds:do5;|feeds:test1", actions1) == Set(actionA, actionB, actionC, actionD)) // union
    assert(AppUtil.filterActionList("endWithDataObjectIds:do5;&feeds:test1", actions1) == Set(actionA, actionC)) // intersection
    assert(AppUtil.filterActionList("endWithDataObjectIds:do5;-feeds:test1", actions1) == Set(actionB)) // minus
  }

  test("filter action list with complex selector 2") {
    assert(AppUtil.filterActionList("startFromDataObjectIds:do2;|layers:L3", actions1) == Set(actionC, actionD)) // union
    assert(AppUtil.filterActionList("startFromDataObjectIds:do2;&layers:L3", actions1) == Set(actionD)) // intersection
    assert(AppUtil.filterActionList("startFromDataObjectIds:do2;-layers:L3", actions1) == Set(actionC)) // minus
  }

  test("filter action list with wrong operation") {
    intercept[RuntimeException](AppUtil.filterActionList("endWithDataObjectIds:do5;+feeds:test1", actions1))
  }

}
