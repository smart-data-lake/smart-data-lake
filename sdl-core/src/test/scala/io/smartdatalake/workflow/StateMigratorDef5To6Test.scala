/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow

import org.json4s.JsonAST.{JInt, JObject}
import org.json4s.jackson.JsonMethods
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StateMigratorDef5To6Test extends AnyFlatSpec with Matchers {

  private val stateV5 =
    """{
      |  "runStateFormatVersion": 5,
      |  "actionsState": {
      |    "load-test": {
      |      "results": [
      |        { "jsonClass": "SparkSubFeed", "dataObjectId": "tgt1", "filter": "dl_ts > 1", "partitionValues": [] }
      |      ]
      |    }
      |  }
      |}""".stripMargin

  "StateMigratorDef5To6" should "drop the legacy SubFeed filter and update the version" in {
    val json = JsonMethods.parse(stateV5).asInstanceOf[JObject]
    // the legacy attribute is present before the migration
    JsonMethods.compact(JsonMethods.render(json)) should include("\"filter\"")

    val migrated = new StateMigratorDef5To6().migrate(json)

    (migrated \ "runStateFormatVersion") shouldBe JInt(6)
    // the column a legacy filter belongs to is unknown, so it is dropped. `filters` defaults to an empty Seq.
    JsonMethods.compact(JsonMethods.render(migrated)) should not include "\"filter\""
    // the other attributes are untouched
    (migrated \ "actionsState" \ "load-test" \ "results" \ "dataObjectId") shouldBe
      (json \ "actionsState" \ "load-test" \ "results" \ "dataObjectId")
  }

  it should "be registered so that a version 5 state file can be read" in {
    ActionDAGRunState.runStateFormatVersion shouldBe 6
  }
}
