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

package io.smartdatalake.workflow

import java.time.{Duration, LocalDateTime}

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.workflow.action.{RuntimeEventState, RuntimeInfo}
import org.scalatest.FunSuite

class ActionDAGRunTest extends FunSuite {

  test("convert ActionDAGRunState to json and back") {
    val state = ActionDAGRunState(SmartDataLakeBuilderConfig(), Map("a" -> RuntimeInfo(RuntimeEventState.SUCCEEDED, startTstmp = Some(LocalDateTime.now()), duration = Some(Duration.ofMinutes(5)), msg = Some("test"))))
    val json = state.toJson
    assert(ActionDAGRunState.fromJson(json) == state)
  }

}
