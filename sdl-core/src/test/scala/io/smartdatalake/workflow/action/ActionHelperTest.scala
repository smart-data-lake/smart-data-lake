/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action

import org.scalatest.FunSuite

class ActionHelperTest extends FunSuite {

  test("replaceSpecialCharactersWithUnderscore") {
    assert(ActionHelper.replaceSpecialCharactersWithUnderscore("1-x.y+z!9") == "1_x_y_z_9")
  }

  test("createTemporaryViewName") {
    assert(ActionHelper.createTemporaryViewName("1-x.y+z!9") == "1_x_y_z_9_sdltemp")
  }

  test("replaceLegacyViewName") {
    assert(ActionHelper.replaceLegacyViewName("select * from src1", "src1_sdltemp") == "select * from src1_sdltemp")
    assert(ActionHelper.replaceLegacyViewName("select src1.* from src1", "src1_sdltemp") == "select src1_sdltemp.* from src1_sdltemp")
    assert(ActionHelper.replaceLegacyViewName("select s.* from src1 as s", "src1_sdltemp") == "select s.* from src1_sdltemp as s")
  }

}
