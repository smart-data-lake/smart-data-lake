/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.misc

import org.scalatest.FunSuite

class GraphUtilTest extends FunSuite {

  val graph1 = GraphUtil.Graph(Set(("A","B"),("A","C"),("B","D"),("C","D"),("D","E")))
  val allNodes = graph1.edges.flatMap(e => Set(e._1,e._2))

  test("get connected outbound nodes") {
    assert(graph1.getConnectedNodesForward("A") == allNodes)
    assert(graph1.getConnectedNodesForward("B") == Set("B", "D", "E"))
    assert(graph1.getConnectedNodesForward("D") == Set("D", "E"))
    assert(graph1.getConnectedNodesForward("E") == Set("E"))
  }

  test("get connected inbound nodes") {
    assert(graph1.getConnectedNodesReverse("E") == allNodes)
    assert(graph1.getConnectedNodesReverse("D") == Set("A", "B", "C", "D"))
    assert(graph1.getConnectedNodesReverse("B") == Set("A", "B"))
    assert(graph1.getConnectedNodesReverse("A") == Set("A"))
  }

}
