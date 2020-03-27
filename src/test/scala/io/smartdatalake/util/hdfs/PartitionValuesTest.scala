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

package io.smartdatalake.util.hdfs

import org.scalatest.FunSuite

class PartitionValuesTest extends FunSuite {

  test("sorting of partition values") {
    val ordering1 = PartitionValues.getOrdering(Seq("dt"))
    assert(
      Seq(PartitionValues(Map("dt"->"20181201")),PartitionValues(Map("dt"->"20170101"))).sorted(ordering1)
      ==
      Seq(PartitionValues(Map("dt"->"20170101")),PartitionValues(Map("dt"->"20181201")))
    )
    val ordering2 = PartitionValues.getOrdering(Seq("dt","cnt"))
    assert(
      Seq(PartitionValues(Map("dt"->"20181201","cnt"->2)),PartitionValues(Map("dt"->"20170101","cnt"->2)),PartitionValues(Map("dt"->"20181201","cnt"->1))).sorted(ordering2)
      ==
      Seq(PartitionValues(Map("dt"->"20170101","cnt"->2)),PartitionValues(Map("dt"->"20181201","cnt"->1)),PartitionValues(Map("dt"->"20181201","cnt"->2)))
    )
  }
}
