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

  test("sorting of partition values: 1 column") {
    val orderingDt = PartitionValues.getOrdering(Seq("dt"))
    assert(
      Seq(PartitionValues(Map("dt" -> "20181201")), PartitionValues(Map("dt" -> "20170101"))).sorted(orderingDt)
        ==
        Seq(PartitionValues(Map("dt" -> "20170101")), PartitionValues(Map("dt" -> "20181201")))
    )
  }

  test("sorting of partition values: 2 columns") {
    val partValSeq = Seq(PartitionValues(Map("dt"->"20181201","cnt"->2)),PartitionValues(Map("cnt"->2,"dt"->"20170101")),PartitionValues(Map("dt"->"20181201","cnt"->1)))

    val orderingDtCnt: Ordering[PartitionValues] = PartitionValues.getOrdering(Seq("dt","cnt"))
    assert(
      partValSeq.sorted(orderingDtCnt)
        ==
        Seq(PartitionValues(Map("dt"->"20170101","cnt"->2)),PartitionValues(Map("dt"->"20181201","cnt"->1)),PartitionValues(Map("dt"->"20181201","cnt"->2)))
    )

    // Reversing the order precedence
    val orderingCntDt: Ordering[PartitionValues] = PartitionValues.getOrdering(Seq("cnt","dt"))
    assert(
      partValSeq.sorted(orderingCntDt)
        ==
        Seq(PartitionValues(Map("dt"->"20181201","cnt"->1)),PartitionValues(Map("dt"->"20170101","cnt"->2)),PartitionValues(Map("dt"->"20181201","cnt"->2)))
    )

    // not all columns considered for ordering: original order maintained
    val orderingDt = PartitionValues.getOrdering(Seq("dt"))
    assert(
      partValSeq.sorted(orderingDt)
        ==
        Seq(PartitionValues(Map("dt"->"20170101","cnt"->2)),PartitionValues(Map("dt"->"20181201","cnt"->2)),PartitionValues(Map("dt"->"20181201","cnt"->1)))
    )

    // more columns considered for ordering than available: ordering on available columns
    val orderingDtCntTest = PartitionValues.getOrdering(Seq("dt","cnt","test"))
    assert(
      partValSeq.sorted(orderingDtCntTest)
        ==
        Seq(PartitionValues(Map("dt"->"20170101","cnt"->2)),PartitionValues(Map("dt"->"20181201","cnt"->1)),PartitionValues(Map("dt"->"20181201","cnt"->2)))
    )

  }

  test("check expected partition values") {
    val partitionValues3 = Seq(PartitionValues(Map("date" -> "20190101", "town" -> "NYC", "year" -> "2019")))
    val partitionValues3a = Seq(PartitionValues(Map("date" -> "20190101", "town" -> "NYC", "year" -> "2020")))
    val partitionValues2 = Seq(PartitionValues(Map("date" -> "20190101", "town" -> "NYC")))
    val partitionValues2r = Seq(PartitionValues(Map("town" -> "NYC", "date" -> "20190101")))
    val partitionValues1 = Seq(PartitionValues(Map("date" -> "20190101")))
    assert(PartitionValues.checkExpectedPartitionValues(partitionValues3, partitionValues3).isEmpty)
    assert(PartitionValues.checkExpectedPartitionValues(partitionValues3, partitionValues2).isEmpty)
    assert(PartitionValues.checkExpectedPartitionValues(partitionValues3, partitionValues2r).isEmpty)
    assert(PartitionValues.checkExpectedPartitionValues(partitionValues2, partitionValues3).nonEmpty)
    assert(PartitionValues.checkExpectedPartitionValues(partitionValues3, partitionValues1).isEmpty)
    assert(PartitionValues.checkExpectedPartitionValues(partitionValues1, partitionValues3).nonEmpty)
    assert(PartitionValues.checkExpectedPartitionValues(partitionValues3 ++ partitionValues3a, partitionValues3 ++ partitionValues3a).isEmpty)
    assert(PartitionValues.checkExpectedPartitionValues(partitionValues3 ++ partitionValues3a, partitionValues3).isEmpty)
    assert(PartitionValues.checkExpectedPartitionValues(partitionValues3, partitionValues3 ++ partitionValues3a).nonEmpty)
  }

  test("check isComplete") {
    assert(PartitionValues(Map("town" -> "NYC", "date" -> "20190101")).isComplete(Seq("town","date")))
    assert(!PartitionValues(Map("town" -> "NYC", "date" -> "20190101")).isComplete(Seq("town","abc")))
    assert(!PartitionValues(Map("town" -> "NYC", "date" -> "20190101")).isComplete(Seq("town")))
    assert(!PartitionValues(Map("town" -> "NYC", "date" -> "20190101")).isComplete(Seq("abc")))
  }

  test("check isInitOf") {
    assert(PartitionValues(Map("town" -> "NYC", "date" -> "20190101")).isInitOf(Seq("town","date")))
    assert(!PartitionValues(Map("town" -> "NYC", "date" -> "20190101")).isInitOf(Seq("town","abc")))
    assert(!PartitionValues(Map("town" -> "NYC", "date" -> "20190101")).isInitOf(Seq("town")))
    assert(PartitionValues(Map("town" -> "NYC", "date" -> "20190101")).isInitOf(Seq("town","date","abc")))
    assert(!PartitionValues(Map("town" -> "NYC", "date" -> "20190101")).isInitOf(Seq("abc")))
  }

  test("check isIncludedIn") {
    assert(PartitionValues(Map("town" -> "NYC", "date" -> "20190101")).isIncludedIn(PartitionValues(Map("date" -> "20190101"))))
    assert(!PartitionValues(Map("town" -> "NYC", "date" -> "20180101")).isIncludedIn(PartitionValues(Map("date" -> "20190101"))))
    assert(!PartitionValues(Map("town" -> "NYC", "abc" -> "a")).isIncludedIn(PartitionValues(Map("date" -> "20190101"))))
    assert(!PartitionValues(Map("town" -> "NYC", "abc" -> "20190101")).isIncludedIn(PartitionValues(Map("date" -> "20190101"))))
  }
}
