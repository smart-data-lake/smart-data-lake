/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019 ELCA Informatique SA (<https://www.elca.ch>)
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

class PartitionLayoutTest extends FunSuite {

  test("extracting tokens from partition layout") {
    val delimiter = PartitionLayout.delimiter
    val testLayout = s"abc/date${delimiter}date:[0-9]+-[0-9]+-[0-9]+$delimiter-${delimiter}type$delimiter-"
    val tokens = PartitionLayout.extractTokens(testLayout)
    assert(tokens == Seq("date", "type"))
  }

  test("creating partition string according to partition layout") {
    val delimiter = PartitionLayout.delimiter
    val testLayout = s"abc/date${delimiter}date:[0-9]+-[0-9]+-[0-9]+$delimiter-${delimiter}type$delimiter-"
    val partitionValues = PartitionValues(Map("date" -> "2000-01-01", "type" -> "ZZ"))
    val partitionString = partitionValues.getPartitionString(testLayout)
    assert(partitionString == "abc/date2000-01-01-ZZ-")
  }

  test("extracting partition values from partition string according to partition layout") {
    val delimiter = PartitionLayout.delimiter
    val testLayout = s"abc/date${delimiter}date:[0-9]+-[0-9]+-[0-9]+$delimiter-${delimiter}type$delimiter-"
    val partitionString = "abc/date2000-01-01-ZZ-test.csv"
    val partitionValues = PartitionLayout.extractPartitionValues(testLayout, "*.csv", partitionString)
    assert(partitionValues == PartitionValues(Map("date" -> "2000-01-01", "type" -> "ZZ")))
  }
}
