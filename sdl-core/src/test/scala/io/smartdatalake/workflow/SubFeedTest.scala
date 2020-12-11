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

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.definitions.Environment.instanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import org.scalatest.FunSuite

class SubFeedTest extends FunSuite {

  implicit val session = TestUtil.sessionHiveCatalog
  implicit val context1: ActionPipelineContext = ActionPipelineContext("testfeed", "testapp", 1, 1, instanceRegistry, None, SmartDataLakeBuilderConfig())

  test("FileSubFeed to SparkSubFeed") {
    val fileSubFeed = FileSubFeed(None, "test1", Seq(PartitionValues(Map("dt"->"20190101"))))
    val sparkSubFeed = SparkSubFeed.fromSubFeed(fileSubFeed)
    assert( fileSubFeed.dataObjectId == sparkSubFeed.dataObjectId)
    assert( fileSubFeed.partitionValues == sparkSubFeed.partitionValues)
  }

  test("SparkSubFeed to FileSubFeed") {
    val sparkSubFeed = SparkSubFeed(None, "test1", Seq(PartitionValues(Map("dt"->"20190101"))))
    val fileSubFeed = FileSubFeed.fromSubFeed(sparkSubFeed)
    assert( fileSubFeed.dataObjectId == sparkSubFeed.dataObjectId)
    assert( fileSubFeed.partitionValues == sparkSubFeed.partitionValues)
  }
}
