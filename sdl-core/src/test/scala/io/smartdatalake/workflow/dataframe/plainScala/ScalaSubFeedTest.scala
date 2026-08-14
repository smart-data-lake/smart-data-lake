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
package io.smartdatalake.workflow.dataframe.plainScala

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.{ActionPipelineContext, FileSubFeed}
import org.scalatest.funsuite.AnyFunSuite

class ScalaSubFeedTest extends AnyFunSuite {

  import ScalaDataFrame.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context1: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

  test("FileSubFeed to ScalaSubFeed") {
    val fileSubFeed = FileSubFeed(None, "test1", Seq(PartitionValues(Map("dt"->"20190101"))))
    val scalaSubFeed = ScalaSubFeed.fromSubFeed(fileSubFeed)
    assert( fileSubFeed.dataObjectId == scalaSubFeed.dataObjectId)
    assert( fileSubFeed.partitionValues == scalaSubFeed.partitionValues)
  }

  test("ScalaSubFeed to FileSubFeed") {
    val scalaSubFeed = ScalaSubFeed(None, "test1", Seq(PartitionValues(Map("dt"->"20190101"))))
    val fileSubFeed = FileSubFeed.fromSubFeed(scalaSubFeed)
    assert( fileSubFeed.dataObjectId == scalaSubFeed.dataObjectId)
    assert( fileSubFeed.partitionValues == scalaSubFeed.partitionValues)
  }

  test("ScalaSubFeed union without DataFrames, without partitionValues") {
    val sf1 = ScalaSubFeed(None, "test1", Seq())
    val sf2 = ScalaSubFeed(None, "test1", Seq())
    val sfUnion = sf1.union(sf2).asInstanceOf[ScalaSubFeed]
    assert(sfUnion.partitionValues.isEmpty)
    assert(sfUnion.dataFrame.isEmpty)
  }

  test("ScalaSubFeed union with one DataFrame, with one partitionValues") {
    val df = Seq(Seq(1), Seq(2), Seq(3)).toDF("test")
    val sf1 = ScalaSubFeed(None, "test1", Seq())
    val sf2 = ScalaSubFeed(Some(df), "test1", Seq(PartitionValues(Map("dt"->"20200101"))))
    val sfUnion = sf1.union(sf2).asInstanceOf[ScalaSubFeed]
    assert(sfUnion.partitionValues.isEmpty)
    // only one side has a reusable DataFrame -> transport only the schema, the DataFrame is read again from the DataObject
    assert(sfUnion.dataFrame.isEmpty)
    assert(sfUnion.schema == df.schema)
  }

  test("ScalaSubFeed union with DataFrames, with partitionValues") {
    val df1 = Seq(Seq(1), Seq(2), Seq(3)).toDF("test")
    val sf1 = ScalaSubFeed(Some(df1), "test1", Seq(PartitionValues(Map("dt"->"20190101"))))
    val df2 = Seq(Seq(4), Seq(5), Seq(6)).toDF("test")
    val sf2 = ScalaSubFeed(Some(df2), "test1", Seq(PartitionValues(Map("dt"->"20200101"))))
    val sfUnion = sf1.union(sf2).asInstanceOf[ScalaSubFeed]
    assert(sfUnion.partitionValues.toSet == Set(PartitionValues(Map("dt"->"20190101")), PartitionValues(Map("dt"->"20200101"))))
    assert(sfUnion.dataFrame.get.collect[Int].sorted == Seq(1,2,3,4,5,6))
  }
}
