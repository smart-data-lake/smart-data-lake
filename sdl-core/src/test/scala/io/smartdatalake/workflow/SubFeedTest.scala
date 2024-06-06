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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.dataobject.FileRef
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class SubFeedTest extends FunSuite {

  implicit val session: SparkSession = TestUtil.session
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context1: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext(instanceRegistry)

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

  test("SparkSubFeed union without DataFrames, without partitionValues") {
    val sf1 = SparkSubFeed(None, "test1", Seq())
    val sf2 = SparkSubFeed(None, "test1", Seq())
    val sfUnion = sf1.union(sf2).asInstanceOf[SparkSubFeed]
    assert(sfUnion.partitionValues.isEmpty)
    assert(sfUnion.dataFrame.isEmpty)
  }

  test("SparkSubFeed union with one DataFrame, with one partitionValues") {
    import session.implicits._
    val df = Seq(1,2,3).toDF("test")
    val sf1 = SparkSubFeed(None, "test1", Seq())
    val sf2 = SparkSubFeed(Some(SparkDataFrame(df)), "test1", Seq(PartitionValues(Map("dt"->"20200101"))))
    val sfUnion = sf1.union(sf2).asInstanceOf[SparkSubFeed]
    assert(sfUnion.partitionValues.isEmpty)
    assert(sfUnion.dataFrame.get.schema.inner == df.schema)
    assert(sfUnion.dataFrame.get.isEmpty)
  }

  test("SparkSubFeed union with DataFrames, with partitionValues") {
    import session.implicits._
    val df1 = Seq(1,2,3).toDF("test")
    val sf1 = SparkSubFeed(Some(SparkDataFrame(df1)), "test1", Seq(PartitionValues(Map("dt"->"20190101"))))
    val df2 = Seq(4,5,6).toDF("test")
    val sf2 = SparkSubFeed(Some(SparkDataFrame(df2)), "test1", Seq(PartitionValues(Map("dt"->"20200101"))))
    val sfUnion = sf1.union(sf2).asInstanceOf[SparkSubFeed]
    assert(sfUnion.partitionValues.toSet == Set(PartitionValues(Map("dt"->"20190101")), PartitionValues(Map("dt"->"20200101"))))
    assert(sfUnion.dataFrame.get.inner.as[Int].collect().toSeq.sorted == Seq(1,2,3,4,5,6))
  }

  test("FileSubFeed union with FileRefs, with partitionValues") {
    val fr1 = Seq("f1","f2","f3").map(f => FileRef(f, f, PartitionValues(Map("dt"->"20190101"))))
    val sf1 = FileSubFeed(Some(fr1), "test1", Seq(PartitionValues(Map("dt"->"20190101"))))
    val fr2 = Seq("f4","f5","f6").map(f => FileRef(f, f, PartitionValues(Map("dt"->"20200101"))))
    val sf2 = FileSubFeed(Some(fr2), "test1", Seq(PartitionValues(Map("dt"->"20200101"))))
    val sfUnion = sf1.union(sf2).asInstanceOf[FileSubFeed]
    assert(sfUnion.partitionValues.toSet == Set(PartitionValues(Map("dt"->"20190101")), PartitionValues(Map("dt"->"20200101"))))
    assert(sfUnion.fileRefs.get.sortBy(_.fileName) == fr1 ++ fr2)
  }
}
