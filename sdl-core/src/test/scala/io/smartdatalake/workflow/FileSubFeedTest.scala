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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.plainScala.ScalaTestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.dataobject.file.FileRef
import org.scalatest.funsuite.AnyFunSuite

class FileSubFeedTest extends AnyFunSuite {

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context1: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext

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
