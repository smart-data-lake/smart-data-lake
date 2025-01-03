/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.dataobject.expectations

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.misc.LogUtil.getRootCause
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.expectation.{ExpectationScope, ExpectationValidationException, UniqueKeyExpectation}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class UniqueKeyExpectationTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
  }

  test("succeed and fail primary key validation with scope=Job") {
    // setup DataObjects
    val srcDO = MockDataObject("src1").register
    val tgtDO = MockDataObject("tgt1", primaryKey = Some(Seq("lastname")), saveMode = SDLSaveMode.Append,
      expectations = Seq(UniqueKeyExpectation("primaryKeyTest", approximate=false))
    ).register

    // prepare
      val action1 = CopyAction("ca", srcDO.id, tgtDO.id)
      val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
      srcDO.writeSparkDataFrame(l1, Seq())
      val srcSubFeed = SparkSubFeed(None, "src1", Seq())

    // start first load -> should succeed
    {
      val tgtSubFeed1 = action1.exec(Seq(srcSubFeed))(contextExec).head
      assert(tgtSubFeed1.metrics.get == Map("count" -> 2, "primaryKeyTest" -> 1.0, "count#mainInput" -> 2, "records_written" -> 2, "count#src1" -> 2))
    }

    // start 2nd load, append same data again -> should succeed as primary key validation is done only per job
    {
      val tgtSubFeed1 = action1.exec(Seq(srcSubFeed))(contextExec).head
      assert(tgtSubFeed1.metrics.get == Map("count" -> 2, "primaryKeyTest" -> 1.0, "count#mainInput" -> 2, "records_written" -> 2, "count#src1" -> 2))
    }

    // prepare source with duplicate record
    val l2 = Seq(("jonson","rob",5),("jonson","bob",3)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l2, Seq())

    // start 3nd load -> should fail because of duplicate records processed in job
    {
      val ex = intercept[TaskFailedException](action1.exec(Seq(srcSubFeed))(contextExec).head)
      assert(getRootCause(ex).isInstanceOf[ExpectationValidationException])
    }
  }

  test("succeed and fail primary key validation with scope=All") {
    // setup DataObjects
    val srcDO = MockDataObject("src1").register
    val tgtDO = MockDataObject("tgt1", primaryKey=Some(Seq("lastname")), saveMode=SDLSaveMode.Append,
      expectations = Seq(UniqueKeyExpectation("primaryKeyTest", approximate=false, scope=ExpectationScope.All))
    ).register

    // prepare
    val action1 = CopyAction("ca", srcDO.id, tgtDO.id)
    val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())

    // start first load -> should succeed
    {
      val tgtSubFeed1 = action1.exec(Seq(srcSubFeed))(contextExec).head
      assert(tgtSubFeed1.metrics.get == Map("count" -> 2, "primaryKeyTest" -> 1.0, "count#mainInput" -> 2, "records_written" -> 2, "count#src1" -> 2, "countAll" -> 2))
    }

    // start 2nd load, append same data again -> should fail because duplicate records present in tgt after writing
    {
      val ex = intercept[TaskFailedException](action1.exec(Seq(srcSubFeed))(contextExec).head)
      assert(getRootCause(ex).isInstanceOf[ExpectationValidationException])
    }
  }
}
