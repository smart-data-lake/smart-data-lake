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
import io.smartdatalake.testutils.{MockDataObject, TestUtil}
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.misc.LogUtil.getRootCause
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.expectation.ExpectationScope.ExpectationScope
import io.smartdatalake.workflow.dataobject.expectation.{ExpectationScope, ExpectationValidationException, SQLExpectation}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class ValidateOnReadTest extends FunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
  }

  test("Dont validate expectations with scope=Job on read if there is a DataFrame-Action having this DataObject as output") {
    testDontValidateExpectationOnReadIfThereIsADataFrameActionHavingThisDataObjectAsOutput(ExpectationScope.Job)
  }

  test("Dont validate expectations with scope=All on read if there is a DataFrame-Action having this DataObject as output") {
    testDontValidateExpectationOnReadIfThereIsADataFrameActionHavingThisDataObjectAsOutput(ExpectationScope.All)
  }

  def testDontValidateExpectationOnReadIfThereIsADataFrameActionHavingThisDataObjectAsOutput(scope: ExpectationScope): Unit = {
    // setup DataObjects
    val srcDO = MockDataObject("src1").register
    val tgt1DO = MockDataObject("tgt1",
      expectations = Seq(SQLExpectation("countTest", scope=scope, aggExpression = "count(lastname)", expectation = Some("> 5")))
    ).register
    val tgt2DO = MockDataObject("tgt2").register

    // prepare simple DAG
    val action1 = CopyAction("ca1", srcDO.id, tgt1DO.id)
    instanceRegistry.register(action1)
    val action2 = CopyAction("ca2", tgt1DO.id, tgt2DO.id)
    instanceRegistry.register(action2)

    // check DataObjectIdsToValidateOnRead
    assert(instanceRegistry.getDataObjectIdsToValidateOnRead == Seq(srcDO.id))
    assert(instanceRegistry.shouldValidateDataObjectOnRead(srcDO.id))
    assert(!instanceRegistry.shouldValidateDataObjectOnRead(tgt1DO.id))

    val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")

    // action2 should succeed, because tgt1 expectations is not validated on read
    tgt1DO.writeSparkDataFrame(l1)
    val tgt2SubFeed = action2.exec(Seq(SparkSubFeed(None, "tgt1", Seq())))(contextExec).head
    assert(tgt2SubFeed.metrics.get.apply("count") == 2)
  }

  test("Validate expectations with scope=Job on read if there is no DataFrame-Action having this DataObject as output") {
    testValidateExpectationsOnReadIfThereIsNoDataFrameActionHavingThisDataObjectAsOutput(ExpectationScope.Job)
  }

  test("Validate expectations with scope=All on read if there is no DataFrame-Action having this DataObject as output") {
    testValidateExpectationsOnReadIfThereIsNoDataFrameActionHavingThisDataObjectAsOutput(ExpectationScope.All)
  }

  def testValidateExpectationsOnReadIfThereIsNoDataFrameActionHavingThisDataObjectAsOutput(scope: ExpectationScope): Unit = {
    // setup DataObjects
    val srcDO = MockDataObject("src1",
      expectations = Seq(SQLExpectation("countTest", scope=scope, aggExpression = "count(lastname)", expectation = Some("> 5")))
    ).register
    val tgt1DO = MockDataObject("tgt1").register

    // prepare simple DAG
    val action1 = CopyAction("ca1", srcDO.id, tgt1DO.id)
    instanceRegistry.register(action1)

    // check DataObjectIdsToValidateOnRead
    assert(instanceRegistry.getDataObjectIdsToValidateOnRead == Seq(srcDO.id))
    assert(instanceRegistry.shouldValidateDataObjectOnRead(srcDO.id))
    assert(!instanceRegistry.shouldValidateDataObjectOnRead(tgt1DO.id))

    val l1 = Seq(("jonson","rob",5),("doe","bob",3)).toDF("lastname", "firstname", "rating")

    // action1 will fail, because src1 expectations is validated on read
    srcDO.writeSparkDataFrame(l1)
    val ex = intercept[TaskFailedException](action1.exec(Seq(SparkSubFeed(None, "src1", Seq())))(contextExec).head)
    assert(getRootCause(ex).isInstanceOf[ExpectationValidationException])
  }
}
