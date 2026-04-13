/*
 * sdl-core - Build your data lake the smart way.
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
package io.smartdatalake.workflow.action.spark

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.SaveModeMergeOptions
import io.smartdatalake.testutils.{MockSparkDataObject, TestUtil}
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

class CopyWithMergeActionTest extends AnyFunSuite with BeforeAndAfter
  with io.smartdatalake.testutils.spark.dataset.TestToolDataset
  with io.smartdatalake.util.spark.dataset.Equality {

  @transient implicit private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
    instanceRegistry.register(TestUtil.defaultSparkConnection)
  }

  test("copy 1st 2nd load, SaveModeMergeOptions, schema evolution") {

    // setup DataObjects
    val feed = "copy"
    val srcDO = MockSparkDataObject("src1").register
    val tgtDO = MockSparkDataObject("tgt1", primaryKey = Some(Seq("lastname", "firstname"))).register

    // prepare & start 1st load
    val action1 = CopyAction("dda", srcDO.id, tgtDO.id, saveModeOptions = Some(SaveModeMergeOptions()))
    val l1 = Seq(("doe", "john", 5), ("pan", "peter", 5), ("hans", "muster", 5)).toDF("lastname", "firstname", "rating")
    srcDO.writeSparkDataFrame(l1, Seq())
    val srcSubFeed = SparkSubFeed(None, "src1", Seq())
    action1.init(Seq(srcSubFeed))
    action1.exec(Seq(srcSubFeed))(contextExec)

    {
      val expected = Seq(("doe", "john", 5), ("pan", "peter", 5), ("hans", "muster", 5))
        .toDF("lastname", "firstname", "rating")
      val actual = tgtDO.getSparkDataFrame()(contextExec)
      val resultat = expected.equal(actual)
      if (!resultat) printFailedTestResult("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }

    // prepare & start 2nd load - schema evolution: column rating -> rating2!
    val l2 = Seq(("doe", "john", 10), ("pan", "peter", 5), ("pan", "peter2", 5)).toDF("lastname", "firstname", "rating2")
    srcDO.writeSparkDataFrame(l2, Seq())
    action1.init(Seq(srcSubFeed))
    action1.exec(Seq(SparkSubFeed(None, "src1", Seq())))(contextExec)

    {
      val expected = Seq(("doe", "john", Some(5), Some(10)), ("pan", "peter", Some(5), Some(5)), ("pan", "peter2", None, Some(5)), ("hans", "muster", Some(5), None))
        .toDF("lastname", "firstname", "rating", "rating2")
      val actual = tgtDO.getSparkDataFrame()(contextExec)
      val resultat = expected.equal(actual)
      if (!resultat) printFailedTestResult("deduplicate 1st 2nd load", Seq())(actual)(expected)
      assert(resultat)
    }
  }
}
