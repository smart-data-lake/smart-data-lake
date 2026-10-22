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
package io.smartdatalake.workflow.sparkconnect

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.{ConfigToolbox, InstanceRegistry}
import io.smartdatalake.testutils.sparkconnect.SparkConnectTestUtil
import io.smartdatalake.workflow.connection.SparkConnectConnection
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end test running a SmartDataLakeBuilder job with a CopyAction between two
 * SparkConnectTableDataObjects. Needs a Spark Connect server, see [[SparkConnectTestUtil]] for how it is resolved or started.
 */
class SparkConnectPipelineTest extends AnyFunSuite {

  test("run CopyAction pipeline end-to-end over Spark Connect") {
    assume(SparkConnectTestUtil.serverAvailable, s"No Spark Connect server available at ${SparkConnectTestUtil.url}")

    // seed source table with a separate connection/session
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
    val connection = SparkConnectConnection("seedCon", SparkConnectTestUtil.url)
    instanceRegistry.register(connection)
    implicit val context: ActionPipelineContext = ConfigToolbox.getDefaultActionPipelineContext(instanceRegistry).copy(phase = ExecutionPhase.Exec)
    val session = connection.sparkSession
    import session.implicits._
    session.sql("DROP TABLE IF EXISTS default.sdlb_e2e_src").collect()
    session.sql("DROP TABLE IF EXISTS default.sdlb_e2e_tgt").collect()
    Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "value")
      .write.mode("overwrite").saveAsTable("default.sdlb_e2e_src")

    // run SDLB job
    val configPath = getClass.getResource("/config/e2e.conf").getPath
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "e2e", configuration = Seq(configPath), applicationName = Some("sparkconnect-e2e"))
    DefaultSmartDataLakeBuilder.run(sdlConfig)

    // check target table content
    val dfTgt = session.read.table("default.sdlb_e2e_tgt")
    assert(dfTgt.count() == 3)
    assert(dfTgt.columns.toSeq == Seq("id", "value"))

    // cleanup
    session.sql("DROP TABLE IF EXISTS default.sdlb_e2e_src").collect()
    session.sql("DROP TABLE IF EXISTS default.sdlb_e2e_tgt").collect()
  }
}
