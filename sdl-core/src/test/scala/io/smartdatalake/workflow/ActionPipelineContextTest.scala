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

import io.smartdatalake.app.{GlobalConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.action.SDLExecutionId
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

/**
 * Tests for [[ActionPipelineContext.referenceTimestamp]], which defaults to `runStartTime` and can be
 * overridden by the SDLB parameter `referenceTimestamp`. See issue #427.
 */
class ActionPipelineContextTest extends AnyFlatSpec with Matchers {

  private val runStartTime = LocalDateTime.of(2026, 8, 24, 10, 0, 0)
  private val attemptStartTime = LocalDateTime.of(2026, 8, 24, 11, 0, 0)
  private val overrideTimestamp = LocalDateTime.of(2021, 12, 5, 18, 35, 58)

  private def context: ActionPipelineContext = ActionPipelineContext(
    feed = "feedTest",
    application = "appTest",
    executionId = SDLExecutionId.executionId1,
    instanceRegistry = new InstanceRegistry,
    appConfig = SmartDataLakeBuilderConfig("feedTest", Some("appTest")),
    runStartTime = runStartTime,
    attemptStartTime = attemptStartTime,
    globalConfig = GlobalConfig()
  )

  /**
   * Reset the memoized Environment.referenceTimestamp so that the given parameter sources are
   * evaluated, restoring the previous state afterwards.
   */
  private def withReferenceTimestampParameter[T](sysProp: Option[String] = None, globalEnvironment: Map[String, String] = Map())(fn: => T): T = {
    val previousValue = Environment._referenceTimestamp
    val previousGlobalConfig = Environment._globalConfig
    Environment._referenceTimestamp = None
    Environment._globalConfig = GlobalConfig(environment = globalEnvironment)
    sysProp.foreach(System.setProperty("sdl.referenceTimestamp", _))
    try fn
    finally {
      System.clearProperty("sdl.referenceTimestamp")
      Environment._referenceTimestamp = previousValue
      Environment._globalConfig = previousGlobalConfig
    }
  }

  "referenceTimestamp" should "default to the start time of the run" in {
    withReferenceTimestampParameter() {
      context.referenceTimestamp shouldBe runStartTime
    }
  }

  it should "be overridden by the java system property sdl.referenceTimestamp" in {
    withReferenceTimestampParameter(sysProp = Some("2021-12-05 18:35:58")) {
      context.referenceTimestamp shouldBe overrideTimestamp
    }
  }

  it should "be overridden by the global.environment configuration section" in {
    withReferenceTimestampParameter(globalEnvironment = Map("referenceTimestamp" -> "2021-12-05 18:35:58")) {
      context.referenceTimestamp shouldBe overrideTimestamp
    }
  }

  it should "let the java system property take precedence over global.environment" in {
    withReferenceTimestampParameter(sysProp = Some("2021-12-05 18:35:58"), globalEnvironment = Map("referenceTimestamp" -> "1999-01-01 00:00:00")) {
      context.referenceTimestamp shouldBe overrideTimestamp
    }
  }

  it should "stay stable over the phases and actions of a run" in {
    withReferenceTimestampParameter() {
      val prepareContext = context
      prepareContext.copy(phase = ExecutionPhase.Exec).referenceTimestamp shouldBe runStartTime
      prepareContext.copy(attemptStartTime = LocalDateTime.now).referenceTimestamp shouldBe runStartTime
    }
  }

  it should "stay stable over the attempts of a run, but follow a new run" in {
    withReferenceTimestampParameter() {
      // a new attempt keeps runStartTime, so the reference timestamp must not change
      context.incrementAttemptId.referenceTimestamp shouldBe runStartTime
      // a new run (e.g. the next iteration of a streaming job) gets a new runStartTime
      context.incrementRunId.referenceTimestamp should not be runStartTime
    }
  }

  it should "keep the override over attempts and runs" in {
    withReferenceTimestampParameter(sysProp = Some("2021-12-05 18:35:58")) {
      context.incrementAttemptId.referenceTimestamp shouldBe overrideTimestamp
      context.incrementRunId.referenceTimestamp shouldBe overrideTimestamp
    }
  }
}
