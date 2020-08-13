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

import java.time.LocalDateTime

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase

private[smartdatalake] case class ActionPipelineContext(
                                                         feed: String,
                                                         application: String,
                                                         runId: Int,
                                                         attemptId: Int,
                                                         instanceRegistry: InstanceRegistry,
                                                         referenceTimestamp: Option[LocalDateTime] = None,
                                                         appConfig: SmartDataLakeBuilderConfig, // application config is needed to persist action dag state for recovery
                                                         runStartTime: LocalDateTime = LocalDateTime.now(),
                                                         attemptStartTime: LocalDateTime = LocalDateTime.now(),
                                                         simulation: Boolean = false,
                                                         var phase: ExecutionPhase = ExecutionPhase.Prepare
) {
  def getReferenceTimestampOrNow: LocalDateTime = referenceTimestamp.getOrElse(LocalDateTime.now)
}
