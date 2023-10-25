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
package io.smartdatalake.config.objects

import com.typesafe.config.Config
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject._

/**
 * A dummy [[HousekeepingMode]] for unit tests.
 *
 * @param arg1 some dummy argument
 */
case class TestHousekeepingMode(arg1: Option[String]) extends HousekeepingMode {
  override private[smartdatalake] def prepare(dataObject: DataObject)(implicit context: ActionPipelineContext): Unit = None

  override private[smartdatalake] def postWrite(dataObject: DataObject)(implicit context: ActionPipelineContext): Unit = None

  override def factory: FromConfigFactory[HousekeepingMode] = TestHousekeepingMode
}

object TestHousekeepingMode extends FromConfigFactory[HousekeepingMode] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): TestHousekeepingMode = {
    extract[TestHousekeepingMode](config)
  }
}