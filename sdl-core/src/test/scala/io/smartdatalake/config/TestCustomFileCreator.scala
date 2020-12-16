/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

package io.smartdatalake.config

import java.io.ByteArrayInputStream

import io.smartdatalake.workflow.action.customlogic.CustomFileCreator
import org.apache.spark.sql.SparkSession

class TestCustomFileCreator extends CustomFileCreator {

  /**
   * This function creates a [[ByteArrayInputStream]] based on custom code.
   *
   * @param session the Spark Session
   * @param config  input config of the action
   * @return a stream containing the custom file data as a sequence of bytes
   */
  override def exec(session: SparkSession, config: Map[String, String]): ByteArrayInputStream = new ByteArrayInputStream(TestCustomFileCreator.fileContents.getBytes)
}

object TestCustomFileCreator {
  val fileContents = "testFileContents"
}
