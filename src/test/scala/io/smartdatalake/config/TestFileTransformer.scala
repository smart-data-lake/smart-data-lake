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
package io.smartdatalake.config

import io.smartdatalake.workflow.action.customlogic.CustomFileTransformer
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}

class TestFileTransformer extends CustomFileTransformer {
  /**
   * This method has to be implemented by the custom transformer to define the transformation.
   *
   * @param input  Hadoop Input Stream of the file to be read
   * @param output Hadoop Output Stream of the file to be written
   * @return exception if something goes wrong and processing should not be stopped for all files
   */
  override def transform(input: FSDataInputStream, output: FSDataOutputStream): Option[Exception] = None

  override def equals(obj: Any): Boolean = getClass.equals(obj.getClass)
}
