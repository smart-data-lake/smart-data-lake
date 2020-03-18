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
package io.smartdatalake.util.misc

import org.apache.hadoop.conf.Configuration

/**
  * Class to make Hadoop configurations serializable; uses the
  * `Writeable` operations to do this.
  * Note: this only serializes the explicitly set values, not any set
  * in site/default or other XML resources.
  * @param conf
  */

private[smartdatalake] class SerializableHadoopConfiguration(var conf: Configuration) extends Serializable  {
  def get: Configuration = conf

  private def writeObject (out: java.io.ObjectOutputStream): Unit = {
    conf.write(out)
  }

  private def readObject (in: java.io.ObjectInputStream): Unit = {
    conf = new Configuration()
    conf.readFields(in)
  }

  private def readObjectNoData(): Unit = {
    conf = new Configuration()
  }

}
