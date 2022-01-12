/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package org.apache.spark.custom

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil

object PrivateAccessor {
  def getHadoopConfiguration(properties: Map[String,String]): Configuration = {
    val sparkConf = new SparkConf().setAll(properties)
    SparkHadoopUtil.get.newConfiguration(sparkConf)
  }
}
