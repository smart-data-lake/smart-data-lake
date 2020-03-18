/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.util.json

import io.smartdatalake.util.misc.ParserUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Provides utility functions for JSON.
 */
private[smartdatalake] object JsonUtil {

  /**
   * Parses a [[RDD]] as JSON and returns a [[DataFrame]]
   *
   * @param className FQCN of xml parser to load
   * @param session Spark [[SparkSession]]
   * @param rdd Spark [[RDD]] with JSON
   * @return [[DataFrame]] with JSON
   */
  def callJsonParser(className: String, session: SparkSession, rdd: RDD[(String, String)]): DataFrame = {
    ParserUtil.callParser(className, session, rdd)
  }


}
