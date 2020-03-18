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
package io.smartdatalake.util.xml

import io.smartdatalake.util.misc.ParserUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Provides utility functionality for XML files.
 */
private[smartdatalake] object XmlUtil {

  /**
   * Parses an [[RDD]] as XML and returns a [[DataFrame]].
   *
   * @param className FQCN of XML parser to use
   * @param session [[SparkSession]]
   * @param rdd Spark [[RDD]] with XML
   * @return [[DataFrame]] of XML content
   */
  def callXmlParser(className: String, session: SparkSession, rdd: RDD[(String, String)]): DataFrame = {
    ParserUtil.callParser(className, session, rdd)
  }

}
