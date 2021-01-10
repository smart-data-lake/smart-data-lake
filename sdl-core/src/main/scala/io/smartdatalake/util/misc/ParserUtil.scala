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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.reflectiveCalls

private[smartdatalake] object ParserUtil {

  /**
   * Parses a [[RDD]] and returns a [[DataFrame]].
   *
   * @param className FQCN of xml parser to use
   * @param session Spark [[SparkSession]]
   * @param rdd Spark [[RDD[(String, String)]]]
   * @return [[DataFrame]]
   */
  def callParser(className: String, session: SparkSession, rdd: RDD[(String, String)]): DataFrame = {
    val parserClass = className
    val df = Class.forName(parserClass).getDeclaredConstructor().newInstance().asInstanceOf[{
      def parse(session: SparkSession, rdd: RDD[(String, String)]): DataFrame
    }]
    // TODO: This is a reflexive call because it calls a method 'parse' which may or may not be a member of the
    //  class named <className>. This is ugly and unsafe code.
    //  It should either be removed or solved with proper type declarations.
    df.parse(session, rdd)
  }

}
