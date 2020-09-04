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
package io.smartdatalake.util.xml

import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

private[smartdatalake] class ParserXml extends SmartDataLakeLogger {
  
  // Prase XML and write DataFrame
  def parse(sqlContext: SQLContext, rdd: RDD[(String, String)]): DataFrame = {
    import sqlContext.implicits._

    // String in XML-Elemente umwandlen
    logger.info("Parse XML")

    val rddXmlElems = rdd.map{ case (id, content) => (id, scala.xml.XML.loadString(content.replaceAll("\n", " ")))}
    
    if (logger.isTraceEnabled()) {
      rddXmlElems.flatMap { case (id, xml) => xml.child.filterNot { n => "#PCDATA".equals(n.label) }.map { n => logger.trace(n.label, n.text) } }
    }

    // Prepare DataFrame. The String contains #PCDATA for characters between end char > and start char <
    val rddRows = rddXmlElems.flatMap { case (file, xml) => xml.child.filterNot { n => "#PCDATA".equals(n.label) }.map { n => (file,n.label, n.text) } }
    rddRows.toDF("id", "label", "text").groupBy($"id").pivot("label").agg(first($"text"))
  }
}
