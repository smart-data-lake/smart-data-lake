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
package io.smartdatalake.util.hdfs

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.util.matching.Regex

private[smartdatalake] object Partition {
  def validateColName(partitionCol: String): Unit = {
    val regexStr = "[A-Za-z0-9_]*"
    assert(partitionCol.matches(regexStr), "partition column name $partitionCol doesn't match the regex $regexStr")
  }
}

/**
 * A partition is defined by values for its partition columns.
 * It can be represented by a Map. The key of the Map are the partition column names.
 */
private[smartdatalake] case class PartitionValues(elements: Map[String, Any]) {
  def getPartitionString(partitionLayout: String): String= {
    PartitionLayout.replaceTokens(partitionLayout, this)
  }
  def getSparkExpr: Column = {
    // "and" filter concatenation of each element
    elements.map {case (k,v) => col(k) === lit(v)}.reduce( (a,b) => a and b)
  }
  def apply(colName: String): Any = elements(colName)
  def get(colName: String): Option[Any] = elements.get(colName)
  def isEmpty: Boolean = elements.isEmpty
  def nonEmpty: Boolean = elements.nonEmpty
  def keys: Set[String] = elements.keySet
  def isDefinedAt(colName: String): Boolean = elements.isDefinedAt(colName)
}

private[smartdatalake] object PartitionValues {
  val singleColFormat = "<partitionColName>=<partitionValue>[,<partitionValue>,...]"
  val multiColFormat = "<partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>[;(<partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>;...]"
  def parseSingleColArg(arg: String): Seq[PartitionValues] ={
    val keyValues = arg.split("=")
    if (keyValues.size!=2) throw new IllegalArgumentException(s"partition values $arg doesn't match format $singleColFormat")
    val partitionCol = keyValues(0)
    Partition.validateColName(partitionCol)
    val partitionValues = keyValues(1).split(",").toSeq
    partitionValues.map( v => PartitionValues(Map(partitionCol->v)))
  }
  def parseMultiColArg(arg: String): Seq[PartitionValues] = {
    val entries = arg.split(";")
    entries.toSeq.map { entry =>
      val colValues = entry.split(",")
      val singlePartitionValues = try {
        colValues.map( v => parseSingleColArg(v).head)
      } catch {
        case x:IllegalArgumentException => throw new IllegalArgumentException(s"multi partition values $arg doesn't match format $multiColFormat", x)
      }
      singlePartitionValues.reduce( (a,b) => PartitionValues(a.elements ++ b.elements))
    }
  }
}

/**
 * Helper methods to handle partition layout string
 */
private[smartdatalake] object PartitionLayout {
  private[hdfs] val delimiter = "%"
  private val tokenRegex = s"$delimiter([0-9a-zA-Z_]+)(:(.*?))?$delimiter".r.unanchored

  def replaceTokens(partitionLayout: String, partitionValues: PartitionValues, fillWithGlobIfMissing: Boolean = true): String = {
    val replacer: Regex.Match => String = (tokenMatch: Regex.Match) => {
      val partitionValue = partitionValues.get(tokenMatch.group(1)).map(_.toString)
      if (fillWithGlobIfMissing) partitionValue.getOrElse("*")
      else partitionValue.getOrElse(throw new IllegalStateException(s"partition value for $tokenMatch not found"))
    }
    tokenRegex.replaceAllIn(partitionLayout, replacer)
  }

  def extractTokens(partitionLayout: String): Seq[String] = {
    tokenRegex.findAllMatchIn(partitionLayout)
      .map( m => m.group(1)).toSeq
  }

  def extractPartitionValues(partitionLayout: String, fileName: String, path: String): PartitionValues = {
    val tokens = extractTokens( partitionLayout )
    // quote regexp characters in partition layout
    var partitionLayoutPrepared = raw"[\.\[\]]".r.replaceAllIn( partitionLayout + fileName, quoteMatch => raw"\\" + quoteMatch.group(0))
    // replace * to regexp .*
    partitionLayoutPrepared = partitionLayoutPrepared.replace("*", ".*")
    // replace tokens in partition layout with a defined or default regexp
    partitionLayoutPrepared = tokenRegex.replaceAllIn( partitionLayoutPrepared, {
      tokenMatch => if (tokenMatch.group(3) != null) s"(${tokenMatch.group(3)})" else "(.*?)"
    })
    // create regex and match with path
    val partitionLayoutRegex = partitionLayoutPrepared.r
    partitionLayoutRegex.findFirstMatchIn(path) match {
      case Some(regexMatch) =>
        val tokenValues = (1 to regexMatch.groupCount).map( i => regexMatch.group(i))
        val tokenMap = tokens.zip(tokenValues).toMap
        PartitionValues(tokenMap)
      case None => throw new Exception(s"prepared regexp partition layout $partitionLayoutPrepared didn't match path $path")
    }
  }
}
