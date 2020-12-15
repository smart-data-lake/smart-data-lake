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

import org.apache.spark.annotation.DeveloperApi
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
@DeveloperApi
case class PartitionValues(elements: Map[String, Any]) {
  private[smartdatalake] def getPartitionString(partitionLayout: String): String= {
    PartitionLayout.replaceTokens(partitionLayout, this)
  }
  private[smartdatalake] def getSparkExpr: Column = {
    // "and" filter concatenation of each element
    elements.map {case (k,v) => col(k) === lit(v)}.reduce( (a,b) => a and b)
  }
  override def toString: String = {
    elements.map {case (k,v) => s"$k=$v"}.mkString("/")
  }
  def apply(colName: String): Any = elements(colName)
  def get(colName: String): Option[Any] = elements.get(colName)
  def isEmpty: Boolean = elements.isEmpty
  def nonEmpty: Boolean = elements.nonEmpty
  def keys: Set[String] = elements.keySet
  def isDefinedAt(colName: String): Boolean = elements.isDefinedAt(colName)
  def filterKeys(colNames: Seq[String]): PartitionValues = this.copy(elements = elements.filterKeys(colNames.contains))
  def getMapString: Map[String,String] = elements.mapValues(_.toString)
}

private[smartdatalake] object PartitionValues {
  val singleColFormat = "<partitionColName>=<partitionValue>[,<partitionValue>,...]"
  val multiColFormat = "<partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>[;(<partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>;...]"

  /**
   * Defines an Ordering for sorting PartitionValues.
   * Sorting a list of partition values is only possible, if the partition columns to be considered are defined.
   * As PartitionValues is a generic structure, the type of a value needs to be inferred for comparision.
   * @param partitions partition columns to use for sorting
   * @return Ordering to be used e.g. with Seq.sort|sortBy
   */
  def getOrdering(partitions: Seq[String]): Ordering[PartitionValues] = new Ordering[PartitionValues] {
    def compare(pv1: PartitionValues, pv2: PartitionValues): Int = {
      partitions.map{
        p => (pv1(p), pv2(p)) match {
          case (v1: String, v2: String) => v1.compare(v2)
          case (v1: Byte, v2: Byte) => v1.compare(v2)
          case (v1: Short, v2: Short) => v1.compare(v2)
          case (v1: Int, v2: Int) => v1.compare(v2)
          case (v1: Long, v2: Long) => v1.compare(v2)
          case (v1: Char, v2: Char) => v1.compare(v2)
          case _ => 0 // if not an ordered type, we don't use it for sorting
        }
      }.find(_!=0).getOrElse(0)
    }
  }
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

  /**
   * Return PartitionValues keys which are not included in given partition columns
   */
  def checkWrongPartitionValues(partitionValues: Seq[PartitionValues], partitions: Seq[String]): Seq[String] = {
    if (partitionValues.nonEmpty) partitionValues.map(_.keys).reduce(_ ++ _).diff(partitions.toSet).toSeq
    else Seq()
  }

  /**
   * Checks if expected partition values are covered by existing partition values
   * challenge: handle multiple partition columns correctly and performant
   * @return list of missing partition values
   */
  def checkExpectedPartitionValues(existingPartitionValues: Seq[PartitionValues], expectedPartitionValues: Seq[PartitionValues]): Seq[PartitionValues] = {
    val partitionColCombinations = expectedPartitionValues.map(_.keys).distinct
    def diffPartitionValues(inputPartitions: Seq[PartitionValues], expectedPartitionValues: Seq[PartitionValues], partitionColCombinations: Seq[Set[String]]): Seq[PartitionValues] = {
      if (partitionColCombinations.isEmpty) return Seq()
      val partitionColCombination = partitionColCombinations.head
      val (partitionValuesCurrentCombination, partitionValuesOtherCombination) = expectedPartitionValues.partition(_.keys==partitionColCombination)
      val missingPartitionValuesCurrentCombination = partitionValuesCurrentCombination.diff(inputPartitions.map(_.filterKeys(partitionColCombination.toSeq)))
      missingPartitionValuesCurrentCombination ++ diffPartitionValues(inputPartitions, partitionValuesOtherCombination, partitionColCombinations.tail)
    }
    diffPartitionValues(existingPartitionValues, expectedPartitionValues, partitionColCombinations)
  }

  def oneToOneMapping(partitionValues: Seq[PartitionValues]): Map[PartitionValues,PartitionValues] = partitionValues.map(x => (x,x)).toMap
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
