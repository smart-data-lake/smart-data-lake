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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.{ActionPipelineContext, SchemaViolationException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A trait to be implemented by DataObjects which store partitioned data
 */
@DeveloperApi
trait CanHandlePartitions { this: DataObject =>

  /**
   * Definition of partition columns
   */
  def partitions: Seq[String]

  /**
   * Definition of partitions that are expected to exists.
   * This is used to validate that partitions being read exists and don't return no data.
   * Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
   * example: "elements['yourColName'] > 2017"
   * @return true if partition is expected to exist.
   */
  private[smartdatalake] def expectedPartitionsCondition: Option[String]

  /**
   * Delete given partitions. This is used to cleanup partitions by housekeeping.
   * Note: this is optional to implement.
   */
  private[smartdatalake] def deletePartitions(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = throw new RuntimeException(s"deletePartitions not implemented")

  /**
   * Move given partitions. This is used to archive partitions by housekeeping.
   * Note: this is optional to implement.
   */
  private[smartdatalake] def movePartitions(partitionValues: Seq[(PartitionValues,PartitionValues)])(implicit session: SparkSession): Unit = throw new RuntimeException(s"movePartitions not implemented")

  /**
   * Compact given partitions combining smaller files into bigger ones. This is used to compact partitions by housekeeping.
   * Note: this is optional to implement.
   */
  private[smartdatalake] def compactPartitions(partitionValues: Seq[PartitionValues])(implicit session: SparkSession, actionPipelineContext: ActionPipelineContext): Unit = throw new RuntimeException(s"compactPartitions not implemented")

  /**
   * list partition values
   */
  def listPartitions(implicit session: SparkSession): Seq[PartitionValues]

  /**
   * create empty partition
   */
  private[smartdatalake] def createEmptyPartition(partitionValues: PartitionValues)(implicit session: SparkSession): Unit = throw new RuntimeException(s"createEmptyPartition not implemented")

  /**
   * Create empty partitions for partition values not yet existing
   */
  private[smartdatalake] final def createMissingPartitions(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = {
    val partitionValuesCols = partitionValues.map(_.keys).reduceOption(_ ++ _).getOrElse(Set()).toSeq
    partitionValues.diff(listPartitions.map(_.filterKeys(partitionValuesCols)))
      .foreach(createEmptyPartition)
  }

  /**
   * Filter list of partition values by expected partitions condition
   */
  private[smartdatalake] final def filterExpectedPartitionValues(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Seq[PartitionValues] = {
    import org.apache.spark.sql.functions.expr
    import session.implicits._
    expectedPartitionsCondition.map{ condition =>
      // partition values value type is any, we need to convert it to string and keep the hashCode for filtering afterwards
      val partitionsValuesStringWithHashCode = partitionValues.map( pv => (pv.elements.mapValues(_.toString), pv.hashCode))
      val expectedHashCodes = partitionsValuesStringWithHashCode.toDF("elements","hashCode").where(expr(condition)).select($"hashCode").as[Int].collect.toSet
      partitionValues.filter( pv => expectedHashCodes.contains(pv.hashCode))
    }.getOrElse(partitionValues)
  }

  /**
   * Validate the schema of a given Spark Data Frame `df` that it contains the specified partition columns
   *
   * @param df The data frame to validate.
   * @param role role used in exception message. Set to read or write.
   * @throws SchemaViolationException if the partitions columns are not included.
   */
  def validateSchemaHasPartitionCols(df: DataFrame, role: String): Unit = {
    val missingCols = partitions.diff(df.columns)
    if (missingCols.nonEmpty) throw new SchemaViolationException(s"($id) DataFrame is missing partition cols ${missingCols.mkString(", ")} on $role")
  }
}
