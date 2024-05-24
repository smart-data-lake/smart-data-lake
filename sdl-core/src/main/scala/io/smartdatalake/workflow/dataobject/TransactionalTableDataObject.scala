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
import io.smartdatalake.workflow.ActionPipelineContext

/**
 * Trait to mark DataObjects that support transactional writes.
 */
trait TransactionalTableDataObject extends TableDataObject with CanCreateSparkDataFrame with CanWriteSparkDataFrame {

  override def options: Map[String, String] = Map() // override options because of conflicting definitions in CanCreateSparkDataFrame and CanWriteSparkDataFrame

  val preReadSql: Option[String] = None
  val postReadSql: Option[String] = None
  val preWriteSql: Option[String] = None
  val postWriteSql: Option[String] = None

  override def preRead(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    super.preRead(partitionValues)
    prepareAndExecSql(preReadSql, Some("preReadSql"), partitionValues)
  }

  override def postRead(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    super.postRead(partitionValues)
    prepareAndExecSql(postReadSql, Some("postReadSql"), partitionValues)
  }

  override def preWrite(implicit context: ActionPipelineContext): Unit = {
    super.preWrite
    prepareAndExecSql(preWriteSql, Some("preWriteSql"), Seq()) // no partition values here...
  }

  override def postWrite(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    super.postWrite(partitionValues)
    prepareAndExecSql(postWriteSql, Some("postWriteSql"), partitionValues)
  }

  def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit

}
