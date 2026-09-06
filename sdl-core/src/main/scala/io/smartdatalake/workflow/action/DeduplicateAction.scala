/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.action

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, ConnectionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Condition
import io.smartdatalake.workflow.action.executionMode.ExecutionMode
import io.smartdatalake.workflow.action.generic.transformer.GenericDfTransformer
import io.smartdatalake.workflow.dataframe.GenericDataFrame

import java.sql.Timestamp

/**
 * Former name of [[UpsertAction]], kept for backward compatibility of existing configurations.
 * It behaves exactly like [[UpsertAction]], see there for a description and the available parameters.
 *
 * The name was misleading: this Action does not deduplicate its input data, it keeps the latest version of every
 * record identified by the primary key of the output table (Slowly Changing Dimension Type 1). Use a
 * DeduplicateTransformer if you need to make the input data unique.
 *
 * @deprecated Renamed to [[UpsertAction]] in version 3.0.0. Change `type = DeduplicateAction` to
 *             `type = UpsertAction` in your configuration, all parameters stay the same.
 */
@Deprecated
@deprecated("Use UpsertAction instead", "3.0.0")
case class DeduplicateAction(
                              override val id: ActionId,
                              inputId: DataObjectId,
                              outputId: DataObjectId,
                              transformers: Seq[GenericDfTransformer] = Seq(),
                              ignoreOldDeletedColumns: Boolean = false,
                              ignoreOldDeletedNestedColumns: Boolean = true,
                              updateCapturedColumnOnlyWhenChanged: Boolean = false,
                              sourceTimestampColumn: Option[String] = None,
                              mergeModeAdditionalJoinPredicate: Option[String] = None,
                              override val cacheOutput: Boolean = false,
                              override val cacheInput: Boolean = false,
                              override val executionMode: Option[ExecutionMode] = None,
                              override val executionCondition: Option[Condition] = None,
                              override val metricsFailCondition: Option[String] = None,
                              override val metadata: Option[ActionMetadata] = None,
                              override val engineConnectionId: Option[ConnectionId] = None
)(implicit val instanceRegistry: InstanceRegistry) extends UpsertActionImpl {

  checkPreconditions()
  validateConfig()

  logger.warn(s"($id) DeduplicateAction is deprecated, use UpsertAction instead. All parameters stay the same.")
}

object DeduplicateAction extends FromConfigFactory[Action] {

  @scala.annotation.nowarn("cat=deprecation")
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DeduplicateAction =
    extract[DeduplicateAction](config)

  /**
   * @deprecated Use [[UpsertAction.deduplicateDataFrame]] instead.
   */
  @deprecated("Use UpsertAction.deduplicateDataFrame instead", "3.0.0")
  def deduplicateDataFrame(
      existingDf: Option[GenericDataFrame],
      pks: Seq[String],
      refTimestamp: Timestamp,
      ignoreOldDeletedColumns: Boolean,
      ignoreOldDeletedNestedColumns: Boolean,
      sourceTimestampColumn: Option[String] = None
  )(df: GenericDataFrame): GenericDataFrame =
    UpsertAction.deduplicateDataFrame(existingDf, pks, refTimestamp, ignoreOldDeletedColumns, ignoreOldDeletedNestedColumns, sourceTimestampColumn)(df)

  /**
   * @deprecated Use [[UpsertAction.deduplicate]] instead.
   */
  @deprecated("Use UpsertAction.deduplicate instead", "3.0.0")
  def deduplicate(baseDf: GenericDataFrame, newDf: GenericDataFrame, keyColumns: Seq[String]): GenericDataFrame =
    UpsertAction.deduplicate(baseDf, newDf, keyColumns)

  /**
   * @deprecated Use [[UpsertAction.enhanceDataFrame]] instead.
   */
  @deprecated("Use UpsertAction.enhanceDataFrame instead", "3.0.0")
  def enhanceDataFrame(df: GenericDataFrame, refTimestamp: Timestamp, sourceTimestampColumn: Option[String] = None): GenericDataFrame =
    UpsertAction.enhanceDataFrame(df, refTimestamp, sourceTimestampColumn)
}
