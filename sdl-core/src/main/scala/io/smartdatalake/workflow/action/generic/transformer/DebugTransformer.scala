/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action.generic.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import org.slf4j.event.Level

/**
 * Log schema, sample data and plan of DataFrame to transform.
 * This transformer can be used to log debug output between different transformers.
 * It's not intended for production use.
 *
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param printSchema  true if the schema of the DataFrame should be logged. Default is true.
 * @param printSchemaOptions SubType specific options to pass to the printSchema statement.
 *                           For Spark this is level=(int). Default is level=Int.MaxValue.
 * @param show         true if data should be logged using show method. Default is false.
 * @param showOptions  SubType specific options to pass to the show statement.
 *                     For Spark this can be numRows=(int), truncate=(int) and vertical=(true/false). Default is numRows=10, truncate=20, vertical=false.
 * @param explain      true if plan should be logged using explain method. Default is false.
 * @param explainOptions SubType specific options to pass to explain statement.
 *                     For Spark this can be mode=(simple/extended/codegen/cost/formatted), see [[ExplainMode]] for details. Default is mode=simple.
 *                     Default is to use the default of the underlying SubFeed type, e.g. 'simple' for Spark.
 * @param logLevel     log level to use for logging. Possible values are 'debug', 'info', 'warn', 'error'. Default is 'info'.
 * @param titleMarker  a string to prefix before and after the DataFrame for finding the debug output easier. Default is '###'.
 */
case class DebugTransformer(override val name: String = "debug", override val description: Option[String] = None
                           , printSchema: Boolean = true, printSchemaOptions: Map[String,String] = Map()
                           , show: Boolean = false, showOptions: Map[String,String] = Map()
                           , explain: Boolean = false, explainOptions: Map[String,String] = Map()
                           , logLevel: String = "info"
                           , titleMarker: String = "###"
                           ) extends GenericDfTransformer with SmartDataLakeLogger {
  private val logEvent = logger.atLevel(Level.valueOf(logLevel.toUpperCase))
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    def getLogMsg(logType: String, content: String) = {
      s"($actionId.$name) '$logType' debug output for $titleMarker $dataObjectId $titleMarker" + System.lineSeparator() + content
    }
    if (printSchema) {
      val level = printSchemaOptions.get("level").map(_.toInt).getOrElse(Int.MaxValue)
      logEvent.log(getLogMsg("schema", indent(df.schema.treeString(level),2)))
    }
    if (show) {
      logEvent.log(getLogMsg("show", indent(df.showString(showOptions),2)))
    }
    if (explain) {
      logEvent.log(getLogMsg("explain", indent(df.explainString(explainOptions),2)))
    }
    // return
    df
  }

  /**
   * This functionality is included in Java 17, e.g. string.indent(n). It's implemented here to be compatible with Java 8.
   */
  private def indent(s: String, n: Int) = {
    assert(n > 0)
    val prefix = " ".repeat(n)
    s.linesIterator.map(prefix + _).mkString(System.lineSeparator())
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = DebugTransformer
}

object DebugTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DebugTransformer = {
    extract[DebugTransformer](config)
  }
}

