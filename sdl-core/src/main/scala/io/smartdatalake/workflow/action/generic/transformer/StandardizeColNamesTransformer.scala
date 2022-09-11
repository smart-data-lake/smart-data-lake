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
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}


/**
 * Standardizes column names to be used without quoting by using camelCase to lower_case_with_underscore rule (default), and further cleanup rules for special characters (default).
 * Parameters below can be used to disable specific rules if needed.
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param camelCaseToLower If selected, converts Camel case names to lower case  with underscores, i.e. TestString -> test_string, testABCtest -> test_ABCtest
 *                         Otherwise converts just to lower case.
 * @param normalizeToAscii If selected, converts UTF-8 special characters (e.g. diacritics, umlauts) to ASCII chars (best effort), i.e. Öffi_émily -> Oeffi_emily
 * @param removeNonStandardSQLNameChars Remove all chars from a string which dont belong to lowercase SQL standard naming characters, i.e abc$!-& -> abc
 */
case class StandardizeColNamesTransformer(override val name: String = "colNamesLowercase", override val description: Option[String] = None, camelCaseToLower: Boolean=true, normalizeToAscii: Boolean=true, removeNonStandardSQLNameChars: Boolean=true) extends GenericDfTransformer {
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    df.standardizeColNames(camelCaseToLower, normalizeToAscii, removeNonStandardSQLNameChars)
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = StandardizeColNamesTransformer
}

object StandardizeColNamesTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): StandardizeColNamesTransformer = {
    extract[StandardizeColNamesTransformer](config)
  }
}