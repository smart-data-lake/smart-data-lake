/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

import com.snowflake.snowpark.types.{DataType, StructType}
import io.smartdatalake.smartdatalake.SnowparkDataFrame
import io.smartdatalake.workflow.ActionPipelineContext

private[smartdatalake] trait CanCreateSnowparkDataFrame {

  def getSnowparkDataFrame()(implicit context: ActionPipelineContext) : SnowparkDataFrame

  def createSnowparkReadSchema(writeSchema: StructType)(implicit context: ActionPipelineContext): StructType = writeSchema
}
