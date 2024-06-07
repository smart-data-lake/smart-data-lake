/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe._


/**
 * Utils to transform DataFrames with nested columns / complex types.
 */
object NestedColumnUtil extends SmartDataLakeLogger {

  def selectSchema(df: GenericDataFrame, tgtSchema: GenericSchema): GenericDataFrame = {
    if (df.schema.fields.map(f => (f.name, f.dataType.sql)) != tgtSchema.fields.map(f => (f.name, f.dataType.sql))) {
      val columns = tgtSchema.fields.map { f =>
        val currentFieldTypes = df.schema.fields.map(f => (f.name, f.dataType)).toMap
        if (logger.isDebugEnabled) logger.debug(s"selecting column ${f.name}: currentDataType=${currentFieldTypes(f.name)} tgtDataType=${f.dataType}")
        selectColumn(df(f.name), currentFieldTypes(f.name), f.dataType).as(f.name)
      }
      df.select(columns)
    } else df
  }

  private[smartdatalake] def selectColumn(column: GenericColumn, currentDataType: GenericDataType, tgtDataType: GenericDataType): GenericColumn = {
    val functions = DataFrameSubFeed.getFunctions(tgtDataType.subFeedType)
    (currentDataType,tgtDataType) match {
      case (ct: GenericStructDataType, tt: GenericStructDataType) =>
        if (ct.fields.map(f => (f.name, f.dataType.sql)) != tt.fields.map(f => (f.name, f.dataType.sql))) {
          val currentFieldTypes = ct.fields.map(f => (f.name, f.dataType)).toMap
          val columns = tt.fields.map { f =>
            if (logger.isDebugEnabled) logger.debug(s"selecting column ${f.name}: currentDataType=${currentFieldTypes(f.name)} tgtDataType=${f.dataType}")
            selectColumn(column(f.name), currentFieldTypes(f.name), f.dataType).as(f.name)
          }
          functions.struct(columns: _*)
        } else column
      case (ct: GenericArrayDataType, tt: GenericArrayDataType) =>
        functions.transform(column, c => selectColumn(c, ct.elementDataType, tt.elementDataType))
      case (ct: GenericMapDataType, tt: GenericMapDataType) =>
        val keyTransformed = functions.transform_keys(column, (k, v) => selectColumn(k, ct.valueDataType, tt.keyDataType))
        val mapTransformed = functions.transform_values(keyTransformed, (k, v) => selectColumn(v, ct.valueDataType, tt.valueDataType))
        mapTransformed
      case (ct, tt) if ct.sql != tt.sql => column.cast(tt)
      case _ => column
    }
  }

  /**
   * Transform a DataFrame with nested Columns, by defining a visitor function.
   * The visitor function is called for every (nested) attribute/column.
   * It receives the DataType, Column and Attribute Path (Seq[String]) as input parameters and should return a TransformColumnCmd.
   * Possible TransformColumnCmd's are KeepColumn, TransformColumn, RemoveColumn.
   */
  def transformColumns(df: GenericDataFrame, visitorFunc: (GenericDataType, GenericColumn, Seq[String]) => TransformColumnCmd): GenericDataFrame = {
    val transforms = df.schema.fields.map { f =>
      logger.debug(s"transforming column ${f.name}: ${f.dataType}")
      transformColumn(f.dataType, df(f.name), Seq(f.name), visitorFunc)
    }
    if (transforms.exists(_.changeRequested)) {
      val selCols = df.schema.fields.zip(transforms).flatMap {
        case (f, KeepColumn) => Some(df(f.name))
        case (f, t: TransformColumn) => Some(t.newColumn.as(f.name))
        case (_, RemoveColumn) => None
      }
      df.select(selCols)
    }
    else df
  }

  private[smartdatalake] def transformColumn(dataType: GenericDataType, column: GenericColumn, path: Seq[String], visitorFunc: (GenericDataType, GenericColumn, Seq[String]) => TransformColumnCmd): TransformColumnCmd = {
    val functions = DataFrameSubFeed.getFunctions(dataType.subFeedType) // dataType.subFeedType
    // apply column transformation
    visitorFunc(dataType, column, path) match {
      // if column is not transformed, handle transform for complex types (struct, array, map)
      case KeepColumn =>
        dataType match {
          case t: GenericStructDataType =>
            val transforms = t.fields.map { f =>
              logger.debug(s"transforming column ${f.name}: ${f.dataType}")
              transformColumn(f.dataType, column(f.name), path :+ f.name, visitorFunc)
            }
            if (transforms.exists(_.changeRequested)) {
              val selFields = t.fields.zip(transforms).flatMap {
                case (f, KeepColumn) => Some(column(f.name))
                case (f, t: TransformColumn) => Some(t.newColumn.as(f.name))
                case (_, RemoveColumn) => None
              }
              TransformColumn(functions.struct(selFields: _*))
            }
            else KeepColumn
          case t: GenericArrayDataType =>
            val arrayTransformed = functions.transform(column, c => transformColumn(t.elementDataType, c, path, visitorFunc).getOrElse(c))
            if (column != arrayTransformed) TransformColumn(arrayTransformed) else KeepColumn
          case t: GenericMapDataType =>
            val keyTransformed = functions.transform_keys(column, (k, v) => transformColumn(t.keyDataType, k, path :+ "key", visitorFunc).getOrElse(k))
            val mapTransformed = functions.transform_values(keyTransformed, (k, v) => transformColumn(t.valueDataType, v, path :+ "value", visitorFunc).getOrElse(v))
            if (column != mapTransformed) TransformColumn(mapTransformed) else KeepColumn
          case _ => KeepColumn
        }
      case x => x
    }
  }
}


/**
 * A trait to define column transformations for NestedColumnUtil.transformColumns.
 */
sealed trait TransformColumnCmd {
  def changeRequested = true
  def getOrElse(originalColumn: GenericColumn) = originalColumn
}
case class TransformColumn(newColumn: GenericColumn) extends TransformColumnCmd {
  override def getOrElse(originalColumn: GenericColumn): GenericColumn = newColumn
}
case object RemoveColumn extends TransformColumnCmd {
  override def getOrElse(originalColumn: GenericColumn): GenericColumn = throw new IllegalStateException("RemoveColumn should not happen here")
}
case object KeepColumn extends TransformColumnCmd {
  override val changeRequested = false
}