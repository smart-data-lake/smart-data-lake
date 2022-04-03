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

/**
 * Currently empty trait. Used in [[io.smartdatalake.workflow.action.HistorizeAction]]
 * and [[io.smartdatalake.workflow.action.DeduplicateAction]] which need any kind
 * of transactional util table
 */
private[smartdatalake] trait TransactionalSparkTableDataObject extends TableDataObject with CanCreateSparkDataFrame with CanWriteSparkDataFrame {

  override def options: Map[String, String] = Map() // override options because of conflicting definitions in CanCreateSparkDataFrame and CanWriteSparkDataFrame

}
