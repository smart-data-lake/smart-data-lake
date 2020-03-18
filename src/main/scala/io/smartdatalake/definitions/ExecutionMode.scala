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
package io.smartdatalake.definitions

/**
 * Execution mode's defines how data is selected when running a data pipeline.
 */
sealed trait ExecutionMode {
  def mainInputId: Option[String]
  def mainOutputId: Option[String]
}

/**
 * Partition difference execution mode lists partitions on input & output DataObject and starts loading all missing partitions.
 * Partition columns to be used for comparision need to be a common 'init' of input and output partition columns.
 * @param partitionColNb optional number of partition columns to use as a common 'init'.
 * @param mainInputId optional selection of inputId to be used for partition comparision. Only needed if there are multiple input DataObject's.
 * @param mainOutputId optional selection of outputId to be used for partition comparision. Only needed if there are multiple output DataObject's.
 */
case class PartitionDiffMode(partitionColNb: Option[Int] = None, override val mainInputId: Option[String] = None, override val mainOutputId: Option[String] = None) extends ExecutionMode
