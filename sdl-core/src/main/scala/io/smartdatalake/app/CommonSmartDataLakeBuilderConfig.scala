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

package io.smartdatalake.app

import io.smartdatalake.util.hdfs.PartitionValues

trait CommonSmartDataLakeBuilderConfig[SDLBConf] {
  def feedSel(value: String): SDLBConf

  def applicationName(value: Option[String]): SDLBConf

  def configuration(value: Option[Seq[String]]): SDLBConf

  //TODO remove master if possible
  def master(value: Option[String]): SDLBConf

  def partitionValues(value: Option[Seq[PartitionValues]]): SDLBConf

  def multiPartitionValues(value: Option[Seq[PartitionValues]]): SDLBConf

  def parallelism(value: Int): SDLBConf

  def statePath(value: Option[String]): SDLBConf

  def overrideJars(value: Option[Seq[String]]): SDLBConf

  def test(value: Option[TestMode.Value]): SDLBConf

  def streaming(value: Boolean): SDLBConf

  def agentPort(value: Option[Int]): SDLBConf

  def azureRelayURL(value: Option[String] = None): SDLBConf
}
