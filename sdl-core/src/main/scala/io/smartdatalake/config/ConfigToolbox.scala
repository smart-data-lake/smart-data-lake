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

package io.smartdatalake.config

import io.smartdatalake.app.GlobalConfig
import org.apache.hadoop.conf.Configuration

/**
 * Helper methods to use config outside of SmartDataLakeBuilder, e.g. Notebooks
 */
object ConfigToolbox {

  /**
   * Load and parse config objects
   *
   * @param locations list of config locations
   * @return instanceRegistry with parsed config objects and parsed global config
   */
  def loadAndParseConfig(locations: Seq[String]): (InstanceRegistry, GlobalConfig) = {
    // load config
    val defaultHadoopConf: Configuration = new Configuration()
    val config = ConfigLoader.loadConfigFromFilesystem(locations, defaultHadoopConf)
    val globalConfig = GlobalConfig.from(config)
    val registry = ConfigParser.parse(config)
    (registry, globalConfig)
  }
}
