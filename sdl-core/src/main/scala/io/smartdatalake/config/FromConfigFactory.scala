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

import com.typesafe.config.Config

/**
 * A factory object that fulfils the contract for a static factory method that parses (case) classes from [[Config]]s.
 *
 * This trait is (usually) implemented by companion objects of SDL config objects (DataObjects, Actions, ...).
 *
 * @see [[ParsableFromConfig]]
 *
 * @tparam CO the type that is parsed by this factory.
 */
private[smartdatalake] trait FromConfigFactory[+CO <: SdlConfigObject with ParsableFromConfig[CO]] {

  /**
   * Factory method for parsing (case) classes from [[Config]]s.
   *
   * @return a new instance of type `CO` parsed from the a context dependent [[Config]].
   */
  def fromConfig(config: Config, instanceRegistry: InstanceRegistry): CO
}
