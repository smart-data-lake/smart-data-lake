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

/**
 * A class that can be parsed from a [[com.typesafe.config.Config]] by [[ConfigParser]].
 *
 * @tparam CO The type of this class.
 *
 * @see [[ParsableFromConfig]]
 */
private[smartdatalake] trait ParsableFromConfig[+CO <: ParsableFromConfig[CO]] {

  /**
   * Returns the factory that can parse this type (that is, type `CO`).
   *
   * Typically, implementations of this method should return the companion object of the implementing class.
   * The companion object in turn should implement [[FromConfigFactory]].
   *
   * @return  the factory (object) for this class.
   */
  def factory: FromConfigFactory[CO]
}

/**
 * A marker trait to exclude an SdlConfigObject from the schema export.
 */
trait ExcludeFromSchemaExport