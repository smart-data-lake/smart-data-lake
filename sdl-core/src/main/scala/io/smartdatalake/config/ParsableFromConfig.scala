/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
 * Implementations must provide a companion object implementing [[FromConfigFactory]]. [[ConfigParser]] resolves
 * it by reflection, there is no member pointing to it. `FactoryMethodCompletenessTest` in sdl-lang asserts that
 * every implementation has such a companion object, except those marked with [[ExcludeFromSchemaExport]].
 *
 * @tparam CO The type of this class.
 *
 * @see [[FromConfigFactory]]
 */
private[smartdatalake] trait ParsableFromConfig[+CO <: ParsableFromConfig[CO]]

/**
 * A marker trait to exclude an SdlConfigObject from the schema export.
 */
trait ExcludeFromSchemaExport