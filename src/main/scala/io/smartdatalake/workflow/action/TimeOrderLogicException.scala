/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.action

/**
 * Exception to signal that a timestamp for the import is too old.
 * Usually encountered when using Historize or Deduplication.
 *
 * @param message Message of Exception
 */
private[smartdatalake] class TimeOrderLogicException(message: String) extends RuntimeException(message) {}
