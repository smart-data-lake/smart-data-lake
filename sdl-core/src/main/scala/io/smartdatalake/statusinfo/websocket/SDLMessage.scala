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

package io.smartdatalake.statusinfo.websocket

import io.smartdatalake.statusinfo.websocket.SDLMessageType.SDLMessageType

/**
 * Represents a message that can be sent between a DAG Execution of SmartDataLakeBuilder and a web frontend, such as Airflow
 * @param msgType The type of the message. For message type Log, log must be filled.
 *                For message type StatusUpdate, statusUpdate must be filled.
 * @param statusUpdate Status update sent to the web frontend
 * @param log Log message sent to the web frontend
 */
case class SDLMessage(msgType: SDLMessageType, statusUpdate: Option[StatusUpdate] = None, log: Option[ActionLog] = None)

