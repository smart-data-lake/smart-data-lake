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
package io.smartdatalake.util.webservice

import scala.util.Try

private[smartdatalake] trait WebserviceClient {

  /**
    * Sends a GET request to a webservice
    *
    * @return Success(with response as Array[Byte])
    *         or Failure([[WebserviceException]] with reason of failure)
    */
  def get(params: Map[String,String] = Map()): Try[Array[Byte]]

  def post(body: Array[Byte], mimeType: String, params: Map[String,String] = Map()): Try[Array[Byte]]

  def put(body: Array[Byte], mimeType: String, params: Map[String,String] = Map()): Try[Array[Byte]]

  def patch(body: Array[Byte], mimeType: String, params: Map[String,String] = Map()): Try[Array[Byte]]
}
