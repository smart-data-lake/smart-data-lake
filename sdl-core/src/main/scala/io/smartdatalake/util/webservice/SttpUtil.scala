/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

import sttp.client3.Response

object SttpUtil {

  def getContent(response: Response[Either[String, String]], context: String): String = {
    validateResponse(response, context)
    response.body.right.get
  }

  def validateResponse(response: Response[Either[String, String]], context: String): Unit = {
    if (response.body.isLeft) {
      throw HttpRequestError(context, response.code.code, response.body.left.get)
    }
    assert(response.isSuccess, throw HttpRequestError(context, response.code.code, "StatusCode is not successfull, but there is no error message!"))
  }
}


case class HttpRequestError(context: String, code: Int, err: String) extends Exception(s"'$context' failed: StatusCode=$code Error=$err")