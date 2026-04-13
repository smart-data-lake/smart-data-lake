/*
 * sdl-core - Build your data lake the smart way.
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
package io.smartdatalake.workflow.connection.authMode

import io.smartdatalake.util.secrets.StringOrSecret
import org.scalatest.funsuite.AnyFunSuite

class OAuthModeIT extends AnyFunSuite {

  // Check request to open OAuth2 server: https://oauth.tools/collection/1599045253169-GHF
  test("sample client_credentials flow") {
    val authMode = OAuthMode(
      oauthUrl = StringOrSecret("https://login-demo.curity.io/oauth/v2/oauth-token"),
      clientId = StringOrSecret("demo-backend-client"),
      clientSecret = StringOrSecret("MJlO3binatD9jk1"),
      oauthScope = StringOrSecret("read")
    )
    val authHeader = authMode.getHeaders
    assert(authHeader.nonEmpty)
    println(s"Headers: $authHeader")
  }

}
