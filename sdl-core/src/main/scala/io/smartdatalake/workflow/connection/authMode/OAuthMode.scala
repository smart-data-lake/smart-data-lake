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

package io.smartdatalake.workflow.connection.authMode

import io.smartdatalake.util.secrets.StringOrSecret

/**
 * [[OAuthMode]] contains the coordinates and credentials to gain access to the OData DataSource
 *
 * @param oauthUrl URL to the OAuth2 authorization instance like "https://login.microsoftonline.com/{tenant-guid}/oauth2/v2.0/token" (supports secret providers)
 * @param clientId Name of the user (supports secrets providers)
 * @param clientSecret Password of the user (supports secret providers)
 * @param oauthScope OAuth authorization scope (like https://xxx.crm4.dynamics.com/.default) (supports secret providers)
 */
case class OAuthMode (
                       oauthUrl: StringOrSecret,
                       clientId: StringOrSecret,
                       clientSecret: StringOrSecret,
                       oauthScope: StringOrSecret
                     )
