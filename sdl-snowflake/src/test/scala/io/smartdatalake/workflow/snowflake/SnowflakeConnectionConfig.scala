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

package io.smartdatalake.workflow.snowflake

import io.smartdatalake.definitions.BasicAuthMode
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.connection.SnowflakeConnection

/**
 * Configuration of Snowflake connection for integration tests
 * Please ensure that the environnement Variables SNOWFLAKE_URL, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_ROLE,
 * SNOWFLAKE_USER and SNOWFLAKE_PASSWORD are set in order to connect to Snowflake.
 */
object SnowflakeConnectionConfig {
  val sfConnection: SnowflakeConnection = SnowflakeConnection(
    id = "sfCon",
    url = sys.env("SNOWFLAKE_URL"),
    warehouse = sys.env("SNOWFLAKE_WAREHOUSE"),
    database = sys.env("SNOWFLAKE_DATABASE"),
    role = sys.env("SNOWFLAKE_ROLE"),
    authMode = BasicAuthMode(Some(StringOrSecret(sys.env("SNOWFLAKE_USER"))), Some(StringOrSecret(sys.env("SNOWFLAKE_PASSWORD"))))
  )
}

