/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake

import org.apache.spark.sql.types.StructType

// Define some helper types to easily differentiate between Spark and Snowpark classes in sdl-snowflake
object smartdatalake {
  type SnowparkDataFrame = com.snowflake.snowpark.DataFrame
  type SparkDataFrame = org.apache.spark.sql.DataFrame
  type SnowparkSession = com.snowflake.snowpark.Session
  type SnowparkStructType = com.snowflake.snowpark.types.StructType
  type SparkStructType = StructType
}
