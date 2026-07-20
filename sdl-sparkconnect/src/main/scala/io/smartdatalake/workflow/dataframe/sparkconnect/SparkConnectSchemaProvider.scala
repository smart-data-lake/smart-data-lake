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
package io.smartdatalake.workflow.dataframe.sparkconnect

import io.smartdatalake.config.ConfigUtil
import io.smartdatalake.util.misc.FileUtil.readFromPath
import io.smartdatalake.util.misc.{SchemaProvider, SchemaProviderType}
import io.smartdatalake.workflow.dataframe.GenericSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType

/**
 * [[SchemaProvider]] implementation for Spark Connect.
 * Discovered on the classpath by [[io.smartdatalake.definitions.Environment.schemaProviders]].
 *
 * The Spark Connect client does not have the classic Spark (catalyst), spark-avro, spark-xml and JSON schema
 * converter libraries on its classpath. Therefore only the schema provider types that can be parsed with the Spark
 * Connect client alone are supported:
 * - [[SchemaProviderType.DDL]]     - parsed with [[StructType.fromDDL]]
 * - [[SchemaProviderType.DDLFile]] - file content read and parsed with [[StructType.fromDDL]]
 *
 * Other schema provider types (caseclass, javabean, xsdfile, jsonschemafile, avroschemafile, openapi) are not
 * supported here; for those [[io.smartdatalake.util.misc.SchemaUtil.readSchemaFromConfigValue]] falls back to a
 * [[io.smartdatalake.workflow.dataframe.LazyGenericSchema]].
 */
object SparkConnectSchemaProvider extends SchemaProvider {

  import io.smartdatalake.util.misc.SchemaProviderType._

  private val supportedTypes: Set[SchemaProviderType.Value] = Set(DDL, DDLFile)

  override def supports(schemaConfig: String): Boolean =
    SchemaProviderType.parse(schemaConfig).exists(supportedTypes.contains)

  override def readSchemaFromConfigValue(schemaConfig: String, lazyFileReading: Boolean): GenericSchema = {
    implicit lazy val defaultHadoopConf: Configuration = new Configuration()
    val (_, value) = ConfigUtil.parseProviderConfigValue(schemaConfig, Some(DDL.toString))
    SchemaProviderType.parse(schemaConfig) match {
      case Some(DDL) => SparkConnectSchema(StructType.fromDDL(value))
      case Some(DDLFile) =>
        require(value.split(";").length == 1,
          s"readSchemaFromConfigValue: DDL schema provider configuration error. Configuration format is '<path-to-ddl-file>', but received $value.")
        val content = readFromPath(new Path(value))
        SparkConnectSchema(StructType.fromDDL(content))
      case other => throw new IllegalStateException(s"SparkConnectSchemaProvider does not support schema provider type $other")
    }
  }
}
