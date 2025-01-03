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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.testutils.DataObjectTestSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
 * Unit tests for [[AvroFileDataObject]].
 */
class AvroFileDataObjectTest extends DataObjectTestSuite with SparkFileDataObjectSchemaBehavior {
  testsFor(readNonExistingSources(createDataObject, fileExtension = ".avro"))
  testsFor(readEmptySourcesWithEmbeddedSchema(createDataObject, fileExtension = ".avro"))
  testsFor(validateSchemaMinOnWrite(createDataObjectWithchemaMin, fileExtension = ".avro"))
  testsFor(validateSchemaMinOnRead(createDataObjectWithchemaMin, fileExtension = ".avro"))

  def createDataObject(path: String, schemaOpt: Option[StructType]): AvroFileDataObject = {
    val dataObj = AvroFileDataObject(id = "schemaTestAvroDO", path = path, schema = schemaOpt.map(SparkSchema.apply))
    instanceRegistry.register(dataObj)
    dataObj
  }

  def createDataObjectWithchemaMin(path: String, schemaOpt: Option[StructType], schemaMinOpt: Option[StructType]): AvroFileDataObject = {
    val dataObj = AvroFileDataObject(id = "schemaTestAvroDO", path = path, schema = schemaOpt.map(SparkSchema.apply), schemaMin = schemaMinOpt.map(SparkSchema.apply))
    instanceRegistry.register(dataObj)
    dataObj
  }

  override def createFile(path: String, data: DataFrame): Unit = {
    data.write.format("com.databricks.spark.avro").save(path)
  }
}
