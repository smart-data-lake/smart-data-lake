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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

class XmlFileDataObjectTest extends DataObjectTestSuite with SparkFileDataObjectSchemaBehavior {

  testsFor(readNonExistingSources(createDataObject, ".xml"))
  testsFor(readEmptySources(createDataObject, ".xml"))
  testsFor(validateSchemaMinOnWrite(createDataObjectWithSchemaMin, ".xml"))

  private def createDataObject(path: String, schemaOpt: Option[StructType]): XmlFileDataObject = {
    val dataObj = XmlFileDataObject(id = "schemaTestXmlDO", path = path, schema = schemaOpt)
    instanceRegistry.register(dataObj)
    dataObj
  }

  private def createDataObjectWithSchemaMin(path: String, schemaOpt: Option[StructType], schemaMinOpt: Option[StructType]): XmlFileDataObject = {
    val dataObj = XmlFileDataObject(id = "schemaTestXmlDO", path = path, schema = schemaOpt, schemaMin = schemaMinOpt)
    instanceRegistry.register(dataObj)
    dataObj
  }

  override def createFile(path: String, data: DataFrame): Unit = {
    data.write.format("com.databricks.spark.xml").save(path)
  }
}
