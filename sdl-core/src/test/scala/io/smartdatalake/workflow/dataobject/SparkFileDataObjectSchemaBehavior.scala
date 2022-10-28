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

import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.{ActionPipelineContext, SchemaViolationException}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.scalatest.{FunSuite, Matchers}

import java.io.File
import java.nio.file.Files

trait SparkFileDataObjectSchemaBehavior { this: FunSuite with Matchers =>

  def readNonExistingSources(createDataObject: (String, Option[StructType]) => DataObject with CanCreateDataFrame with CanCreateSparkDataFrame with UserDefinedSchema,
                                     fileExtension: String = null)
                                         (implicit context: ActionPipelineContext): Unit = {

    test("It is not possible to read from an non-existing file without user-defined-schema.") {
      val path = tempFilePath(fileExtension)
      val dataObj = createDataObject(path, None)
      an [IllegalArgumentException] should be thrownBy dataObj.getSparkDataFrame()
    }
  }

  def readEmptySources(createDataObject: (String, Option[StructType]) => DataObject with CanCreateDataFrame with CanCreateSparkDataFrame with UserDefinedSchema,
                       fileExtension: String = null)
                      (implicit context: ActionPipelineContext): Unit = {

    test("Reading from an empty file with user-defined schema results in an empty data frame.") {
      val schema = Seq(
          StructField("header1", StringType, nullable = true),
          StructField("header2", IntegerType, nullable = true)
      )

      val path = tempFilePath(fileExtension)
      val session = context.sparkSession
      import session.implicits._
      createFile(path, Seq.empty[String].toDF())
      try {
        val dataObj = createDataObject(path, Some(StructType(schema)))
        val df = dataObj.getSparkDataFrame()

        df.schema should contain theSameElementsInOrderAs schema
        df shouldBe empty
      } finally {
        FileUtils.forceDelete(new File(path))
      }
    }
  }

  def readEmptySourcesWithEmbeddedSchema(createDataObject: (String, Option[StructType]) => DataObject with CanCreateDataFrame with CanCreateSparkDataFrame with UserDefinedSchema,
                       fileExtension: String = null)
                      (implicit context: ActionPipelineContext): Unit = {

    test("Reading an empty file creates an empty data frame with the user-defined schema.") {
      val embeddedSchema = Seq(
        StructField("_c1", StringType, nullable = true)
      )
      val userSchema = Seq(
        StructField("header1", StringType, nullable = true),
        StructField("header2", IntegerType, nullable = true)
      )

      val path = tempFilePath(fileExtension)
      implicit val session: SparkSession = context.sparkSession
      createFile(path, DataFrameUtil.getEmptyDataFrame(StructType(embeddedSchema)))
      try {
        val dataObj = createDataObject(path, Some(StructType(userSchema)))
        val df = dataObj.getSparkDataFrame()

        df.schema should contain theSameElementsInOrderAs userSchema
        df shouldBe empty
      } finally {
        FileUtils.forceDelete(new File(path))
      }
    }

    test("Reading an empty file without user-defined schema creates an empty data frame with the embedded schema.") {
      val embeddedSchema = Seq(
        StructField("_c1", StringType, nullable = true)
      )

      val path = tempFilePath(fileExtension)
      implicit val session: SparkSession = context.sparkSession
      createFile(path, DataFrameUtil.getEmptyDataFrame(StructType(embeddedSchema)))
      try {
        val dataObj = createDataObject(path, None)
        val df = dataObj.getSparkDataFrame()

        df.schema should contain theSameElementsInOrderAs embeddedSchema
        df shouldBe empty
      } finally {
        FileUtils.forceDelete(new File(path))
      }
    }
  }

  def validateSchemaMinOnWrite(createDataObject: (String, Option[StructType], Option[StructType]) => DataObject with CanWriteSparkDataFrame,
                               fileExtension: String = null)
                              (implicit context: ActionPipelineContext) : Unit = {

    implicit val session: SparkSession = context.sparkSession
    import session.implicits._

    val schemaMin = Seq(
      StructField("id", StringType, nullable = true),
      StructField("value", IntegerType, nullable = true)
    )

    test("Write - SchemaMin full match is valid.") {
      val path = tempFilePath(fileExtension)
      try {
        val df = Seq(
          ("a", 1),
          ("b", 2)
        ).toDF("id", "value")
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        noException should be thrownBy {
          dataObj.writeSparkDataFrame(df, partitionValues = Seq.empty)
        }

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }

    test("Write - SchemaMin subset is valid.") {
      val path = tempFilePath(fileExtension)
      try {
        val df = Seq(
          ("a", 1, 2.3f),
          ("b", 2, 2.4f)
        ).toDF("id", "value", "foo")
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        noException should be thrownBy {
          dataObj.writeSparkDataFrame(df, partitionValues = Seq.empty)
        }

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }

    test("Write - SchemaMin violation: invalid column name.") {
      val path = tempFilePath(fileExtension)
      try {
        val df = Seq(
          ("a", 1),
          ("b", 2)
        ).toDF("foo", "value")
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        val thrown = the [SchemaViolationException] thrownBy {
          dataObj.writeSparkDataFrame(df, partitionValues = Seq.empty)
        }
        println(thrown.getMessage)

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }

    test("Write - SchemaMin violation: invalid data type.") {
      val path = tempFilePath(fileExtension)
      try {
        val df = Seq(
          (1, 1),
          (2, 2)
        ).toDF("id", "value")
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        val thrown = the [SchemaViolationException] thrownBy {
          dataObj.writeSparkDataFrame(df, partitionValues = Seq.empty)
        }
        println(thrown.getMessage)

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }

    test("Write - SchemaMin violation: missing columns.") {
      val path = tempFilePath(fileExtension)
      try {
        val df = Seq(
          Tuple1("a"),
          Tuple1("b")
        ).toDF("id")
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        val thrown = the [SchemaViolationException] thrownBy {
          dataObj.writeSparkDataFrame(df, partitionValues = Seq.empty)
        }
        println(thrown.getMessage)

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }

    test("Write - SchemaMin violation: missing columns (empty).") {
      val path = tempFilePath(fileExtension)
      try {
        val df = session.emptyDataFrame
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        val thrown = the [SchemaViolationException] thrownBy {
          try {
            dataObj.writeSparkDataFrame(df, partitionValues = Seq.empty)
          } catch {
            case _: AnalysisException => succeed //data frame writer does not support empty schemata
          }
        }
        println(thrown.getMessage)

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }
  }

  def validateSchemaMinOnRead(createDataObject: (String, Option[StructType], Option[StructType]) => DataObject with CanCreateDataFrame with CanCreateSparkDataFrame,
                               fileExtension: String = null)
                              (implicit context: ActionPipelineContext) : Unit = {

    implicit val session: SparkSession = context.sparkSession
    import session.implicits._

    val schemaMin = Seq(
      StructField("id", StringType, nullable = true),
      StructField("value", IntegerType, nullable = true)
    )

    test("Read - SchemaMin full match is valid.") {
      val path = tempFilePath(fileExtension)
      try {
        val df = Seq(
          ("a", 1),
          ("b", 2)
        ).toDF("id", "value")
        createFile(path, data = df)
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        noException should be thrownBy {
          dataObj.getSparkDataFrame()
        }

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }

    test("Read - SchemaMin subset is valid.") {
      val path = tempFilePath(fileExtension)
      try {
        val df = Seq(
          ("a", 1, 2.3f),
          ("b", 2, 2.4f)
        ).toDF("id", "value", "foo")
        createFile(path, data = df)
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        noException should be thrownBy {
          dataObj.getSparkDataFrame()
        }

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }

    test("Read - SchemaMin violation: invalid column name.") {
      val path = tempFilePath(fileExtension)
      try {
        val df = Seq(
          ("a", 1),
          ("b", 2)
        ).toDF("foo", "value")
        createFile(path, data = df)
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        val thrown = the [SchemaViolationException] thrownBy {
          dataObj.getSparkDataFrame()
        }
        println(thrown.getMessage)

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }

    test("Read - SchemaMin violation: invalid data type.") {
      val path = tempFilePath(fileExtension)
      try {
        val df = Seq(
          (1, 1),
          (2, 2)
        ).toDF("id", "value")
        createFile(path, data = df)
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        val thrown = the [SchemaViolationException] thrownBy {
          dataObj.getSparkDataFrame()
        }
        println(thrown.getMessage)

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }

    test("Read - SchemaMin violation: missing columns.") {
      val path = tempFilePath(fileExtension)
      try {
        val df = Seq(
          Tuple1("a"),
          Tuple1("b")
        ).toDF("id")
        createFile(path, data = df)
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        val thrown = the [SchemaViolationException] thrownBy {
          dataObj.getSparkDataFrame()
        }
        println(thrown.getMessage)

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }

    test("Read - SchemaMin violation: missing columns (empty).") {
      val path = tempFilePath(fileExtension)
      try {
        val df = session.emptyDataFrame
        try {
          createFile(path, data = df)
        } catch {
          case _: AnalysisException => succeed //data frame writer does not support empty schemata
        }
        val dataObj = createDataObject(path, Some(df.schema), Some(StructType(schemaMin)))

        val thrown = the [SchemaViolationException] thrownBy {
          dataObj.getSparkDataFrame()
        }
        println(thrown.getMessage)

      } finally {
        val f = new File(path)
        if(f.exists())
          FileUtils.forceDelete(f)
      }
    }
  }

  def createFile(path: String, data: DataFrame = null): Unit

  private def tempFilePath(suffix: String): String = {
    val tempDir = Files.createTempDirectory("csv")
    val tempFile = Files.createTempFile(tempDir, "temp", suffix).toFile
    FileUtils.forceDelete(tempFile) // we just want the path, but no file created
    tempFile.getPath
  }
}
