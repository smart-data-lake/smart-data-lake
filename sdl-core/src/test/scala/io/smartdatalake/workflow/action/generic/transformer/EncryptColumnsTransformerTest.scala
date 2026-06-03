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
package io.smartdatalake.workflow.action.generic.transformer

import com.typesafe.config.{Config, ConfigFactory}
import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, GlobalConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.SdlConfigObject.stringToDataObjectId
import io.smartdatalake.config.{ConfigParser, ConfigurationException, InstanceRegistry}
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.crypt.{EncryptDecrypt, EncryptDecryptECB}
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.spark.dataset.Quality
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.SDLExecutionId
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

case class Test_Record(
    id: Integer,
    str: String,
    fl: Float,
    db: Double,
    lo: Long
)

class EncryptColumnsTransformerTest extends AnyFunSuite with Quality {
  private implicit val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val session: SparkSession = TestUtil.session
  import session.implicits._

  val statePath = "target/stateTest/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))
  val test_key = "A%D*G-KaPdSgVkYp"

  def run_test(enc_type: String): sql.DataFrame = {
    val sdlb = DefaultSmartDataLakeBuilder

    val config: Config = ConfigFactory.parseString(
      s"""
        |actions = {
        |   actenc = {
        |     type = CopyAction
        |     inputId = src
        |     outputId = enc
        |     metadata {
        |       feed = test_run
        |     }
        |     transformers = [{
        |       type = EncryptColumnsTransformer
        |       encryptColumns = ["c2","c3"]
        |       key = "$test_key"
        |       algorithm = $enc_type
        |     }]
        |   }
        |   actdec = {
        |     type = CopyAction
        |     inputId = enc
        |     outputId = dec
        |     metadata {
        |       feed = test_run
        |     }
        |     transformers = [{
        |       type = DecryptColumnsTransformer
        |       decryptColumns = ["c2","c3"]
        |       key = "$test_key"
        |       algorithm = $enc_type
        |     }]
        |   }
        |}
        |dataObjects {
        |  src {
        |    #id = ~{id}
        |    type = CsvFileDataObject
        |    path = "target/raw"
        |  }
        |  enc {
        |    type = ParquetFileDataObject
        |    path = "target/column_encrypted"
        |    schema = "c1 STRING, c2 STRING, c3 STRING"
        |  }
        |  dec {
        |    type = ParquetFileDataObject
        |    path = "target/decrypted"
        |  }
        |}
        |""".stripMargin
    ).resolve

    val globalConfig = GlobalConfig.from(config)
    implicit val instanceRegistry: InstanceRegistry = ConfigParser.parse(config)
    instanceRegistry.register(TestUtil.defaultSparkConnection)

    implicit val actionPipelineContext: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    val sdlConfig = SmartDataLakeBuilderConfig(configuration = Seq("cp:/application.conf"), feedSel = s"ids:actenc,ids:actdec")

    val srcDO = instanceRegistry.get[CsvFileDataObject]("src")
    val dfSrc = Seq(("testData", "Foo", "ice"), ("bar", "Space", "water"), ("gogo", "Space", "water")).toDF("c1", "c2", "c3")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))
    dfSrc.createdLog("dfSrc")

    val initialSubFeeds: Seq[SparkSubFeed] = Seq(SparkSubFeed(None, srcDO.id, Seq()))
    val (_, _) = sdlb.exec(
      sdlConfig,
      SDLExecutionId.executionId1,
      runStartTime = LocalDateTime.now,
      attemptStartTime = LocalDateTime.now,
      actionsToSkip = Map(),
      initialSubFeeds = initialSubFeeds,
      dataObjectsState = Seq(),
      stateStore = None,
      stateListeners = Seq(),
      simulation = false,
      globalConfig = globalConfig
    )

    logger.debug(s"run_test($enc_type): check result. first check the encoded dataFrame.")
    val enc: ParquetFileDataObject = instanceRegistry.get[ParquetFileDataObject]("enc")
    val dfEnc = Try(enc.getSparkDataFrame()) match {
      case Success(df) => df
      case Failure(e)  =>
        logger.error(s"run_test($enc_type): enc.getSparkDataFrame() failed !")
        logger.error(s"enc: $enc")
        throw e
    }
    dfEnc.createdLog("dfEnc")
    val colName = dfEnc.columns
    assert(colName.toSeq == Seq("c1", "c2", "c3"))
    val testCols: List[String] = dfEnc.select("c2").map(f => f.getString(0)).collect().toList
    dfEnc.show(false)
    logger.info(s"run_test: $enc_type encrypted dataFrame: ${testCols.length} testCols = ${testCols.mkString(",")}")
    assert(testCols != Seq("Foo", "Space", "Space"))
    if (enc_type === "GCM") {
      assert(testCols.length > 2,
        s"testCols must be longer than 2, but only ${testCols.length} testCols given: ${testCols.mkString(",")}")
      assert(testCols(1) !== testCols(2),
        "2 encrypted items should not result in the same ciphertext with GCM")
    } else if (enc_type === "ECB") {
      assert(testCols(1) === testCols(2), "2 encrypted items should result in the same ciphertext with ECB")
    }

    logger.debug(s"run_test($enc_type): check the decoded DataFrame")
    val dec = instanceRegistry.get[ParquetFileDataObject]("dec")
    val dfDec = dec.getSparkDataFrame()
    dfDec.show(false)
    logger.info(s"run_test: $enc_type decrypted dataFrame")

    val colDecName = dfDec.columns
    assert(colDecName.toSeq == Seq("c1", "c2", "c3"))
    val testDecCol = dfDec.select("c2").map(f => f.getString(0)).collect().toList
    assert(testDecCol == Seq("Foo", "Space", "Space"))
    dfEnc
  }

  test("test GCM column encryption and decryption") {
    run_test("GCM")
  }

  test("test ECB column encryption and decryption") {
    val df = run_test("ECB")
    assert(df.select("c2").take(2)(1).getAs[String]("c2") === "0RK5Cr5ax1OXlBO7Q+BHxA==")
  }

  test("test column encryption, unsupported algorithm") {
    intercept[ConfigurationException] {
      run_test("notSupported")
    }
  }

  test("test column encryption and decryption with Class Name") {
    val df = run_test("io.smartdatalake.util.crypt.EncryptDecryptECB")
    assert(df.select("c2").take(2)(1).getAs[String]("c2") === "0RK5Cr5ax1OXlBO7Q+BHxA==")
  }

  test("colEncrypt null value test") {
    val df = Seq(
      (1, "a"),
      (2, null)
    ).toDF("id", "str")
    val cols = Seq("id", "str")
    val crypt: EncryptDecrypt = new EncryptDecryptECB(test_key.getBytes())
    val df_enc = crypt.encryptColumns(df, cols)
    // null values should result in null values during column encryption
    assert(df_enc.select("str").take(2)(1).isNullAt(0))
  }

  test("colEncrypt data type test") {
    val df = Seq(
      (1, "a"),
      (2, "b"),
      (3, null)
    ).toDF("id", "str")
      .withColumn("fl", lit(3.41f))
      .withColumn("db", lit(3.41d))
      .withColumn("lo", lit(3456L))

    val cols = Seq("id", "str", "fl", "db", "lo")
    val crypt: EncryptDecrypt = new EncryptDecryptECB(test_key.getBytes())
    val df_enc = crypt.encryptColumns(df, cols)
    val file = "./test_enc.parquet"

    // write/read to CSV file -> would result in String columns, since CSV does not store Metadata
    // df_enc.write.mode(SaveMode.Overwrite).format("csv").option(key = "header", value = true).save(file)
    // val df_enc_file = session.read.format("csv").option(key = "header", value = true).load(file)

    // write/read to parquet file
    df_enc.write.mode(SaveMode.Overwrite).parquet(file)
    val df_enc_file = session.read.parquet(file)

    val df_dec = crypt.decryptColumns(df_enc_file, cols)
    assert(df_dec.schema("id").dataType == IntegerType)
    assert(df_dec.schema("str").dataType == StringType)
    assert(df_dec.schema("fl").dataType == FloatType)
    assert(df_dec.schema("db").dataType == DoubleType)
    assert(df_dec.schema("lo").dataType == LongType)
  }
}
