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

package io.smartdatalake.workflow.action.generic.transformer

import io.smartdatalake.testutils.TestUtil
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.SdlConfigObject.stringToDataObjectId
import io.smartdatalake.config.{ConfigParser, ConfigurationException, InstanceRegistry}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, GlobalConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.testutils.TestUtil.sparkSessionBuilder
import io.smartdatalake.workflow.action.SDLExecutionId
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.nio.file.Files
import java.time.LocalDateTime

class EncryptColumnsTransformerTest extends FunSuite {
  private val tempDir = Files.createTempDirectory("test")

  val statePath = "target/stateTest/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))

  def run_test(enc_type: String): Unit = {
    val sdlb = new DefaultSmartDataLakeBuilder()

    val config = ConfigFactory.parseString(
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
        |       key = "A%D*G-KaPdSgVkYp"
        |       algorithm = ${enc_type}
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
        |       key = "A%D*G-KaPdSgVkYp"
        |       algorithm = ${enc_type}
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
        |  }
        |  dec {
        |    type = ParquetFileDataObject
        |    path = "target/decrypted"
        |  }
        |}
        |""".stripMargin).resolve

    val globalConfig = GlobalConfig.from(config)
    implicit val instanceRegistry: InstanceRegistry = ConfigParser.parse(config)
    implicit val session: SparkSession = TestUtil.session
    import session.implicits._

    implicit val actionPipelineContext: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = s"ids:actenc,ids:actdec")

    val srcDO = instanceRegistry.get[CsvFileDataObject]("src")
    val dfSrc = Seq(("testData", "Foo", "ice"), ("bar", "Space", "water"), ("gogo", "Space", "water")).toDF("c1", "c2", "c3")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    // Run SDLB
    implicit val hadoopConf: Configuration = session.sparkContext.hadoopConfiguration
    val initialSubFeeds: Seq[SparkSubFeed] = Seq(SparkSubFeed(None, srcDO.id, Seq()))
    val (subFeeds, stats) = sdlb.exec(sdlConfig, SDLExecutionId.executionId1, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now, actionsToSkip = Map(), initialSubFeeds = initialSubFeeds, dataObjectsState = Seq(), stateStore = None, stateListeners = Seq(), simulation = false, globalConfig = globalConfig)

    // check result
    // first check the encoded dataFrame
    val enc = instanceRegistry.get[ParquetFileDataObject]("enc")
    val dfEnc = enc.getSparkDataFrame()
    val colName = dfEnc.columns
    assert(colName.toSeq == Seq("c1", "c2", "c3"))
    val testCol = dfEnc.select("c2").map(f => f.getString(0)).collect.toList
    dfEnc.show(false)
    print(s"### ${enc_type} encrypted dataFrame")
    assert(testCol != Seq("Foo", "Space", "Space"))
    if (enc_type === "GCM") {
      assert(testCol(1) !== testCol(2), "2 encrypted items should not result in the same ciphertext with GCM")
    } else if (enc_type === "ECB") {
      assert(testCol(1) === testCol(2), "2 encrypted items should result in the same ciphertext with ECB")
    }

    // check the decoded DataFrame
    val dec = instanceRegistry.get[ParquetFileDataObject]("dec")
    val dfDec = dec.getSparkDataFrame()
    dfDec.show(false)
    print(s"### ${enc_type} decrypted dataFrame")

    val colDecName = dfDec.columns
    assert(colDecName.toSeq == Seq("c1", "c2", "c3"))
    val testDecCol = dfDec.select("c2").map(f => f.getString(0)).collect.toList
    assert(testDecCol == Seq("Foo", "Space", "Space"))
  }

  test("test column encryption and decryption") {
    run_test("GCM")
  }

  test("test ECB column encryption and decryption") {
    run_test("ECB")
  }

  test("test column encryption, unsupported algorithm") {
    intercept[ConfigurationException]{
      run_test("notSupported")
    }
  }

  test("test column encryption and decryption with Class Name") {
    run_test("io.smartdatalake.util.crypt.EncryptDecryptECB")
  }
}
