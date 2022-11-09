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

/* Literature:
https://spark.apache.org/docs/3.2.0/sql-data-sources-parquet.html#columnar-encryption
https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md#class-propertiesdrivencryptofactory
https://www.slideshare.net/databricks/data-security-at-scale-through-spark-and-parquet-encryption
 */


package io.smartdatalake.util.kms

import io.smartdatalake.testutils.TestUtil
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}
import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.SdlConfigObject.stringToDataObjectId
import io.smartdatalake.config.{ConfigParser, InstanceRegistry}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, GlobalConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.testutils.TestUtil.sparkSessionBuilder
import io.smartdatalake.workflow.action.SDLExecutionId
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions

import java.nio.file.Files
import java.time.LocalDateTime

class ParquetCryptTest extends FunSuite with BeforeAndAfter {


  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  val statePath = "target/stateTest/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))

  //TODO the uniform full file encryption not work. Even when specifying columns to encrypt, the other stay unencrypted.
  test("test uniform encryption") {
    val feedName = "test"
    val sdlb = new DefaultSmartDataLakeBuilder()

    val config = ConfigFactory.parseString(
      """
        |global = {
        |   sparkOptions {
        |     "parquet.crypto.factory.class" = "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"
        |     "parquet.encryption.kms.client.class" = "io.smartdatalake.util.kms.DummyKms"
        |   # "parquet.encryption.double.wrapping" = "false"
        |     "parquet.encryption.key.list" = "keyA:FGSDHiohafg4932zfdgj45sd,  keyB:AAECAAECAAECAAECAAECAA=="
        |     "parquet.encryption.uniform.key" = "keyA"
        |   }
        |}
        |actions = {
        |   act = {
        |     type = CopyAction
        |     inputId = src
        |     outputId = tgt
        |     metadata {
        |       feed = test_run
        |     }
        |   }
        |}
        |dataObjects {
        |  src {
        |    #id = ~{id}
        |    type = CsvFileDataObject
        |    path = "target/raw"
        |  }
        |  tgt {
        |    type = ParquetFileDataObject
        |    parquetOptions {
        |       #"parquet.encryption.uniform.key" = "keyA"
        |       #"parquet.encryption.column.keys" = "keyB:secondcolumn;keyB:thirdcolumn" # format: "<master key ID>:<column>,<column>;<master key ID>:<column>,.."
        |       #"parquet.encryption.footer.key" = "keyB"
        |    }
        |    path = "target/full_encrypted"
        |  }
        |}
        |""".stripMargin).resolve

    val globalConfig = GlobalConfig.from(config)
    implicit val instanceRegistry: InstanceRegistry = ConfigParser.parse(config)
    implicit val session: SparkSession = sparkSessionBuilder(withHive = true, globalConfig.sparkOptions.getOrElse(Map())).getOrCreate()
    import session.implicits._

    implicit val actionPipelineContext: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "ids:act")

    val srcDO = instanceRegistry.get[CsvFileDataObject]("src")
    assert(srcDO != None)
    val dfSrc = Seq(("testData", "Foo", "ice"), ("bar", "Space", "water")).toDF("testColumn", "secondColumn", "thirdColumn")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    // Run SDLB
    implicit val hadoopConf: Configuration = session.sparkContext.hadoopConfiguration
    val initialSubFeeds: Seq[SparkSubFeed] = Seq(SparkSubFeed(None, srcDO.id, Seq()))
    val (subFeeds, stats) = sdlb.exec(sdlConfig, SDLExecutionId.executionId1, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now, actionsToSkip = Map(), initialSubFeeds = initialSubFeeds, dataObjectsState = Seq(), stateStore = None, stateListeners = Seq(), simulation = false, globalConfig = globalConfig)

    // check result
    val tgt = instanceRegistry.get[ParquetFileDataObject]("tgt")
    val dfTgt = tgt.getSparkDataFrame()
    val colName = dfTgt.columns
    assert(colName.toSeq == Seq("testcolumn", "secondcolumn", "thirdcolumn"))
  }


  test("test column encryption") {
    val feedName = "test"
    val sdlb = new DefaultSmartDataLakeBuilder()

    val config = ConfigFactory.parseString(
      """
        |global = {
        |   sparkOptions {
        |     "parquet.crypto.factory.class" = "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"
        |     "parquet.encryption.kms.client.class" = "io.smartdatalake.util.kms.DummyKms"
        |     "parquet.encryption.double.wrapping" = "false"
        |     "parquet.encryption.key.list" = "keyA:AAECAwQFBgcICQoLDA0ODw== ,  keyB:AAECAAECAAECAAECAAECAA=="
        |     #"parquet.encryption.algorithm" = "AES_GCM" # AES_GCM is default, alternatively AES_GCM_CTR_V1 which verifies the integrity of the metadata parts only and not that of the data parts with lower overhead
        |     "parquet.encryption.plaintext.footer" = "true"
        |   }
        |}
        |actions = {
        |   act = {
        |     type = CopyAction
        |     inputId = src
        |     outputId = tgt
        |     metadata {
        |       feed = test_run
        |     }
        |   }
        |}
        |dataObjects {
        |  src {
        |    #id = ~{id}
        |    type = CsvFileDataObject
        |    path = "target/raw"
        |  }
        |  tgt {
        |    type = ParquetFileDataObject
        |    parquetOptions {
        |       "parquet.encryption.column.keys" = "keyA:secondcolumn;keyB:thirdcolumn" # format: "<master key ID>:<column>,<column>;<master key ID>:<column>,.."
        |       "parquet.encryption.footer.key" = "keyA" # also required in plain text footer mode to sign footer content -> verify integrity
        |    }
        |    path = "target/encrypted"
        |  }
        |}
        |""".stripMargin).resolve

    val globalConfig = GlobalConfig.from(config)
    implicit val instanceRegistry: InstanceRegistry = ConfigParser.parse(config)
    implicit val session: SparkSession = sparkSessionBuilder(withHive = true, globalConfig.sparkOptions.getOrElse(Map())).getOrCreate()
    import session.implicits._

    implicit val actionPipelineContext: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "ids:act")

    val srcDO = instanceRegistry.get[CsvFileDataObject]("src")
    assert(srcDO != None)
    val dfSrc = Seq(("testData", "Foo", "ice"), ("bar", "Space", "water")).toDF("testColumn", "secondColumn", "thirdColumn")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    // Run SDLB
    //sdlb.startSimulation(sdlConfig, Seq(SparkSubFeed(Some(SparkDataFrame(dfSrc)), srcDO.id, Seq())))
    //sdlb.run(sdlConfig)
    implicit val hadoopConf: Configuration = session.sparkContext.hadoopConfiguration
    val initialSubFeeds : Seq[SparkSubFeed] = Seq(SparkSubFeed(None, srcDO.id, Seq()))
    val (subFeeds, stats) = sdlb.exec(sdlConfig, SDLExecutionId.executionId1, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now, actionsToSkip = Map(), initialSubFeeds = initialSubFeeds, dataObjectsState = Seq(), stateStore = None, stateListeners = Seq(), simulation = false, globalConfig = globalConfig)

    // check result
    val tgt = instanceRegistry.get[ParquetFileDataObject]("tgt")
    val dfTgt = tgt.getSparkDataFrame()
    val colName = dfTgt.columns
    assert(colName.toSeq == Seq("testcolumn", "secondcolumn", "thirdcolumn"))
  }

  test("test decryption") {
    val feedName = "test"
    val sdlb = new DefaultSmartDataLakeBuilder()

    val config = ConfigFactory.parseString(
      """
        |global = {
        |   sparkOptions {
        |     "parquet.crypto.factory.class" = "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"
        |     "parquet.encryption.kms.client.class" = "io.smartdatalake.util.kms.DummyKms"
        |     "parquet.encryption.key.list" = "keyA:AAECAwQFBgcICQoLDA0ODw==" // ,  keyB:AAECAAECAAECAAECAAECAA=="
        |     "parquet.encryption.algorithm" = "AES_GCM" # AES_GCM is default, alternatively AES_GCM_CTR_V1 which verifies the integrity of the metadata parts only and not that of the data parts with lower overhead
        |     "parquet.encryption.plaintext.footer" = "true"
        |   }
        |}
        |actions = {
        |   act = {
        |     type = CopyAction
        |     inputId = src
        |     outputId = tgt
        |     metadata {
        |       feed = test_run
        |     }
        |   }
        |}
        |dataObjects {
        |  src {
        |    type = ParquetFileDataObject
        |    parquetOptions {
        |       #"parquet.encryption.uniform.key" = "keyA"
        |       "parquet.encryption.column.keys" = "keyA:secondcolumn" # format: "<master key ID>:<column>,<column>;<master key ID>:<column>,.."
        |       "parquet.encryption.footer.key" = "keyA" # also required in plain text footer mode to sign footer content -> verify integrity
        |    }
        |    path = "target/encrypted"
        |  }
        |  tgt {
        |    #id = ~{id}
        |    type = CsvFileDataObject
        |    schema = "testcolumn STRING, secondcolumn STRING, thirdColumn STRING"
        |    path = "target/decrypted"
        |  }
        |}
        |""".stripMargin).resolve

    val globalConfig = GlobalConfig.from(config)
    implicit val instanceRegistry: InstanceRegistry = ConfigParser.parse(config)
    implicit val session: SparkSession = sparkSessionBuilder(withHive = true, globalConfig.sparkOptions.getOrElse(Map())).getOrCreate()

    implicit val actionPipelineContext: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "ids:act")

    val srcDO = instanceRegistry.get[ParquetFileDataObject]("src")
    //val dfSrc = srcDO.getSparkDataFrame()
    // Run SDLB
    implicit val hadoopConf: Configuration = session.sparkContext.hadoopConfiguration
    //val initialSubFeeds : Seq[SparkSubFeed] = Seq(SparkSubFeed(Some(SparkDataFrame(dfSrc)), srcDO.id, Seq()))
    val initialSubFeeds: Seq[SparkSubFeed] = Seq(SparkSubFeed(None, srcDO.id, Seq()))
    val (subFeeds, stats) = sdlb.exec(sdlConfig, SDLExecutionId.executionId1, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now, actionsToSkip = Map(), initialSubFeeds = initialSubFeeds, dataObjectsState = Seq(), stateStore = None, stateListeners = Seq(), simulation = false, globalConfig = globalConfig)

    // check result
    val tgt = instanceRegistry.get[CsvFileDataObject]("tgt")
    val dfTgt = tgt.getSparkDataFrame()
    val colName = dfTgt.columns
    assert(colName.toSeq == Seq("testcolumn", "secondcolumn", "thirdcolumn"))
  }
}