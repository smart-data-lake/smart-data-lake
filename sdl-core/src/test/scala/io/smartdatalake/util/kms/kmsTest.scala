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


  test("testencryption") {
    val feedName = "test"
    val sdlb = new DefaultSmartDataLakeBuilder()

    val config = ConfigFactory.parseString(
      """
        |global = {
        |   sparkOptions {
        |     "parquet.crypto.factory.class" = "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"
        |     "parquet.encryption.kms.client.class" = "io.smartdatalake.util.kms.DummyKms"
        |     "parquet.encryption.key.list" = "keyA:AAECAwQFBgcICQoLDA0ODw== ,  keyB:AAECAAECAAECAAECAAECAA=="
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
        |    path = "target/src"
        |  }
        |  tgt {
        |    type = ParquetFileDataObject
        |    parquetOptions {
        |       "parquet.encryption.uniform.key" = "keyA"
        |       #"parquet.encryption.column.keys" = "keyA:testcolumn"
        |       #"parquet.encryption.footer.key" = "keyB"
        |    }
        |    path = "target/tgt"
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
    val dfSrc = Seq(("testData", "Foo"), ("bar", "Space")).toDF("testColumn", "secondColumn")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    // Run SDLB
    //sdlb.startSimulation(sdlConfig, Seq(SparkSubFeed(Some(SparkDataFrame(dfSrc)), srcDO.id, Seq())))
    //sdlb.run(sdlConfig)
    implicit val hadoopConf: Configuration = session.sparkContext.hadoopConfiguration
    val initialSubFeeds : Seq[SparkSubFeed] = Seq(SparkSubFeed(Some(SparkDataFrame(dfSrc)), srcDO.id, Seq()))
    val (subFeeds, stats) = sdlb.exec(sdlConfig, SDLExecutionId.executionId1, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now, actionsToSkip = Map(), initialSubFeeds = initialSubFeeds, dataObjectsState = Seq(), stateStore = None, stateListeners = Seq(), simulation = false, globalConfig = globalConfig)

    // check result
    val tgt = instanceRegistry.get[ParquetFileDataObject]("tgt")
    val dfTgt = tgt.getSparkDataFrame()
    val colName = dfTgt.columns
    assert(colName.toSeq == Seq("testcolumn", "secondcolumn"))
  }
}