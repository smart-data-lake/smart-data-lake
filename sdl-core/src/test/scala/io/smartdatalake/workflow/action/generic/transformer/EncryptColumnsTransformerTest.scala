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

import java.nio.file.Files
import java.time.LocalDateTime

class EncryptColumnsTransformerTest extends FunSuite {
  private val tempDir = Files.createTempDirectory("test")
  private val tempPath = tempDir.toAbsolutePath.toString

  val statePath = "target/stateTest/"
  implicit val filesystem: FileSystem = HdfsUtil.getHadoopFsWithDefaultConf(new Path(statePath))

  test("test column encryption") {
    val feedName = "test"
    val sdlb = new DefaultSmartDataLakeBuilder()

    val config = ConfigFactory.parseString(
      """
        |actions = {
        |   act = {
        |     type = CopyAction
        |     inputId = src
        |     outputId = tgt
        |     metadata {
        |       feed = test_run
        |     }
        |     transformers = [{
        |       type = EncryptColumnsTransformer
        |       encryptColumns = ["c2","c3"]
        |       #keyVariable = "CLEAR#keyblabla123456789123456789gerg;lkndfkgjnbq34tnafegnql5k3naklefnjqk35jnbhaefnlkq3n'akfnblkq3n4h'lkanblkwqn5h'lkne'lbknq5'lhkna'lkbnql5k3hn"
        |       keyVariable = "CLEAR#keyblabla"
        |     }]
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
        |    path = "target/column_encrypted"
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
    val dfSrc = Seq(("testData", "Foo", "ice"), ("bar", "Space", "water"), ("gogo", "Space", "water")).toDF("c1", "c2", "c3")
    srcDO.writeDataFrame(SparkDataFrame(dfSrc), Seq())(TestUtil.getDefaultActionPipelineContext(sdlb.instanceRegistry))

    // Run SDLB
    implicit val hadoopConf: Configuration = session.sparkContext.hadoopConfiguration
    val initialSubFeeds: Seq[SparkSubFeed] = Seq(SparkSubFeed(None, srcDO.id, Seq()))
    val (subFeeds, stats) = sdlb.exec(sdlConfig, SDLExecutionId.executionId1, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now, actionsToSkip = Map(), initialSubFeeds = initialSubFeeds, dataObjectsState = Seq(), stateStore = None, stateListeners = Seq(), simulation = false, globalConfig = globalConfig)

    // check result
    val tgt = instanceRegistry.get[ParquetFileDataObject]("tgt")
    val dfTgt = tgt.getSparkDataFrame()
    val colName = dfTgt.columns
    //assert(colName.toSeq == Seq("testcolumn", "secondcolumn", "thirdcolumn"))
  }

  test("test column decryption") {
    val feedName = "test"
    val sdlb = new DefaultSmartDataLakeBuilder()

    val config = ConfigFactory.parseString(
      """
        |actions = {
        |   act = {
        |     type = CopyAction
        |     inputId = src
        |     outputId = tgt
        |     metadata {
        |       feed = test_run
        |     }
        |     transformers = [{
        |       type = DecryptColumnsTransformer
        |       decryptColumns = ["c2","c3"]
        |       #keyVariable = "CLEAR#keyblabla123456789123456789gerg;lkndfkgjnbq34tnafegnql5k3naklefnjqk35jnbhaefnlkq3n'akfnblkq3n4h'lkanblkwqn5h'lkne'lbknq5'lhkna'lkbnql5k3hn"
        |       keyVariable = "CLEAR#keyblabla"
        |     }]
        |   }
        |}
        |dataObjects {
        |  src {
        |    #id = ~{id}
        |    type = ParquetFileDataObject
        |    path = "target/column_encrypted"
        |  }
        |  tgt {
        |    type = ParquetFileDataObject
        |    path = "target/decrypted"
        |  }
        |}
        |""".stripMargin).resolve

    val globalConfig = GlobalConfig.from(config)
    implicit val instanceRegistry: InstanceRegistry = ConfigParser.parse(config)
    implicit val session: SparkSession = sparkSessionBuilder(withHive = true, globalConfig.sparkOptions.getOrElse(Map())).getOrCreate()
    import session.implicits._

    implicit val actionPipelineContext: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
    val sdlConfig = SmartDataLakeBuilderConfig(feedSel = "ids:act")

    val srcDO = instanceRegistry.get[ParquetFileDataObject]("src")

    // Run SDLB
    implicit val hadoopConf: Configuration = session.sparkContext.hadoopConfiguration
    val initialSubFeeds: Seq[SparkSubFeed] = Seq(SparkSubFeed(None, srcDO.id, Seq()))
    val (subFeeds, stats) = sdlb.exec(sdlConfig, SDLExecutionId.executionId1, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now, actionsToSkip = Map(), initialSubFeeds = initialSubFeeds, dataObjectsState = Seq(), stateStore = None, stateListeners = Seq(), simulation = false, globalConfig = globalConfig)

    // check result
    val tgt = instanceRegistry.get[ParquetFileDataObject]("tgt")
    val dfTgt = tgt.getSparkDataFrame()
    val testCol = dfTgt.select("c2").map(f=>f.getString(0)).collect.toList
    assert(testCol == Seq("Foo", "Space", "Space"))
  }

}
