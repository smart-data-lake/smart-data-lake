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
package io.smartdatalake.workflow.action

import java.io.File
import java.nio.file.Files

import com.holdenkarau.spark.testing.Utils
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.connection.SftpFileRefConnection
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.{ActionPipelineContext, FileSubFeed}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.sshd.server.SshServer
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

class FileTransferActionTest extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {

  protected implicit val session: SparkSession = TestUtil.sessionHiveCatalog

  val tempDir: File = Utils.createTempDir()
  val tempPath: String = tempDir.toPath.toAbsolutePath.toString

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  private var sshd: SshServer = _
  val sshPort = 8001
  val sshUser = "test"
  val sshPwd = "test"

  override protected def beforeAll(): Unit = {
    sshd = TestUtil.setupSSHServer(sshPort, sshUser, sshPwd)
  }

  override protected def afterAll(): Unit = {
    sshd.stop()
  }

  before {
    instanceRegistry.clear
    instanceRegistry.register(SftpFileRefConnection( "con1", "localhost", sshPort, "CLEAR#"+sshUser, Some("CLEAR#"+sshPwd), ignoreHostKeyVerification = true))
  }

  test("copy file from sftp to hadoop without partitions") {

    val feed = "filetransfer"
    val ftpDir = "testSrc"
    val hadoopDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(ftpDir).resolve(resourceFile).toFile)

    // setup DataObjects
    val srcDO = SFtpFileRefDataObject( "src1", tempDir.resolve(ftpDir).toString.replace('\\', '/'), "con1")
    val tgtDO = CsvFileDataObject( "tgt1", tempDir.resolve(hadoopDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true"))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)


    // prepare & start load
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val action1 = FileTransferAction("fta", srcDO.id, tgtDO.id)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    action1.exec(Seq(srcSubFeed))

    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 1)
    assert(r1.head.fileName == resourceFile)
  }

  test("copy file from sftp to hadoop with partitions and no partition values filter") {

    val feed = "filetransfer"
    val ftpDir = "testSrc"
    val hadoopDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val datePartitionVal = "20190101"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(ftpDir).resolve(datePartitionVal).resolve(resourceFile).toFile)

    // setup DataObjects
    val srcDO = SFtpFileRefDataObject( "src1"
      , tempDir.resolve(ftpDir).toString.replace('\\', '/')
      , connectionId = "con1"
      , partitions = Seq("date", "town", "year")
      , partitionLayout = Some("%date%/AB_%town%_%year:[0-9]+%")
    )
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(hadoopDir).toString.replace('\\', '/')
      , partitions = Seq("date", "town", "year")
      , csvOptions = Map("header" -> "true"))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val action1 = FileTransferAction("fta1", srcDO.id, tgtDO.id)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    action1.exec(Seq(srcSubFeed))

    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 1)
    assert(r1.head.fileName == resourceFile)
    assert(r1.head.partitionValues.keys == Set("date", "town", "year"))
  }

  test("copy file from sftp to hadoop with partitions and negative top-level partition filter") {

    val feed = "filetransfer"
    val ftpDir = "testSrc"
    val hadoopDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val datePartitionVal = "20190101"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(ftpDir).resolve(datePartitionVal).resolve(resourceFile).toFile)

    // setup DataObjects
    val srcDO = SFtpFileRefDataObject( "src1"
      , tempDir.resolve(ftpDir).toString.replace('\\', '/')
      , connectionId = "con1"
      , partitions = Seq("date", "town", "year")
      , partitionLayout = Some("%date%/AB_%town%_%year:[0-9]+%")
    )
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(hadoopDir).toString.replace('\\', '/')
      , partitions = Seq("date", "town", "year")
      , csvOptions = Map("header" -> "true"))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val action1 = FileTransferAction("fta", srcDO.id, tgtDO.id)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq(PartitionValues(Map("date"->"00010101"))))
    action1.exec(Seq(srcSubFeed))

    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.isEmpty)
  }

  test("copy file from sftp to hadoop with partitions and positive top-level partition filter") {

    val feed = "filetransfer"
    val ftpDir = "testSrc"
    val hadoopDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val datePartitionVal = "20190101"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(ftpDir).resolve(datePartitionVal).resolve(resourceFile).toFile)

    // setup DataObjects
    val srcDO = SFtpFileRefDataObject("src1"
      , tempDir.resolve(ftpDir).toString.replace('\\', '/')
      , connectionId = "con1"
      , partitions = Seq("date", "town", "year")
      , partitionLayout = Some("%date%/AB_%town%_%year:[0-9]+%")
    )
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(hadoopDir).toString.replace('\\', '/')
      , partitions = Seq("date", "town", "year")
      , csvOptions = Map("header" -> "true"))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val action1 = FileTransferAction("fta", srcDO.id, tgtDO.id)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq(PartitionValues(Map("date"->datePartitionVal))))
    action1.exec(Seq(srcSubFeed))

    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 1)
    assert(r1.head.fileName == resourceFile)
    assert(r1.head.partitionValues.keys == Set("date", "town", "year"))
  }

  test("copy file from sftp to hadoop with partitions and positive all-level partition filter") {

    val feed = "filetransfer"
    val ftpDir = "testSrc"
    val hadoopDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val datePartitionVal = "20190101"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(ftpDir).resolve(datePartitionVal).resolve(resourceFile).toFile)

    // setup DataObjects
    val srcDO = SFtpFileRefDataObject( "src1"
      , tempDir.resolve(ftpDir).toString.replace('\\', '/')
      , connectionId = "con1"
      , partitions = Seq("date", "town", "year")
      , partitionLayout = Some("%date%/AB_%town%_%year:[0-9]+%")
    )
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(hadoopDir).toString.replace('\\', '/')
      , partitions = Seq("date", "town", "year")
      , csvOptions = Map("header" -> "true"))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val action1 = FileTransferAction("fta", srcDO.id, tgtDO.id)
    val partitionValuesFilter = PartitionValues(Map("date"->datePartitionVal, "town"->"NYC", "year"->2019))
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq(partitionValuesFilter))
    action1.exec(Seq(srcSubFeed))

    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 1)
    assert(r1.head.fileName == resourceFile)
    assert(r1.head.partitionValues.keys == Set("date", "town", "year"))
  }

  test("copy file from sftp to hadoop with partitions and negative all-level partition filter") {

    val feed = "filetransfer"
    val ftpDir = "testSrc"
    val hadoopDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val datePartitionVal = "20190101"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(ftpDir).resolve(datePartitionVal).resolve(resourceFile).toFile)

    // setup DataObjects
    val srcDO = SFtpFileRefDataObject( "src1"
      , tempDir.resolve(ftpDir).toString.replace('\\', '/')
      , connectionId = "con1"
      , partitions = Seq("date", "town", "year")
      , partitionLayout = Some("%date%/AB_%town%_%year:[0-9]+%")
    )
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(hadoopDir).toString.replace('\\', '/')
      , partitions = Seq("date", "town", "year")
      , csvOptions = Map("header" -> "true"))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val action1 = FileTransferAction("fta", srcDO.id, tgtDO.id)
    val partitionValuesFilter = PartitionValues(Map("date"->datePartitionVal, "town"->"NYC", "year"->"0001"))
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq(partitionValuesFilter))
    action1.exec(Seq(srcSubFeed))

    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.isEmpty)
  }

  test("copy file from hadoop to hadoop without partitions") {

    val feed = "filetransfer"
    val srcDir = "testSrc"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile).toFile)

    // setup DataObjects
    val srcDO = CsvFileDataObject("src1", tempDir.resolve(srcDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true"))
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true"))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val action1 = FileTransferAction("fta", srcDO.id, tgtDO.id)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed)).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 1)
    assert(r1.head.fileName == resourceFile)
  }

  test("copy file from hadoop to hadoop without partitions and mode overwrite and deleteDataAfterRead") {

    val feed = "filetransfer"
    val srcDir = "testSrc"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)

    // copy data 1 file to hadoop
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile+"1").toFile)

    // setup DataObjects
    val srcDO = CsvFileDataObject("src1", tempDir.resolve(srcDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true"))
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true"), saveMode = SaveMode.Overwrite)
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load 1
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val action1 = FileTransferAction("fta", srcDO.id, tgtDO.id, deleteDataAfterRead = true)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    action1.preExec
    val tgtSubFeed1 = action1.exec(Seq(srcSubFeed)).head
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed1))
    assert(tgtSubFeed1.dataObjectId == tgtDO.id)

    // check 1
    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 1)
    assert(r1.head.fileName == resourceFile+"1")

    // copy data 2 file to hadoop
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile+"2").toFile)

    // start load 2
    action1.preExec
    val tgtSubFeed2 = action1.exec(Seq(srcSubFeed)).head
    action1.postExec(Seq(srcSubFeed), Seq(tgtSubFeed2))

    // check 2
    val r2 = tgtDO.getFileRefs(Seq())
    assert(r2.size == 1)
    assert(r2.head.fileName == resourceFile+"2")
  }

  test("copy file from hadoop to hadoop without partitions and mode append and deleteDataAfterRead") {

    val feed = "filetransfer"
    val srcDir = "testSrc"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"
    val tempDir = Files.createTempDirectory(feed)

    // copy data 1 file to hadoop
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile+"1").toFile)

    // setup DataObjects
    val srcDO = CsvFileDataObject("src1", tempDir.resolve(srcDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true"))
    val tgtDO = CsvFileDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), csvOptions = Map("header" -> "true"), saveMode = SaveMode.Append)
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load 1
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val action1 = FileTransferAction("fta", srcDO.id, tgtDO.id, deleteDataAfterRead = true)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed)).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    // check 1
    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 1)
    assert(r1.head.fileName == resourceFile+"1")

    // copy data 2 file to hadoop
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir).resolve(resourceFile+"2").toFile)

    // start load 2
    action1.exec(Seq(srcSubFeed)).head

    // check 2
    val r2 = tgtDO.getFileRefs(Seq())
    assert(r2.size == 2)
  }

  test("copy webservice output to hadoop file") {

    val feed = "filetransfer"
    val tgtDir = "testTgt"
    val tempDir = Files.createTempDirectory(feed)

    // setup DataObjects
    // For testing we will read something from Spark UI API...
    val srcDO = WebserviceFileDataObject("src1", WebserviceOptions(session.sparkContext.uiWebUrl.get + "/api/v1/applications"))
    val tgtDO = JsonFileDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val action1 = FileTransferAction("fta", srcDO.id, tgtDO.id)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed)).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 1)
  }

  test("copy partitioned webservice output to hadoop file") {

    val feed = "filetransfer"
    val tgtDir = "testTgt"
    val tempDir = Files.createTempDirectory(feed)

    // setup DataObjects
    // For testing we will read something from Spark UI API...
    val srcDO = WebserviceFileDataObject("src1", WebserviceOptions(session.sparkContext.uiWebUrl.get + "/api/v1")
      , partitionDefs = Seq(WebservicePartitionDefinition("subject", Seq("applications","version"))), partitionLayout = Some("/%subject%"))
    val tgtDO = JsonFileDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'))
    instanceRegistry.register(srcDO)
    instanceRegistry.register(tgtDO)

    // prepare & start load
    implicit val context1 = ActionPipelineContext(feed, "test", instanceRegistry)
    val action1 = FileTransferAction("fta", srcDO.id, tgtDO.id)
    val srcSubFeed = FileSubFeed(None, "src1", partitionValues = Seq())
    val tgtSubFeed = action1.exec(Seq(srcSubFeed)).head
    assert(tgtSubFeed.dataObjectId == tgtDO.id)

    val r1 = tgtDO.getFileRefs(Seq())
    assert(r1.size == 2)
  }
}
