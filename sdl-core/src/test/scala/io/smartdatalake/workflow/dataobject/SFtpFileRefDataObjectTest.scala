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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.BasicAuthMode
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.filetransfer.StreamFileTransfer
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.SFtpFileRefConnection
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.sshd.server.SshServer
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, Matchers}

import java.nio.file.{Files, Path}

class SFtpFileRefDataObjectTest extends FunSuite with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  implicit val session: SparkSession = TestUtil.session
  implicit val registry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext

  private var sshd: SshServer = _
  val sshPort = 8001
  val sshUser = "test"
  val sshPwd = "test"
  var con: SFtpFileRefConnection = _

  var tempDir: Path = _

  override protected def beforeAll(): Unit = {
    sshd = TestUtil.setupSSHServer(sshPort, sshUser, sshPwd)
    con = SFtpFileRefConnection( "con1", "localhost", sshPort, BasicAuthMode(Some(StringOrSecret(sshUser)), Some(StringOrSecret(sshPwd))), ignoreHostKeyVerification = true, maxParallelConnections = 10)
  }

  override protected def afterAll(): Unit = {
    sshd.stop()
  }

  override def beforeEach(): Unit = {
    registry.clear()
    registry.register(con)
    tempDir = Files.createTempDirectory("sftp-test")
  }

  override def afterEach(): Unit = {
    FileUtils.deleteDirectory(tempDir.toFile)
    tempDir = null
  }

  test("initialize") {
    // no partition
    SFtpFileRefDataObject( "src1", "test", connectionId = "con1")

    // partitions without layout
    intercept[IllegalArgumentException](SFtpFileRefDataObject( "src1", "test", connectionId = "con1", partitions = Seq("test")))

    // layout without partitions
    intercept[IllegalArgumentException](SFtpFileRefDataObject( "src1", "test", connectionId = "con1", partitionLayout = Some("%test%")))

    // layout with incomplete partitions
    intercept[IllegalArgumentException](SFtpFileRefDataObject( "src1", "test", connectionId = "con1", partitions = Seq("test1"), partitionLayout = Some("%test%")))

    // with partitions
    SFtpFileRefDataObject( "src1", "test", connectionId = "con1", partitions = Seq("test"), partitionLayout = Some("%test%"))

    // with multiple partitions
    SFtpFileRefDataObject( "src1", "test", connectionId = "con1", partitions = Seq("test1", "test2"), partitionLayout = Some("%test1%/abc/%test2%/def"))
  }

  test("get FileRef's without partitions") {

    val ftpDir = "testSrc"
    val resourceFile = "AB_NYC_2019.csv"

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(ftpDir).resolve(resourceFile).toFile)

    // setup DataObject
    val sftpDO = SFtpFileRefDataObject( "src1", tempDir.resolve(ftpDir).toString.replace('\\','/'), connectionId = "con1")

    // list files
    val fileRefs = sftpDO.getFileRefs(Seq())
    assert(fileRefs.size == 1)
    assert(fileRefs.head.fileName == resourceFile)
    assert(sftpDO.listPartitions.isEmpty)
  }

  test("get FileRef's with partitions in filename") {

    val ftpDir = "testSrc"
    val resourceFile = "AB_NYC_2019.csv"

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(ftpDir).resolve(resourceFile).toFile)

    // setup DataObject
    val sftpDO = SFtpFileRefDataObject( "src1"
      , tempDir.resolve(ftpDir).toString.replace('\\','/')
      , connectionId = "con1"
      , partitions = Seq("town", "year")
      , partitionLayout = Some("AB_%town%_%year:[0-9]+%" ))
    val partitionValuesExpected = Seq(PartitionValues(Map("town" -> "NYC", "year" -> "2019")))

    // list all files and extract partitions
    val fileRefsAll = sftpDO.getFileRefs(Seq())
    assert(fileRefsAll.size == 1)
    assert(fileRefsAll.head.fileName == resourceFile)
    assert(fileRefsAll.head.partitionValues == partitionValuesExpected.head)

    // list with matched partition filter
    val fileRefsPartitionFilter = sftpDO.getFileRefs(partitionValuesExpected)
    assert(fileRefsPartitionFilter.size == 1)
    assert(fileRefsPartitionFilter.head.fileName == resourceFile)

    // list with unmatched partition filter
    val fileRefsPartitionNoMatchFilter = sftpDO.getFileRefs(Seq(PartitionValues(Map("town" -> "NYC", "year" -> "2020"))))
    assert(fileRefsPartitionNoMatchFilter.isEmpty)

    // check list partition values
    val partitionValuesListed = sftpDO.listPartitions
    partitionValuesListed shouldEqual partitionValuesListed
  }

  test("get FileRef's with partitions as directories") {

    val ftpDir = "testSrc"
    val partitionDir = "20190101"
    val resourceFile = "AB_NYC_2019.csv"

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(ftpDir).resolve(partitionDir).resolve(resourceFile).toFile)

    // setup DataObject
    val sftpDO = SFtpFileRefDataObject( "src1"
      , tempDir.resolve(ftpDir).toString.replace('\\','/')
      , connectionId = "con1"
      , partitions = Seq("date", "town", "year")
      , partitionLayout = Some("%date%/AB_%town%_%year:[0-9]+%" ))
    val partitionValuesExpected = Seq(PartitionValues(Map("date" -> "20190101", "town" -> "NYC", "year" -> "2019")))

    // list all files and extract partitions
    val fileRefsAll = sftpDO.getFileRefs(Seq())
    assert(fileRefsAll.size == 1)
    assert(fileRefsAll.head.fileName == resourceFile)
    assert(fileRefsAll.head.partitionValues == partitionValuesExpected.head)

    // list with matched partition filter
    val fileRefsPartitionFilter = sftpDO.getFileRefs(partitionValuesExpected)
    assert(fileRefsPartitionFilter.size == 1)
    assert(fileRefsPartitionFilter.head.fileName == resourceFile)

    // list with unmatched partition filter
    val fileRefsPartitionNoMatchFilter = sftpDO.getFileRefs(Seq(PartitionValues(Map("date" -> "20190101", "town" -> "NYC", "year" -> "2020"))))
    assert(fileRefsPartitionNoMatchFilter.isEmpty)

    // check list partition values
    val partitionValuesListed = sftpDO.listPartitions
    partitionValuesListed shouldEqual partitionValuesListed
  }

  test("rename file, handle already existing") {

    val ftpDir = "testSrc"
    val resourceFile = "AB_NYC_2019.csv"

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(ftpDir).resolve(resourceFile).toFile)

    // setup DataObject
    val sftpDO = SFtpFileRefDataObject( "src1", tempDir.resolve(ftpDir).toString.replace('\\','/'), connectionId = "con1")
    val fileRefs = sftpDO.getFileRefs(Seq())
    assert(fileRefs.map(_.fileName) == Seq(resourceFile))

    // rename 1
    sftpDO.renameFileHandleAlreadyExisting(
      tempDir.resolve(ftpDir).resolve(resourceFile).toString.replace('\\','/'),
      tempDir.resolve(ftpDir).resolve(resourceFile+".temp").toString.replace('\\','/')
    )
    val fileRefs1 = sftpDO.getFileRefs(Seq())
    assert(fileRefs1.map(_.fileName) == Seq(resourceFile+".temp"))

    // copy data file again to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(ftpDir).resolve(resourceFile).toFile)

    // rename 2 -> handle already existing
    sftpDO.renameFileHandleAlreadyExisting(
      tempDir.resolve(ftpDir).resolve(resourceFile).toString.replace('\\','/'),
      tempDir.resolve(ftpDir).resolve(resourceFile+".temp").toString.replace('\\','/')
    )
    val fileRefs2 = sftpDO.getFileRefs(Seq())
    assert(fileRefs2.size == 2 && fileRefs2.map(_.fileName).forall(_.startsWith(resourceFile)))
  }


  test("overwrite target") {

    val srcDir1 = "testSrc1"
    val srcDir2 = "testSrc2"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir1).resolve(resourceFile + "1").toFile)
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir2).resolve(resourceFile + "2").toFile)

    // setup DataObject
    val srcDO1 = SFtpFileRefDataObject("src1", tempDir.resolve(srcDir1).toString.replace('\\', '/'), connectionId = "con1")
    val srcDO2 = SFtpFileRefDataObject("src1", tempDir.resolve(srcDir2).toString.replace('\\', '/'), connectionId = "con1")
    val tgtDO = SFtpFileRefDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), connectionId = "con1")

    // src1 file transfer
    val filetransfer1 = new StreamFileTransfer(srcDO1, tgtDO)
    val srcFileRefs1 = srcDO1.getFileRefs(Seq())
    assert(srcFileRefs1.nonEmpty)
    val fileRefPairs1 = tgtDO.translateFileRefs(srcFileRefs1)
    tgtDO.startWritingOutputStreams(fileRefPairs1.map(_.tgt.partitionValues))
    filetransfer1.exec(fileRefPairs1)
    val tgtFileRefs1 = tgtDO.getFileRefs(Seq())
    assert(tgtFileRefs1.map(f => tgtDO.relativizePath(f.fullPath)).toSet == Set("AB_NYC_2019.csv1"))

    // src2 file transfer overwriting partition 2022
    val filetransfer2 = new StreamFileTransfer(srcDO2, tgtDO)
    val srcFileRefs2 = srcDO2.getFileRefs(Seq())
    assert(srcFileRefs2.nonEmpty)
    val fileRefPairs2 = tgtDO.translateFileRefs(srcFileRefs2)
    tgtDO.startWritingOutputStreams(fileRefPairs2.map(_.tgt.partitionValues))
    filetransfer2.exec(fileRefPairs2)
    val tgtFileRefs2 = tgtDO.getFileRefs(Seq())
    assert(tgtFileRefs2.map(f => tgtDO.relativizePath(f.fullPath)).toSet == Set("AB_NYC_2019.csv2"))
  }

  test("overwrite directory based partition") {

    val srcDir1 = "testSrc1"
    val srcDir2 = "testSrc2"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir1).resolve("2022").resolve(resourceFile+"1").toFile)
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir2).resolve("2022").resolve(resourceFile+"2").toFile)

    // setup DataObject
    val srcDO1 = SFtpFileRefDataObject("src1", tempDir.resolve(srcDir1).toString.replace('\\', '/'), connectionId = "con1", partitions = Seq("year"), partitionLayout = Some("%year%/"))
    val srcDO2 = SFtpFileRefDataObject("src1", tempDir.resolve(srcDir2).toString.replace('\\', '/'), connectionId = "con1", partitions = Seq("year"), partitionLayout = Some("%year%/"))
    val tgtDO = SFtpFileRefDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), connectionId = "con1", partitions = Seq("year"), partitionLayout = Some("%year%/"))

    // src1 file transfer
    val filetransfer1 = new StreamFileTransfer(srcDO1, tgtDO)
    val srcFileRefs1 = srcDO1.getFileRefs(Seq())
    assert(srcFileRefs1.nonEmpty)
    val fileRefPairs1 = tgtDO.translateFileRefs(srcFileRefs1)
    tgtDO.startWritingOutputStreams(fileRefPairs1.map(_.tgt.partitionValues))
    filetransfer1.exec(fileRefPairs1)
    val tgtFileRefs1 = tgtDO.getFileRefs(Seq())
    assert(tgtFileRefs1.map(f => tgtDO.relativizePath(f.fullPath)).toSet == Set("2022/AB_NYC_2019.csv1"))

    // src2 file transfer overwriting partition 2022
    val filetransfer2 = new StreamFileTransfer(srcDO2, tgtDO)
    val srcFileRefs2 = srcDO2.getFileRefs(Seq())
    assert(srcFileRefs2.nonEmpty)
    val fileRefPairs2 = tgtDO.translateFileRefs(srcFileRefs2)
    tgtDO.startWritingOutputStreams(fileRefPairs2.map(_.tgt.partitionValues))
    filetransfer2.exec(fileRefPairs2)
    val tgtFileRefs2 = tgtDO.getFileRefs(Seq())
    assert(tgtFileRefs2.map(f => tgtDO.relativizePath(f.fullPath)).toSet == Set("2022/AB_NYC_2019.csv2"))
  }

  test("overwrite directory and filename based partition") {

    val srcDir1 = "testSrc1"
    val tgtDir = "testTgt"
    val resourceFile = "AB_NYC_2019.csv"

    // copy data file to ftp
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(srcDir1).resolve("20220101").resolve(resourceFile + "2").toFile)
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(tgtDir).resolve("20220101").resolve(resourceFile + "1").toFile)
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(tgtDir).resolve("20220101").resolve("AB_BN_2019.csv").toFile)
    TestUtil.copyResourceToFile(resourceFile, tempDir.resolve(tgtDir).resolve("20220201").resolve(resourceFile).toFile)

    // setup DataObject
    val srcDO1 = SFtpFileRefDataObject("src1", tempDir.resolve(srcDir1).toString.replace('\\', '/'), connectionId = "con1", partitions = Seq("date","town","year"), partitionLayout = Some("%date%/AB_%town%_%year:[0-9]+%"))
    val tgtDO = SFtpFileRefDataObject("tgt1", tempDir.resolve(tgtDir).toString.replace('\\', '/'), connectionId = "con1", partitions = Seq("date","town","year"), partitionLayout = Some("%date%/AB_%town%_%year:[0-9]+%"))

    // src1 file transfer
    val filetransfer1 = new StreamFileTransfer(srcDO1, tgtDO)
    val srcFileRefs1 = srcDO1.getFileRefs(Seq())
    assert(srcFileRefs1.map(f => srcDO1.relativizePath(f.fullPath)).toSet==Set("20220101/AB_NYC_2019.csv2"))
    val tgtFileRefs1 = tgtDO.getFileRefs(Seq())
    assert(tgtFileRefs1.map(f => tgtDO.relativizePath(f.fullPath)).toSet == Set("20220101/AB_BN_2019.csv","20220101/AB_NYC_2019.csv1","20220201/AB_NYC_2019.csv"))
    val fileRefPairs1 = tgtDO.translateFileRefs(srcFileRefs1, Some("\\.csv.*".r)) // keep only filetype from source filename, the rest is handled as partition values...
    tgtDO.startWritingOutputStreams(fileRefPairs1.map(_.tgt.partitionValues))
    filetransfer1.exec(fileRefPairs1)
    val tgtFileRefs1after = tgtDO.getFileRefs(Seq(PartitionValues(Map("date"->"20220101","town"->"NYC","year"->"2019"))))
    assert(tgtFileRefs1after.map(f => tgtDO.relativizePath(f.fullPath)).toSet == Set("20220101/AB_NYC_2019.csv2"))
    val tgtFileRefsFinal = tgtDO.getFileRefs(Seq())
    assert(tgtFileRefsFinal.map(f => tgtDO.relativizePath(f.fullPath)).toSet == Set("20220101/AB_BN_2019.csv","20220101/AB_NYC_2019.csv2","20220201/AB_NYC_2019.csv"))
  }
}
