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

import java.nio.file.{Files, Path}

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.BasicAuthMode
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.connection.SftpFileRefConnection
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.sshd.server.SshServer
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite, Matchers}

class SFtpFileRefDataObjectTest extends FunSuite with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  implicit val session: SparkSession = TestUtil.sessionHiveCatalog
  implicit val registry: InstanceRegistry = new InstanceRegistry

  private var sshd: SshServer = _
  val sshPort = 8001
  val sshUser = "test"
  val sshPwd = "test"

  var tempDir: Path = _

  override protected def beforeAll(): Unit = {
    sshd = TestUtil.setupSSHServer(sshPort, sshUser, sshPwd)
  }

  override protected def afterAll(): Unit = {
    sshd.stop()
  }

  override def beforeEach(): Unit = {
    registry.clear()
    registry.register(SftpFileRefConnection( "con1", "localhost", sshPort, BasicAuthMode("CLEAR#"+sshUser, "CLEAR#"+sshPwd), ignoreHostKeyVerification = true))
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
      , partitionLayout = Some("AB_%town%_%year%" ))
    val partitionValuesExpected = Seq(PartitionValues(Map("town" -> "NYC", "year" -> "2019")))

    // list all files and extract partitions
    val fileRefsAll = sftpDO.getFileRefs(Seq())
    assert(fileRefsAll.size == 1)
    assert(fileRefsAll.head.fileName == resourceFile)
    assert(fileRefsAll.head.partitionValues.keys == Set("town","year"))

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
      , partitionLayout = Some("%date%/AB_%town%_%year%" ))
    val partitionValuesExpected = Seq(PartitionValues(Map("date" -> "20190101", "town" -> "NYC", "year" -> "2019")))

    // list all files and extract partitions
    val fileRefsAll = sftpDO.getFileRefs(Seq())
    assert(fileRefsAll.size == 1)
    assert(fileRefsAll.head.fileName == resourceFile)
    assert(fileRefsAll.head.partitionValues.keys == Set("date","town","year"))

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
}
