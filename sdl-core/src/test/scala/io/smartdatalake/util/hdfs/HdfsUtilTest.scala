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

package io.smartdatalake.util.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Options.Rename
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.scalatest.FunSuite

import java.nio.file.Files

class HdfsUtilTest extends FunSuite {

  test("touch file") {
    val file = new Path("target/touch.me")
    implicit val filesystem: FileSystem = file.getFileSystem(new Configuration())
    HdfsUtil.touchFile(file)
    val stat1 = filesystem.getFileStatus(file)
    Thread.sleep(1000)
    HdfsUtil.touchFile(file)
    val stat2 = filesystem.getFileStatus(file)
    assert(stat1.getModificationTime != stat2.getModificationTime)
  }

  // this functionality is needed by SparkFileDataObject.compactPartitions
  test("move subdirectory into parent directory, keeping existing files in parent directory") {
    val tempDir = Files.createTempDirectory("hdfsUtil")
    val path = new Path(tempDir.toString)
    val tempPath = new Path(tempDir.toString, "temp/")
    val tempPathSubdir = new Path(tempPath, "test")
    implicit val filesystem: FileSystem = path.getFileSystem(new Configuration())
    filesystem.mkdirs(path)
    HdfsUtil.touchFile(new Path(path,"test1"))
    HdfsUtil.touchFile(new Path(tempPathSubdir,"test2"))
    filesystem.rename(tempPathSubdir, path)
    assert(filesystem.listStatus(tempPath).isEmpty)
    assert(filesystem.listStatus(path).map(_.getPath.getName).toSeq.sorted == Seq("temp", "test", "test1"))
    assert(filesystem.listStatus(new Path(path, "test")).map(_.getPath.getName).toSeq == Seq("test2"))
  }

}
