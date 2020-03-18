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
package io.smartdatalake.util.util

import java.nio.file.{Files, Paths}

import io.smartdatalake.util.misc.AclUtil
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.scalatest.OptionValues._
import org.scalatest.{BeforeAndAfter, FunSuite}

class AclUtilTest extends FunSuite with BeforeAndAfter {

  protected val fs: FileSystem = FileSystem.get(new Configuration())

  val tmpDirOnFS: java.nio.file.Path = Files.createTempDirectory("sdl_test")

  before {
    FileUtils.forceMkdir(tmpDirOnFS.toFile)
  }

  after {
    FileUtils.deleteDirectory(tmpDirOnFS.toFile)
  }

  test("Parent method returns None when there is no parent.") {
    assert(AclUtil.parent(Some(new Path("/"))) === None)
  }

  test("Parent method returns the root directory of a root subsirectory.") {
    val parentPath = AclUtil.parent(Some(new Path("/child")))
    assert(parentPath.value === new Path("/"))
  }

  test("Parent method returns the parent directory of the supplied path.") {
    val parentPath = AclUtil.parent(Some(new Path("/path/to/parent/child")))
    assert(parentPath.value === new Path("/path/to/parent/"))
  }

  test("Parent method works when child path contains a wildcard inside the last path element.") {
    val parentPath = AclUtil.parent(Some(new Path("/path/to/parent/child_*_suffix")))
    assert(parentPath.value === new Path("/path/to/parent"))
  }

  test("Parent method works when child path contains a wildcard at the beginning of the last path element.") {
    val parentPath = AclUtil.parent(Some(new Path("/path/to/parent/*_middle_suffix")))
    assert(parentPath.value === new Path("/path/to/parent"))
  }

  test("Parent method works when child path contains a wildcard at the end of the last path element.") {
    val parentPath = AclUtil.parent(Some(new Path("/path/to/parent/child_middle_*")))
    assert(parentPath.value === new Path("/path/to/parent"))
  }

  test("Parent of some path with wildcards exist") {
    val testPath: Path = new Path(Paths.get(tmpDirOnFS.toString, "data").toString)
    fs.mkdirs(testPath)

    val testFile = Path.mergePaths(testPath, new Path("some_test_path.xml.gz"))
    val writer: FSDataOutputStream = fs.create(testFile)
    writer.close()
    val wildcardPath = Path.mergePaths(testPath, new Path("some*path.xml.gz"))
    val exists = AclUtil.exists(fs, Some(wildcardPath))
    assert(exists === true)
  }

  test("Parent of root direcory") {
    val rootPath = Some(new Path("/"))
    val rootParentPath = AclUtil.parent(rootPath)
    assert(rootParentPath.isEmpty)
  }

  test("Traverse directoryUpTo some existing directory (user home)") {
    val path = Some(new Path("/user/app_datalake/integration/someapp"))
    val upperPath = AclUtil.traverseDirectoryUpTo("app_datalake", path)
    assert(upperPath.get == new Path("/user/app_datalake"))
  }

  test("Traverse directoryUpTo some existing directory (feed)") {
    val path = Some(new Path("/user/app_datalake/integration/someapp/somefeed/data"))
    val upperPath = AclUtil.traverseDirectoryUpTo("somefeed", path)
    assert(upperPath.get == new Path("/user/app_datalake/integration/someapp/somefeed"))
  }

  test("Traverse directoryUpTo some non existent directory") {
    val path = Some(new Path("/user/app_datalake/integration/someapp"))
    val upperPath = AclUtil.traverseDirectoryUpTo("non_existing", path)
    assert(upperPath.isEmpty)
  }

  test("Path contains Feed") {
    val path = Some(new Path("/user/app_datalake/integration/someapp/somefeed/data"))
    assert(AclUtil.pathContainsFeed(path, "somefeed"))
  }

  test("Path does not contain Feed") {
    val path = Some(new Path("/tmp/u123456/someapp"))
    assert(!AclUtil.pathContainsFeed(path, "randomfeed"))
  }

  test("Path to file exists") {
    val testFile: Path = new Path(Paths.get(tmpDirOnFS.toString, "test.csv").toString)
    val writer: FSDataOutputStream = fs.create(testFile)
    writer.writeUTF("test")
    writer.close()
    assert(AclUtil.exists(fs, Some(testFile)))
  }

  test("Path to dir exists") {
    val testPath: Path = new Path(Paths.get(tmpDirOnFS.toString, "test_path").toString)
    fs.mkdirs(testPath)
    assert(AclUtil.exists(fs, Some(testPath)))
  }

  test("Path to file does not exist") {
    assert(!AclUtil.exists(fs, Some(new Path("does-absolutely-not-exist.csv"))))
  }

  test("Path to file does not exist for None") {
    assert(!AclUtil.exists(fs, None))
  }

  test("Root dir has level 0") {
    val rootPath = "/"
    assert(AclUtil.getPathLevel(rootPath) == 0)
  }

  test("User dir has level 1") {
    val path1 = "/user"
    assert(AclUtil.getPathLevel(path1) == 1)

    val path2 = "/user/"
    assert(AclUtil.getPathLevel(path2) == 1)
  }

  test("User home dir has level 2") {
    val path1 = "/user/app_dir"
    assert(AclUtil.getPathLevel(path1) == 2)

    val path2 = "/user/app_dir/"
    assert(AclUtil.getPathLevel(path2) == 2)
  }

  test("ACL modify on  root dir is NOT allowed") {
    val path = "/user"
    assert(!AclUtil.isAclModifyAllowed(path))
  }

  test("ACL modify on /user dir is NOT allowed") {
    val path = "/user"
    assert(!AclUtil.isAclModifyAllowed(path))
  }

  test("ACL modify on user home dir is allowed") {
    val path = "/user/app_dir"
    assert(AclUtil.isAclModifyAllowed(path))
  }

  test("ACL overwrite on user home dir is NOT allowed") {
    val path = "/user/app_dir"
    assert(!AclUtil.isAclOverwriteAllowed(path))
  }

  test("ACL modify on stage dir is allowed") {
    val path = "/user/app_dir/stage"
    assert(AclUtil.isAclModifyAllowed(path))
  }

  test("ACL overwrite on stage dir is NOT allowed") {
    val path = "/user/app_dir/stage"
    assert(!AclUtil.isAclOverwriteAllowed(path))
  }

  test("ACL modify on source dir is allowed") {
    val path = "/user/app_dir/stage/somesource"
    assert(AclUtil.isAclModifyAllowed(path))
  }

  test("ACL overwrite on source dir is NOT allowed") {
    val path = "/user/app_dir/stage/somesource"
    assert(!AclUtil.isAclOverwriteAllowed(path))
  }

  test("ACL modify on feed dir is NOT allowed") {
    val path = "/user/app_dir/stage/somesource/somefeed"
    assert(!AclUtil.isAclModifyAllowed(path))
  }

  test("ACL overwrite on feed dir is allowed") {
    val path = "/user/app_dir/stage/somesource/somefeed"
    assert(AclUtil.isAclOverwriteAllowed(path))
  }
}
