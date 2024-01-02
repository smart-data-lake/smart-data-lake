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
package io.smartdatalake.util.misc

import java.nio.file.{Files, Paths}

import io.smartdatalake.definitions.Environment
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
    assert(AclUtil.parent(new Path("/")) === None)
  }

  test("Parent method returns the root directory of a root subdirectory.") {
    val parentPath = AclUtil.parent(new Path("/child"))
    assert(parentPath.value === new Path("/"))
  }

  test("Parent method returns the parent directory of the supplied path.") {
    val parentPath = AclUtil.parent(new Path("/path/to/parent/child"))
    assert(parentPath.value === new Path("/path/to/parent/"))
  }

  test("Parent method works when child path contains a wildcard inside the last path element.") {
    val parentPath = AclUtil.parent(new Path("/path/to/parent/child_*_suffix"))
    assert(parentPath.value === new Path("/path/to/parent"))
  }

  test("Parent method works when child path contains a wildcard at the beginning of the last path element.") {
    val parentPath = AclUtil.parent(new Path("/path/to/parent/*_middle_suffix"))
    assert(parentPath.value === new Path("/path/to/parent"))
  }

  test("Parent method works when child path contains a wildcard at the end of the last path element.") {
    val parentPath = AclUtil.parent(new Path("/path/to/parent/child_middle_*"))
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

  test("Parent method returns the parent directory of the supplied path with scheme/authority.") {
    val parentPath = AclUtil.parent(new Path("hdfs://dfs.nameservices/path/to/parent/child"))
    assert(parentPath.value === new Path("hdfs://dfs.nameservices/path/to/parent/"))
  }

  test("Parent of root directory with schema/authority") {
    val rootPath = new Path("hdfs://dfs.nameservices/")
    val rootParentPath = AclUtil.parent(rootPath)
    assert(rootParentPath.isEmpty)
  }

  test("Parent of root directory") {
    val rootPath = new Path("/")
    val rootParentPath = AclUtil.parent(rootPath)
    assert(rootParentPath.isEmpty)
  }

  def noOpAclSetter(p:Path): Unit = ()

  test("Traverse directoryUp some existing directory (user home)") {
    val path = new Path("/user/app_dir/integration/someapp")
    val upperPath = AclUtil.traverseDirectoryUp(path, Environment.hdfsAclsUserHomeLevel, noOpAclSetter)
    assert(upperPath == new Path("/user/app_dir"))
  }

  test("Traverse directoryUp some existing directory (user home) with scheme/authority") {
    val path = new Path("hdfs://dfs.nameservices/user/app_dir/integration/someapp")
    val upperPath = AclUtil.traverseDirectoryUp(path, Environment.hdfsAclsUserHomeLevel, noOpAclSetter)
    assert(upperPath == new Path("hdfs://dfs.nameservices/user/app_dir"))
  }

  test("Traverse directoryUp some existing directory (feed)") {
    val path = new Path("/user/app_dir/integration/someapp/somefeed/data")
    val upperPath = AclUtil.traverseDirectoryUp(path, Environment.hdfsAclsUserHomeLevel, noOpAclSetter)
    assert(upperPath == new Path("/user/app_dir"))
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
    val rootPath = new Path("/")
    assert(AclUtil.getPathLevel(rootPath) == 0)
  }

  test("Root dir with scheme/authority has level 0") {
    val rootPath = new Path("hdfs://dfs.nameservices/")
    assert(AclUtil.getPathLevel(rootPath) == 0)
  }

  test("User dir has level 1") {
    val path1 = new Path("/user")
    assert(AclUtil.getPathLevel(path1) == 1)

    val path2 = new Path("/user/")
    assert(AclUtil.getPathLevel(path2) == 1)
  }

  test("User home dir has level 2") {
    val path1 = new Path("/user/app_dir")
    assert(AclUtil.getPathLevel(path1) == 2)

    val path2 = new Path("/user/app_dir/")
    assert(AclUtil.getPathLevel(path2) == 2)
  }

  test("User home dir with scheme/authority has level 2") {
    val path1 = new Path("hdfs://dfs.nameservices/user/app_dir")
    assert(AclUtil.getPathLevel(path1) == 2)

    val path2 = new Path("hdfs://dfs.nameservices/user/app_dir/")
    assert(AclUtil.getPathLevel(path2) == 2)
  }

  test("ACL modify on root dir is NOT allowed") {
    val path = new Path("/")
    assert(!AclUtil.isAclModifyAllowed(path))
  }

  test("ACL modify on root dir with scheme/authority is NOT allowed") {
    val path = new Path("hdfs://dfs.nameservices/")
    assert(!AclUtil.isAclModifyAllowed(path))
  }

  test("ACL modify on /user dir is NOT allowed") {
    val path = new Path("/user")
    assert(!AclUtil.isAclModifyAllowed(path))
  }

  test("ACL modify on /user with scheme/authority dir is NOT allowed") {
    val path = new Path("hdfs://dfs.nameservices/user")
    assert(!AclUtil.isAclModifyAllowed(path))
  }

  test("ACL modify on user home dir is allowed") {
    val path = new Path("/user/app_dir")
    assert(AclUtil.isAclModifyAllowed(path))
  }

  test("ACL modify on user home dir with scheme/authority is allowed") {
    val path = new Path("hdfs://dfs.nameservices/user/app_dir")
    assert(AclUtil.isAclModifyAllowed(path))
  }

  test("ACL overwrite on user home dir is NOT allowed") {
    val path = new Path("/user/app_dir")
    assert(!AclUtil.isAclOverwriteAllowed(path))
  }

  test("ACL overwrite on user home dir with scheme/authority is NOT allowed") {
    val path = new Path("hdfs://dfs.nameservices/user/app_dir")
    assert(!AclUtil.isAclOverwriteAllowed(path))
  }

  test("ACL modify on stage dir is allowed") {
    val path = new Path("/user/app_dir/stage")
    assert(AclUtil.isAclModifyAllowed(path))
  }

  test("ACL overwrite on stage dir is NOT allowed") {
    val path = new Path("/user/app_dir/stage")
    assert(!AclUtil.isAclOverwriteAllowed(path))
  }

  test("ACL modify on source dir is allowed") {
    val path = new Path("/user/app_dir/stage/somesource")
    assert(AclUtil.isAclModifyAllowed(path))
  }

  test("ACL overwrite on source dir is NOT allowed") {
    val path = new Path("/user/app_dir/stage/somesource")
    assert(!AclUtil.isAclOverwriteAllowed(path))
  }

  test("ACL overwrite on feed dir is allowed") {
    val path = new Path("/user/app_dir/stage/somesource/somefeed")
    assert(AclUtil.isAclOverwriteAllowed(path))
  }

  test("ACL overwrite on feed dir with scheme/authority is allowed") {
    val path = new Path("hdfs://dfs.nameservices/user/app_dir/stage/somesource/somefeed")
    assert(AclUtil.isAclOverwriteAllowed(path))
  }

  test("extract user home") {
    assert(AclUtil.extractPathLevel(new Path("hdfs://dfs.nameservices/user/app_dir"),Environment.hdfsAclsUserHomeLevel) == "app_dir")
    assert(AclUtil.extractPathLevel(new Path("hdfs://dfs.nameservices/user/app_dir/"),Environment.hdfsAclsUserHomeLevel) == "app_dir")
    assert(AclUtil.extractPathLevel(new Path("hdfs://dfs.nameservices/user/app_dir/test/abc"),Environment.hdfsAclsUserHomeLevel) == "app_dir")
    intercept[IllegalArgumentException](AclUtil.extractPathLevel(new Path("hdfs://dfs.nameservices/user/"),Environment.hdfsAclsUserHomeLevel) == "app_dir")
  }

  test("check hdfsAclsLimitToBasedir") {

    // check user home validation
    AclUtil.checkBasedirPath("app_dir", new Path("hdfs://dfs.nameservices/user/app_dir"))
    intercept[IllegalArgumentException](AclUtil.checkBasedirPath("app_other_dir", new Path("hdfs://dfs.nameservices/user/app_dir")))
    val hdfsBaseDirOrg = Environment.hdfsBasedir

    // check basedir valdiation
    Environment._hdfsBasedir = Some(Some(new Path("hdfs://dfs.nameservices/user/app_other_dir").toUri))
    AclUtil.checkBasedirPath("app_dir", new Path("hdfs://dfs.nameservices/user/app_other_dir"))
    intercept[IllegalArgumentException](AclUtil.checkBasedirPath("app_dir", new Path("hdfs://dfs.nameservices/user/app_dir")))
    Environment._hdfsBasedir = Some(hdfsBaseDirOrg) // revert environment config
  }
}
