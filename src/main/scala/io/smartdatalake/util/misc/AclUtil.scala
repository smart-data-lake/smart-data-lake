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
package io.smartdatalake.util.misc

import io.smartdatalake.definitions.Environment
import org.apache.hadoop.fs.permission.{AclEntry, FsPermission}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
 * Example ACL configuration
 *
 * ...
 * acl {
 *   permission="rwxr-x---"
 *   acls [
 *     {
 *       aclType="group"
 *       name="t_datalake_application"
 *       permission="r-x"
 *     }
 * ...
 */
private[smartdatalake] case class AclDef (permission: String, acls: Seq[AclElement])
private[smartdatalake] case class AclElement (aclType:String, name:String, permission: String) {
  /**
   * @return ACL specification as string, i.e. "group:t_datalake_application:r-x"
   */
  def getAclSpec: String = {
    s"$aclType:$name:$permission"
  }
}

/**
 * Utils for setting ACLs on Hadoop Filesystems
 *
 * Setting ACLs in the directory hierarchy is a sensitive task.
 * In SmartDataLake ACLs can be configured for HadoopFileDataObjects.
 * Setting ACLs on a given path of a DataObject has to
 * 1) overwrite ACLs on the directory and its subdirectories
 * 2) modify ACLs on the parent directories up to a certain level configured by hdfsAclMinLevelPermissionModify
 * Additional limitations for overwriting ACLs can be configured by hdfsAclMinLevelPermissionOverwrite
 *
 * By default, changing ACLs is also limited to the user home directory. This is configured by hdfsAclsLimitToUserHome and hdfsAclsUserHomeLevel.
 * It checks if path at hdfsAclsUserHomeLevel ends with username.
 *
 * A common structure for directories on HDFS is as follows:
 * hdfs://dfs.nameservices/user/app_dir/<layer>/<source_name>/<feed_name>
 *
 * Levels are counted as follows
 * Root hdfs://dfs.nameservices/ or also "/" -> 0
 * User Dir on Hadoop hdfs://dfs.nameservices/user -> 1
 * User Home hdfs://dfs.nameservices/user/app_dir -> 2
 * Layer hdfs://dfs.nameservices/user/app_dir/<layer> -> 3
 * Source hdfs://dfs.nameservices/user/app_dir/<layer>/<source_name> -> 4
 * Feed hdfs://dfs.nameservices/user/app_dir/<layer>/<source_name>/<feed_name> -> 5
 * ...
 *
 * In this structure good limitations are: MinLevelPermissionModify=2, MinLevelPermissionOverwrite=5
 */
private[smartdatalake] object AclUtil extends SmartDataLakeLogger {

  //TODO: make this more generic
  private val BasicAclSpecApp = "user::rwx,group::r-x,other::---" // app
  private val BasicAclSpecLab = "user::rwx,group::rwx,other::---" // lab
  private val BasicAclSpecUser = "user::rwx,group::---,other::---" // user
  private val AppUserSubstring = "_app_" // bsp. fbd_t_app_datalake, fbd_app_datalake
  private val LabUserSubstring = "_lab_" // bsp. fbd_t_lab_datalake

  /**
   * Sets configured permissions and ACLs on files respecting hdfsAclMinLevelPermissionModify and hdfsAclMinLevelPermissionOverwrite
   *
   * ACLs for everything in the given directory are overwritten
   * ACLs for parent directories are modified up to userHome and hdfsAclMinLevelPermissionModify
   *
   * @param aclConfig with ACL entries and permissions
   * @param path Hadoop path on which the ACL's should be applied
   */
  def addACLs(aclConfig: AclDef, path: Path)(implicit fileSystem: FileSystem): Unit = {

    // check if modification allowed
    if (Environment.hdfsAclsLimitToUserHome) checkUserPath(currentUser, path)
    require(getPathLevel(path) > Environment.hdfsAclsMinLevelPermissionOverwrite, s"ACLs can't be overwritten on path '$path', level=${getPathLevel(path)} because hdfsAclsMinLevelPermissionOverwrite=${Environment.hdfsAclsMinLevelPermissionOverwrite}")
    require(Environment.hdfsAclsMinLevelPermissionOverwrite >= Environment.hdfsAclsMinLevelPermissionModify, s"hdfsAclsMinLevelPermissionOverwrite (${Environment.hdfsAclsMinLevelPermissionOverwrite}) must be greater than or equal to hdfsAclsMinLevelPermissionModify (${Environment.hdfsAclsMinLevelPermissionModify})")

    if (exists(fileSystem, Some(path))) {
      logger.info(s"writing ACLs for path <$path> with config $aclConfig")
      val basicAcls = AclEntry.parseAclSpec(basicAclSpec(currentUser), true).asScala.toSeq
      val aclSpec = aclConfig.acls.map(_.getAclSpec).mkString(",")
      val configuredAcls = AclEntry.parseAclSpec(aclSpec, true).asScala.toSeq
      val combinedAcls = basicAcls ++ configuredAcls
      val permission = readPermission(aclConfig.permission)
      val modifyAclsOnPath = modifyAcls(fileSystem, combinedAcls, _: Path)
      val setAclsAndPermissionsOnPath = overridePermissionAndAcl(fileSystem, permission, combinedAcls, _: Path)

      // 1.) overwrite permissions on path
      setAclsAndPermissionsOnPath(path)
      // 2.) overwrite permissions of childrens
      traverseDirectory(fileSystem, path, setAclsAndPermissionsOnPath)
      // 3.) extend permissions of parents
      traverseDirectoryUp(path, Environment.hdfsAclsMinLevelPermissionModify, modifyAclsOnPath)

      logger.debug(s"finished setting permissions and ACLs on $path")
    } else {
      logger.warn(s"Hadoop path $path does not exist, ACLs cannot be set.")
    }
  }

  /**
    * Make sure that path is under user home.
    */
  def checkUserPath(currentUser: String, path: Path): Unit = {
    val userHome = extractPathLevel(path, Environment.hdfsAclsUserHomeLevel)
    // userHome or username might be pre/postfixed. Check is therefore if one contains the other and vice versa.
    require( userHome.contains(currentUser) || currentUser.contains(userHome), s"Permissions can only be set under hadoop Homedir if hdfsAclsLimitToUserHome is enabled, path=$path")
  }

  def extractPathLevel(path: Path, level: Int): String = {
    val pathLevel = getPathLevel(path)
    require(level<=pathLevel, s"Path ${path.toUri} is not defined for level $level")
    path.toUri.getPath.split("/")(level)
  }

  /**
   * Evaluates basic ACLs according to user name
   *
   * @param user name of current user
   * @return String with ACL specification user::<>,group::<>,other::<>
   */
  def basicAclSpec(user: String): String = {
    user match {
      case u if user.contains(AppUserSubstring) =>
        logger.debug(s"user <$u>: $BasicAclSpecApp")
        BasicAclSpecApp
      case u if user.contains(LabUserSubstring) =>
        logger.debug(s"user <$u>: $BasicAclSpecLab")
        BasicAclSpecLab
      case _ =>
        logger.debug(s"user <$user>: $BasicAclSpecUser")
        BasicAclSpecUser
    }
  }

  /**
   * Reads the current user from system properties
   *
   * @return User name as string
   */
  def currentUser: String = {
    System.getProperty("user.name")
  }

  /**
   * Create a FsPermission from a Unix symbolic permission string
   *
   * @param unixSymbolicPermission e.g. "-rw-rw-rw--" oder "rwxr-w---t"
   */
  def readPermission(unixSymbolicPermission: String): FsPermission = {
    FsPermission.valueOf(unixSymbolicPermission)
  }

  /**
   * Set (override) permissions and ACL on a specific path
   *
   * @param fileSystem       Hadoop filesystem
   * @param permission permission for the given path
   * @param aclList    ACL for the given path
   * @param path       Path of file or directory in Hadoop filesystem
   */
  def overridePermissionAndAcl(fileSystem: FileSystem, permission: FsPermission, aclList: Seq[AclEntry], path: Path): Unit = {
    if (isAclOverwriteAllowed(path)) {
      logger.debug(s"setting permission: $permission on file/directory: $path")
      fileSystem.setPermission(path, permission)
      logger.debug(s"setting ACL: $aclList on file/directory: $path")
      fileSystem.setAcl(path, aclList.asJava)
    } else {
      logger.debug(s"ACLs can't be overwritten on path '$path', Level: ${getPathLevel(path)}")
    }
  }

  def modifyAcls(fileSystem: FileSystem, aclList: Seq[AclEntry], path: Path): Unit = {
    if (isAclModifyAllowed(path)) {
      logger.debug(s"setting ACL: $aclList on file/directory: $path")
      fileSystem.modifyAclEntries(normalizePath(fileSystem, path), aclList.asJava)
    } else {
      logger.debug(s"ACLs can't be extended on path '$path', Level: ${getPathLevel(path)}")
    }
  }

  /**
    * Normalizes a path
    *
    * @param fileSystem   Hadoop filesystem
    * @param path   Path of file or directory in Hadoop filesystem
    * @return       If path has wildcards then path of parent directory, otherwise the input path
    */
  def normalizePath(fileSystem: FileSystem, path: Path): Path = {
    if (isWildcard(fileSystem, path)) {
      path.getParent
    } else {
      path
    }
  }

  /**
   * Traverses a directory tree recursively
   * On all files and directories, the given ACLs and permissions will be set.
   *
   * @param fileSystem     Hadoop filesystem
   * @param path Path of files or directories in Hadoop filesystem
   */
  def traverseDirectory(fileSystem: FileSystem, path: Path, setPermissionsAndAcl: Path => Unit): Unit = {
    logger.debug(s"traversing: ${path.toString}")
    fileSystem.listStatus(normalizePath(fileSystem, path)).foreach {
      fileStatus => {
        // action for dir and files
        setPermissionsAndAcl(fileStatus.getPath)
        if (fileStatus.isDirectory) traverseDirectory(fileSystem, fileStatus.getPath, setPermissionsAndAcl)
      }
    }
  }

  /**
    * Traverses a directory structure up until reaching min level given and sets ACLs.
    * ACLs are only applied on directories and not on files!
    *
    * @param path                     Path from which the parent is accessed from
    * @param minLevel minimum path level allowed to set ACLs (inclusive)
    * @param modifiyPermissionsAndAcl Partial function used for setting the ACLs on the current parent directory
    * @return
    */
  @tailrec
  def traverseDirectoryUp(path: Path, minLevel: Int, modifiyPermissionsAndAcl: Path => Unit): Path = {
    val pathLevel = getPathLevel(path)
    if (pathLevel >= minLevel) {
      modifiyPermissionsAndAcl(path)
    }
    if (pathLevel > minLevel) {
      val parentPath = parent(path)
      if (parentPath.isDefined) traverseDirectoryUp(parentPath.get, minLevel, modifiyPermissionsAndAcl)
      else path
    } else {
      path
    }
  }

  def parent(path: Path): Option[Path] = Option(path.getParent)

  def isWildcard(fileSystem: FileSystem, p: Path): Boolean = {
    logger.debug(s"isDirectory($p): ${fileSystem.isDirectory(p)}")
    if (fileSystem.isDirectory(p)) return false
    logger.debug(s"isFile($p): ${fileSystem.isFile(p)}")
    if (fileSystem.isFile(p)) return false
    val parentPath = parent(p)
    if (parentPath.isEmpty) return false
    if (parentPath.get.toUri.getPath.isEmpty) return false
    logger.debug(s"isDirectory($parentPath): ${fileSystem.isDirectory(parentPath.get)}")
    if (!fileSystem.isDirectory(parentPath.get)) return false
    // return
    true
  }

  def exists(fileSystem: FileSystem, path: Option[Path]): Boolean = {
    path match {
      case Some(p) =>
        if (!isWildcard(fileSystem, p)) {
          fileSystem.exists(p)
        } else {
          val parentPath = parent(p)
          if (parentPath.isDefined && fileSystem.isDirectory(parentPath.get)) fileSystem.exists(parentPath.get) else false
        }
      case _ => false
    }
  }

  /**
    * Calculates the level in the directory tree for a given path
    *
    * Root "/" -> 0
    * User "/user" or "/user/" -> 1
    *
    * @param path Path
    * @return level of folder in directory tree as Int
    */
  def getPathLevel(path: Path): Int = path.depth

  /**
    * Checks if it's allowed to write the ACLs on a given Hadoop path.
    * To do that, the current path level is calculated and compared to the constant MinLevelPermissionOverwrite.
    *
    * @param path Hadoop path to check
    * @return True if it's allowed to overwrite the ACLs on this level, otherwise false
    */
  def isAclOverwriteAllowed(path: Path): Boolean = {
    getPathLevel(path) >= Environment.hdfsAclsMinLevelPermissionOverwrite
  }

  /**
    * Checks if it's allowed to execute a modify of the ACLs on a given Hadoop path.
    * To do that, the current path level is calculated and compared to the constant MinLeveLPermissionModify
    *
    * @param path Hadoop path to check
    * @return True if it's allowed to modify the ACLs on this level, otherwise false
    */
  def isAclModifyAllowed(path: Path): Boolean = {
    getPathLevel(path) >= Environment.hdfsAclsMinLevelPermissionModify
  }

}
