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

private[smartdatalake] object AclUtil extends SmartDataLakeLogger {


  private val BasicAclSpecApp = "user::rwx,group::r-x,other::---" // app
  private val BasicAclSpecLab = "user::rwx,group::rwx,other::---" // lab
  private val BasicAclSpecUser = "user::rwx,group::---,other::---" // user

  private val AppUserSubstring = "_app_" // bsp. fbd_t_app_datalake, fbd_app_datalake
  private val LabUserSubstring = "_lab_" // bsp. fbd_t_lab_datalake

  //
  // Threshholds for Hadoop path level, between these threshholds, the permission and ACLs may be overwritten
  //
  // Root /: level 0
  // User Dir on Hadoop /user, /user/: 1
  // User Home /user/lab_datalake: 2
  // Layer /user/lab_datalake/stage|integration|btl: 3
  // Source /user/lab_datalake/stage|integration|btl/<source_name>: 4
  // Feed /user/lab_datalake/stage|integration|btl/<source_name>/<feed_name>: 5
  //
  private val MinLevelPermissionOverwrite = 5 // incl. and underneath /user/lab_datalake/stage|integration|btl/<source_name>/<feed_name>
  private val MinLevelPermissionModify = 2 // up to user home /user/<lab|app|fbd_lab|fbd_app> oder /tmp/<app|lab>

  /**
   * Sets configured permissions and ACLs on files for each source (S) and feed (F)
   * In Hadoop the directories are structured as followed:
   *
   * /user/labX|appX|uX
   * stage/S1
   * stage/S2
   * |-/F1
   * |-/...
   * |-/Fn
   * ...
   * stage/Sn
   * integration/S1
   * integration/S2
   * ...
   * integration/Sn
   * ...
   *
   * The ACLs are set on this hierarchy as followed:
   *
   * - /user/labX|appX|uX: modify
   * - /user/labX|appX|uX/stage|integration: modify
   * - /user/labX|appX|uX/stage|integration/Sn: modify
   * - /user/labX|appX|uX/stage|integration/Sn/Fn: overwrite recursively
   *
   * @param aclConfig with ACL entries and permissions
   * @param path Hadoop path on which the ACL's should be applied
   */
  def addACLs(aclConfig: AclDef, path: Path)(implicit fileSystem: FileSystem): Unit = {

    checkUserPath(currentUser, path.toString)

    if (exists(fileSystem, Some(path))) {
      logger.info(s"writing ACLs for path <$path> with config $aclConfig")
      val basicAcls = AclEntry.parseAclSpec(basicAclSpec(currentUser), true).asScala.toSeq
      val aclSpec = aclConfig.acls.map(_.getAclSpec).mkString(",")
      val configuredAcls = AclEntry.parseAclSpec(aclSpec, true).asScala.toSeq
      val combinedAcls = basicAcls ++ configuredAcls
      val permission = readPermission(aclConfig.permission)
      val modifyAclsOnPath = modifyAcls(fileSystem, combinedAcls, _: Option[Path])
      val setAclsAndPermissionsOnPath = overridePermissionAndAcl(fileSystem, permission, combinedAcls, _: Option[Path])

      // 1.) overwrite permissions on path
      setAclsAndPermissionsOnPath(Some(path))
      // 2.) overwrite permissions of childrens
      traverseDirectory(fileSystem, Some(path), setAclsAndPermissionsOnPath)
      // 3.) extend permissions of parents
      traverseDirectoryUpToHome(Some(path), modifyAclsOnPath)

      logger.debug(s"finished setting permissions and ACLs on $path")
    } else {
      logger.warn(s"Hadoop path $path does not exist, ACLs cannot be set.")
    }
  }

  /**
    *
    * @param path
    */
  def checkUserPath(currentUser: String, path: String): Unit = {
    val user = currentUser.replaceFirst("^fbd_", "").replaceFirst("^t_", "")
    assert(
      s"""\\/(fbd_|fbd_t_)?$user\\/""".r
        .unanchored
        .findFirstMatchIn(path)
        .isDefined, s"Permissions can only be set under hadoop Homedir, path=$path")
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
   * Set permissions and ACL on a specific path
   *
   * @param fileSystem       Hadoop filesystem
   * @param permission permission for the given path
   * @param aclList    ACL for the given path
   * @param path       Path of file or directory in Hadoop filesystem
   */
  def overridePermissionAndAcl(fileSystem: FileSystem, permission: FsPermission, aclList: Seq[AclEntry],
                               path: Option[Path]): Unit = {
    path match {
      case Some(p) =>
        logger.debug(s"setting permission and ACLs on files/directories under path: ${path.getOrElse("(none)")}")
        if (isAclOverwriteAllowed(p.toString)) {
              logger.debug(s"setting permission: $permission on file/directory: $p")
              fileSystem.setPermission(p, permission)
              logger.debug(s"setting ACL: $aclList on file/directory: $p")
              fileSystem.setAcl(p, aclList.asJava)
        } else {
          logger.debug(s"ACLs can't be overwritten on path '$p', Level: ${getPathLevel(p.toString)}")
        }
      case _ => logger.warn("ACLs can't be set")
    }
  }

  def modifyAcls(fileSystem: FileSystem, aclList: Seq[AclEntry],
                 path: Option[Path]): Unit = {
    path match {
      case Some(p) => logger.debug(s"modifying ACL: $aclList on path: ${path.getOrElse("(none)")}")
        if (isAclModifyAllowed(p.toString)) {
          logger.debug(s"setting ACL: $aclList on file/directory: $p")
          fileSystem.modifyAclEntries(normalizePath(fileSystem, p), aclList.asJava)
        }
        else {
          logger.debug(s"ACLs can't be extended on path '$p', Level: ${getPathLevel(p.toString)}")
        }
      case _ => logger.warn("ACLs can't be set")
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
   * @param filePath Path of files or directories in Hadoop filesystem
   */
  def traverseDirectory(fileSystem: FileSystem, filePath: Option[Path],
                        setPermissionsAndAcl: Option[Path] => Unit): Unit = {
    filePath match {
      case Some(fp) =>
        logger.debug(s"traversing: ${fp.toString}")
          fileSystem.listStatus(normalizePath(fileSystem, fp)).foreach {
            fileStatus => {
              // action for dir and files
              setPermissionsAndAcl(Some(fileStatus.getPath))
              if (fileStatus.isDirectory) traverseDirectory(fileSystem, Some(fileStatus.getPath), setPermissionsAndAcl)
            }
          }
      case None => logger.debug("Path <None> can not be traversed")
    }
  }

  /**
   * Traverses a directory hierarchy up until parentDir
   *
   * @param parentDir name of target directory
   * @param path      path of parentDir
   * @return path
   */
  @tailrec
  def traverseDirectoryUpTo(parentDir: String, path: Option[Path]): Option[Path] = {
    parent(path) match {
      case Some(p) if p.getName != parentDir =>
        logger.debug(s"<${p.getName}> != <$parentDir>")
        traverseDirectoryUpTo(parentDir, Some(p))
      case Some(p) if p.getName == parentDir =>
        logger.debug(s"<${p.getName}> == <$parentDir>")
        Some(p)
      case _ => None // for root dir '/' path.getParent will be null!
    }
  }

  /**
    * Traverses a directory structure up until reaching the user home and sets ACLs.
    * ACLs are only applied on directories and not on files!
    *
    * @param path                     Path from which the parent is accessed from
    * @param modifiyPermissionsAndAcl Partial function used for setting the ACLs on the current parent directory
    */
  @tailrec
  def traverseDirectoryUpToHome(path: Option[Path], modifiyPermissionsAndAcl: Option[Path] => Unit): Unit = {
    path match {
      case Some(p) =>
        if (p.toString != "/user" && p.toString != "/tmp") {
          logger.debug(s"<${p.getName}> != /user && != /tmp")
          modifiyPermissionsAndAcl(Option(p))
          traverseDirectoryUpToHome(Option(p.getParent), modifiyPermissionsAndAcl)
        }
      case _ => logger.debug(s"No path, traversing stopped")
    }
  }

  def parent(path: Option[Path]): Option[Path] = {
    path match {
      case Some(p) => Option(p.getParent)
      case _ => None
    }
  }

  def isWildcard(fileSystem: FileSystem, p: Path): Boolean = {
    logger.debug(s"isDirectory($p): ${fileSystem.isDirectory(p)}")
    if (fileSystem.isDirectory(p)) {
      false
    } else {
      logger.debug(s"isFile($p): ${fileSystem.isFile(p)}")
      if (fileSystem.isFile(p)) {
        false
      } else {
        val parent = new Path(s"$p/..")
        logger.debug(s"isDirectory($parent): ${fileSystem.isDirectory(parent)}")
        if (parent.toUri.getPath != "" && fileSystem.isDirectory(parent)) true else false
      }
    }
  }

  def exists(fileSystem: FileSystem, path: Option[Path]): Boolean = {
    path match {
      case Some(p) =>
        if (!isWildcard(fileSystem, p)) {
          fileSystem.exists(p)
        } else {
          val parent = new Path(s"$p/..")
          if (fileSystem.isDirectory(parent)) fileSystem.exists(parent) else false
        }
      case _ => false
    }
  }

  def pathContainsFeed(path: Option[Path], feed: String): Boolean = {
    path match {
      case Some(p) => p.toString.contains("/" + feed + "/")
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
  def getPathLevel(path: String): Int = {
    val p = if (path.endsWith("/")) path else path + "/"
    p.count(c => c == '/')-1
  }

  /**
    * Checks if it's allowed to write the ACLs on a given Hadoop path.
    * To do that, the current path level is calculated and compared to the constant MinLevelPermissionOverwrite.
    *
    * @param path Hadoop path to check
    * @return True if it's allowed to overwrite the ACLs on this level, otherwise false
    */
  def isAclOverwriteAllowed(path: String): Boolean = {
    getPathLevel(path) >= MinLevelPermissionOverwrite
  }

  /**
    * Checks if it's allowed to execute a modify of the ACLs on a given Hadoop path.
    * To do that, the current path level is calculated and compared to the constant MinLeveLPermissionModify and MinLevelPermissionOverwrite
    *
    * @param path Hadoop path to check
    * @return True if it's allowed to modify the ACLs on this level, otherwise false
    */
  def isAclModifyAllowed(path: String): Boolean = {
    getPathLevel(path) >= MinLevelPermissionModify && getPathLevel(path) < MinLevelPermissionOverwrite
  }

}
