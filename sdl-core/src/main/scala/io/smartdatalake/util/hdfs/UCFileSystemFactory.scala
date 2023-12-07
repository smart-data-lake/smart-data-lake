/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.{CustomCodeUtil, SmartDataLakeLogger}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import java.lang.reflect.Method


/**
 * UC FileSystem factory wraps the Hadoop FileSystem created by DefaultFileSystemFactory with UCFileSystemWrapper.
 */
class UCFileSystemFactory extends FileSystemFactory {
  def getFileSystem(path: Path, hadoopConf: Configuration): FileSystem = {
    val dbUtils = UCFileSystemFactory.getDbUtils
    // register access before creating filesystem!
    dbUtils.registerPathAccess(path)
    // Remark: hadoop configuration is ignored by getFS. It uses defaults from Databricks Spark Session.
    dbUtils.getFS(path.toString)
  }
}

object UCFileSystemFactory {

  /**
   * Checks if Databricks classes exist on classpath
   */
  def isDatabricksEnv: Boolean = dbUtils.nonEmpty

  /**
   * Checks if Databricks FSUtils are configured for Unity Catalog.
   * Note: This seems to be an unreliable information, as it depends on wether and how a spark session is initialized.
   * @return
   */
  def needUcFileSystem: Boolean = dbUtils.exists(_.isUnityCatalogEnabled)

  def getDbUtils: DbUtilsInterface = UCFileSystemFactory.dbUtils
    .getOrElse(throw new RuntimeException("dbUtils is not defined"))

  // DbUtilsInterface can be instantiated only if classpath contains Databricks classes
  private[smartdatalake] lazy val dbUtils: Option[DbUtilsInterface] = try {
    val fsUtilsInst = CustomCodeUtil.getClassInstanceByName[Any]("com.databricks.backend.daemon.dbutils.FSUtils")
    val credentialScopeHelperClass = Environment.classLoader().loadClass("com.databricks.unity.CredentialScopeSQLHelper")
    Some(new DbUtilsInterface(fsUtilsInst, credentialScopeHelperClass))
  } catch {
    case _:ClassNotFoundException => None
  }
}

/**
 * Interface to dynamically access Databricks methods that only exists in class path on Databricks environment
 */
private[smartdatalake] class DbUtilsInterface(fsUtilsInst: Any, credentialScopeHelperClass: Class[_]) extends SmartDataLakeLogger {

  def checkPermissionAccess[X](pathAndActions: Seq[_], withUnityCatalog: java.lang.Boolean, code : => X): X = {
    checkPermissionMethod.invoke(fsUtilsInst, pathAndActions, withUnityCatalog, code _).asInstanceOf[X]
  }
  private lazy val checkPermissionMethod = getMethod(fsUtilsInst.getClass, "checkPermission", Seq(classOf[Seq[_]], classOf[Boolean], classOf[Function0[_]]))
  checkPermissionMethod.setAccessible(true)

  def getFS(path: String): FileSystem = {
    getFSMethod.invoke(fsUtilsInst, path).asInstanceOf[FileSystem]
  }
  private lazy val getFSMethod = getMethod(fsUtilsInst.getClass,"getFS", Seq(classOf[String]))
  getFSMethod.setAccessible(true)

  def isUnityCatalogEnabled: Boolean = {
    isUnityCatalogEnabledMethod.invoke(fsUtilsInst).asInstanceOf[java.lang.Boolean]
  }
  private lazy val isUnityCatalogEnabledMethod = getMethod(fsUtilsInst.getClass, "isUnityCatalogEnabled", Seq())
  isUnityCatalogEnabledMethod.setAccessible(true)

  def registerPathAccess(path: Path): Unit = {
    logger.info(s"register path access for $path")
    registerPathAccessMethod.invoke(null, path, None) // invoking static method
  }
  private lazy val registerPathAccessMethod: Method = getMethod(credentialScopeHelperClass, "registerPathAccess", Seq(classOf[Path], classOf[Option[_]]))

  /**
   * Helper method to log alternative methods with the same name before throwing NoSuchMethodException
   */
  private def getMethod(cls: Class[_], name: String, parameterTypes: Seq[Class[_]]) = {
    try {
      cls.getMethod(name, parameterTypes:_*)
    } catch {
      case e:NoSuchMethodException =>
        val otherMethodsWithSameName = cls.getMethods.filter(_.getName == "registerPathAccess")
        logger.warn(s"${e.getClass.getSimpleName}: ${e.getMessage}. Alternative methods with same name: ${otherMethodsWithSameName.map(_.toString).mkString(", ")}")
        throw e
    }
  }

}
