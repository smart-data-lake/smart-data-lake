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
package io.smartdatalake.util.filetransfer

import io.smartdatalake.util.misc.SmartDataLakeLogger
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.sftp.{OpenMode, RemoteFile, SFTPClient, SFTPException}
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.xfer.LocalDestFile
import org.apache.hadoop.fs.FileSystem

import java.io.{InputStream, OutputStream}
import java.net.{InetAddress, Proxy, Socket}
import java.util
import javax.net.SocketFactory
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.util.Try

private[smartdatalake] object SshUtil extends SmartDataLakeLogger {

  def connectWithUserPw(host: String, port: Int, user: String, password: String, proxy: Option[Proxy] = None, ignoreHostKeyValidation: Boolean = false): SSHClient = {
    val ssh = connect(host, port, proxy, ignoreHostKeyValidation)
    logger.info(s"host connected, trying to authenticate by user/password...")
    ssh.authPassword(user, password)
    logger.info(s"SSH authentication by user/password successful.")
    // return
    ssh
  }

  def connectWithPublicKey(host: String, port: Int, user: String, proxy: Option[Proxy] = None, ignoreHostKeyValidation: Boolean = false): SSHClient = {
    val ssh = connect(host, port, proxy, ignoreHostKeyValidation)
    logger.info(s"host connected, trying to authenticate by public key...")
    ssh.authPublickey(user)
    logger.info(s"SSH authentication by public key successful.")
    //return
    ssh
  }

  private def connect(host: String, port: Int, proxy: Option[Proxy] = None, ignoreHostKeyValidation: Boolean): SSHClient = {
    val ssh = new SSHClient()
    // disable host key validation
    if (ignoreHostKeyValidation) ssh.addHostKeyVerifier(new PromiscuousVerifier)
    // or load them from known hosts file
    else ssh.loadKnownHosts()
    // configure proxy if defined
    proxy.foreach(p => ssh.setSocketFactory(new ProxySocketFactory(p)))
    // connect
    logger.info(s"connecting to host $host by ssh, proxy=$proxy")
    ssh.connect(host, port)
    // return
    ssh
  }

  /**
   * Lists ftp files with wildcards (globs)
   * Note: sftp.ls doesn't support globs
   * @param path with globs to be listed
   * @param sftp client
   * @return
   */
  def sftpListFiles(path: String)(implicit sftp: SFTPClient): Seq[String] = {
    def splitPathElements(path: String): (String, Seq[String]) = {
      val pathElements = if (path.endsWith("/")) path.split("/"):+"*" else path.split("/")
      val basePath = pathElements.takeWhile(e => !e.contains('*')).mkString("/")
      val childPathElements = pathElements.dropWhile(e => !e.contains('*'))
      (basePath, childPathElements)
    }
    def lsGlobElement( basePath: String, globPathElementsResolved: Seq[String], globPathElementsTodo: Seq[String] ): Seq[String] = {
      val path = (basePath+:globPathElementsResolved).mkString("/")
      val globPathElementPattern = "^"+globPathElementsTodo.head.replaceAll("([\\^$\\.])","\\\\$1").replace("*",".*")+"$"
      val newglobPathElementsTodo = globPathElementsTodo.drop(1)
      val fileAttrs = try {
        sftp.ls(path).asScala.filter( _.getName.matches(globPathElementPattern))
      } catch {
        // do not fail if directory is missing
        case e:SFTPException if e.getMessage == "No such file or directory" =>
          logger.warn(s"no such file or directory $path")
          Seq()
      }
      if (newglobPathElementsTodo.isEmpty) {
        fileAttrs.filter(!_.isDirectory)
          .map( a => (globPathElementsResolved :+ a.getName).mkString("/")).toSeq
      }
      else {
        fileAttrs.filter(_.isDirectory)
          .flatMap( f => lsGlobElement( basePath, globPathElementsResolved :+ f.getName, newglobPathElementsTodo )).toSeq
      }
    }
    val (basePath, childPathElements) = splitPathElements(path)
    lsGlobElement( basePath, Seq(), childPathElements).map( f => basePath + "/" + f )
  }

  def sftpCopyFileToHDFS(srcPath: String, tgtDir: String, deleteSource: Boolean, overwrite: Boolean)(implicit sftp: SFTPClient, hdfs: FileSystem): Seq[String] = {

    logger.info(s"sftpCopyFileToHDFS: $srcPath -> $tgtDir")

    // stats collector - entries are added from subclass HDFSFile
    val copiedFiles = mutable.ListBuffer[String]()

    // start transfer
    sftp.getFileTransfer.download(srcPath, new HDFSFile(tgtDir, copiedFiles, overwrite))

    // delete source
    if (deleteSource) sftp.rm(srcPath)

    // return
    copiedFiles.toSeq
  }

  def getInputStream(path: String, onCloseFunc: () => Unit)(implicit sftp: SFTPClient): InputStream = {
    import net.schmizz.sshj.sftp.{OpenMode, RemoteFile}

    import java.util
    val stat = sftp.stat(path)
    val handle: RemoteFile = sftp.open(path, util.EnumSet.of(OpenMode.READ))
    new handle.RemoteFileInputStream() {
      override def close(): Unit = try {
        super.close()
      } finally {
        Try(handle.close()) // closing input stream must also close remote file
        onCloseFunc() // call additional close hook
      }
    }
  }

  def getOutputStream(path: String, overwrite: Boolean, onCloseFunc: () => Unit)(implicit sftp: SFTPClient): OutputStream = {
    val handle: RemoteFile = sftp.open(path, util.EnumSet.of(OpenMode.WRITE, OpenMode.CREAT, if (overwrite) OpenMode.TRUNC else OpenMode.APPEND))
    new handle.RemoteFileOutputStream() {
      override def close(): Unit = try {
        super.close()
      } finally {
        handle.close() // closing output stream must also close remote file
        onCloseFunc() // call additional close hook
      }
    }
  }

  // Custom HDFS destination file implementation. This allows to write directly to HDFS Filesystem.
  private class HDFSFile(filename: String, copiedFiles: mutable.Buffer[String], overwrite: Boolean)(implicit hdfs: FileSystem) extends LocalDestFile {
    import org.apache.hadoop.fs.Path
    import org.apache.hadoop.fs.permission.FsPermission

    import java.io.IOException

    // prepare output stream
    val path = new Path(filename)

    // override methods
    override def getChild(file: String): HDFSFile = {
      val childFilename = filename + (if (!filename.last.equals('/')) "/" else "") + file
      new HDFSFile(childFilename, copiedFiles, overwrite)
    }

    override def getOutputStream: java.io.OutputStream = {
      getOutputStream(false)
    }

    override def getOutputStream(append: Boolean): OutputStream = {
      // update stats
      copiedFiles.append(filename)
      if (append) hdfs.append(path)
      else hdfs.create(path, overwrite)
    }

    override def getLength: Long = {
      hdfs.getFileStatus(path).getLen
    }

    override def getTargetDirectory(dirname: String): LocalDestFile = {
      val tgtDir = getChild(dirname)
      // check parent directory
      if (hdfs.exists(path) && hdfs.isFile(path)) throw new IOException(path + " existiert bereits als file")
      // check target directory
      if (hdfs.exists(tgtDir.path) && hdfs.isFile(tgtDir.path)) throw new IOException(tgtDir.path + " existiert bereits als file")
      // create directories if missing
      if (!hdfs.mkdirs(tgtDir.path)) throw new IOException("Fehler beim Erstellen des directory " + tgtDir.path)
      tgtDir
    }

    override def getTargetFile(filename: String): LocalDestFile = {
      val tgtFile = getChild(filename)
      // create parent directory if missing
      //if (!fs.exists(path)) println( s"creating directory $path" )
      if (!hdfs.exists(path) && !hdfs.mkdirs(path)) throw new IOException("Fehler beim Erstellen des directory " + path)
      // check target file
      if (hdfs.exists(tgtFile.path) && hdfs.isDirectory(tgtFile.path)) throw new IOException("Ein directory mit demselben Namen existiert bereits")
      tgtFile
    }

    override def setLastAccessedTime(atime: Long): Unit = {/*NOP*/} //hdfs.setTimes(path, -1, atime) // access time is not relevant on hdfs

    override def setLastModifiedTime(mtime: Long): Unit = hdfs.setTimes(path, mtime, mtime) // set access time the same as modification time

    override def setPermissions(perms: Int): Unit = hdfs.setPermission(path, new FsPermission(perms.toShort))
  }

  /**
   * Socket factory needed for using proxy with sshj
   */
  class ProxySocketFactory(proxy: Proxy) extends SocketFactory {
    override def createSocket = new Socket(proxy)
    override def createSocket(host: String, port: Int) = throw new RuntimeException("not implemented")
    override def createSocket(address: InetAddress, port: Int) = throw new RuntimeException("not implemented")
    override def createSocket(host: String, port: Int, clientAddress: InetAddress, clientPort: Int) = throw new RuntimeException("not implemented")
    override def createSocket(address: InetAddress, port: Int, clientAddress: InetAddress, clientPort: Int) = throw new RuntimeException("not implemented")
  }
}