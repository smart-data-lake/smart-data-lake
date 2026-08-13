/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.testutils.sparkconnect

import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.SparkSession

import java.io.File
import java.net.{InetSocketAddress, Socket, URI}
import scala.sys.process.Process
import scala.util.Try

/**
 * Utility to provide a Spark Connect server for tests.
 *
 * The server is resolved once per JVM in the following order:
 * 1. If env variable SPARK_CONNECT_URL is set, this externally managed server is used (never started/stopped by tests).
 * 2. If a server is already listening on the default url sc://localhost:15002, it is used (never stopped by tests).
 * 3. If the module script start-spark-connect.sh is found (in the working directory or subdirectory sdl-sparkconnect),
 *    it is used to start a local server with delta lake support, downloading a Spark distribution first if needed.
 *    The server is stopped again by a JVM shutdown hook. This is the default when executing "mvn test".
 * 4. If env variable SPARK_HOME is set, a local server is started with sbin/start-connect-server.sh
 *    (without delta lake support) and stopped again by a JVM shutdown hook.
 * Otherwise [[serverAvailable]] is false and tests needing a server should be cancelled with `assume`.
 *
 * Note that server availability is checked with a fast tcp probe, as the grpc client blocks with a long
 * retry policy if the server is not reachable.
 */
object SparkConnectTestUtil extends SmartDataLakeLogger {

  private val defaultPort = 15002

  /**
   * Url of the Spark Connect server to use for tests, from env variable SPARK_CONNECT_URL, default is sc://localhost:15002.
   */
  val url: String = sys.env.getOrElse("SPARK_CONNECT_URL", s"sc://localhost:$defaultPort")

  private val (host, port) = {
    val uri = new URI(url)
    (uri.getHost, if (uri.getPort > 0) uri.getPort else defaultPort)
  }

  /**
   * True if a Spark Connect server is reachable at [[url]], starting a local server if needed and possible.
   * Resolved once per JVM on first access.
   */
  lazy val serverAvailable: Boolean = {
    if (isPortOpen()) {
      logger.info(s"using running Spark Connect server at $url")
      true
    } else if (sys.env.contains("SPARK_CONNECT_URL")) {
      logger.warn(s"Spark Connect server configured by env variable SPARK_CONNECT_URL is not reachable at $url")
      false
    } else findModuleScript() match {
      case Some(script) => startWithModuleScript(script)
      case None => sys.env.get("SPARK_HOME") match {
        case Some(sparkHome) => startWithSparkHome(sparkHome)
        case None =>
          logger.info("no Spark Connect server running, start-spark-connect.sh not found and SPARK_HOME not set - tests needing a server will be cancelled")
          false
      }
    }
  }

  /**
   * True if a Spark Connect server is available and has delta lake support.
   * Delta lake support on the server side is needed for merge and partition operations,
   * as row-level operations are not supported for plain parquet tables. See also start-spark-connect.sh.
   */
  lazy val deltaAvailable: Boolean = serverAvailable && Try {
    SparkSession.builder().remote(url).getOrCreate()
      .conf.getOption("spark.sql.extensions").exists(_.contains("DeltaSparkSessionExtension"))
  }.getOrElse(false)

  /**
   * Name of the Iceberg catalog configured on the test server, see [[icebergAvailable]] and start-spark-connect.sh.
   */
  val icebergCatalog: String = "iceberg1"

  /**
   * True if a Spark Connect server is available and has Iceberg support, i.e. the Iceberg session extensions
   * are registered and the catalog [[icebergCatalog]] is configured. See also start-spark-connect.sh.
   */
  lazy val icebergAvailable: Boolean = serverAvailable && Try {
    val session = SparkSession.builder().remote(url).getOrCreate()
    session.conf.getOption("spark.sql.extensions").exists(_.contains("IcebergSparkSessionExtensions")) &&
      session.conf.getOption(s"spark.sql.catalog.$icebergCatalog").isDefined
  }.getOrElse(false)

  private def isPortOpen(timeoutMs: Int = 2000): Boolean = Try {
    val socket = new Socket()
    try socket.connect(new InetSocketAddress(host, port), timeoutMs)
    finally socket.close()
  }.isSuccess

  /**
   * Search the module script start-spark-connect.sh relative to the working directory.
   * When executing "mvn test" the working directory is the module directory,
   * when executing tests from the IDE it might be the repository root.
   */
  private def findModuleScript(): Option[File] = {
    Seq(new File("start-spark-connect.sh"), new File("sdl-sparkconnect/start-spark-connect.sh"))
      .find(_.isFile).map(_.getAbsoluteFile)
  }

  /**
   * Start a local Spark Connect server with delta lake support using the module script.
   * Note that the script downloads a Spark distribution into the module directory on first use.
   */
  private def startWithModuleScript(script: File): Boolean = {
    logger.info(s"starting local Spark Connect server using $script - this downloads a Spark distribution on first use")
    val exitCode = Process(Seq("bash", script.getPath)).! // the script waits for the server port itself
    if (exitCode != 0) {
      logger.warn(s"$script failed with exit code $exitCode")
      false
    } else {
      script.getParentFile.listFiles.find(f => f.isDirectory && f.getName.matches("spark-.*-bin-.*")) match {
        case Some(sparkHome) => registerStopServerOnShutdown(sparkHome.getPath, script.getParentFile)
        case None => logger.warn(s"Spark distribution directory not found in ${script.getParentFile}, Spark Connect server will not be stopped on JVM shutdown")
      }
      true
    }
  }

  /**
   * Start a local Spark Connect server from a Spark distribution given by SPARK_HOME.
   * Note that this server has no delta lake support.
   */
  private def startWithSparkHome(sparkHome: String): Boolean = {
    val startScript = new File(sparkHome, "sbin/start-connect-server.sh")
    if (!startScript.isFile) {
      logger.warn(s"can not start Spark Connect server, $startScript not found")
      return false
    }
    // use a working directory under target to keep spark-warehouse, metastore_db and derby.log out of the project root
    val workDir = new File("target/spark-connect-server").getAbsoluteFile
    workDir.mkdirs()
    logger.info(s"starting local Spark Connect server using $startScript in $workDir")
    val exitCode = Process(Seq(startScript.getPath), workDir, "SPARK_HOME" -> sparkHome).!
    if (exitCode != 0) {
      logger.warn(s"start-connect-server.sh failed with exit code $exitCode")
      return false
    }
    val available = waitForPort(timeoutSec = 60)
    if (available) registerStopServerOnShutdown(sparkHome, workDir)
    else logger.warn(s"Spark Connect server did not open port $port within 60s")
    available
  }

  private def waitForPort(timeoutSec: Int): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutSec * 1000L
    var open = isPortOpen(timeoutMs = 1000)
    while (!open && System.currentTimeMillis() < deadline) {
      Thread.sleep(1000)
      open = isPortOpen(timeoutMs = 1000)
    }
    open
  }

  private def registerStopServerOnShutdown(sparkHome: String, workDir: File): Unit = {
    sys.addShutdownHook {
      logger.info("stopping local Spark Connect server")
      Process(Seq(s"$sparkHome/sbin/stop-connect-server.sh"), workDir, "SPARK_HOME" -> sparkHome).!
    }
  }
}
