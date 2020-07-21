/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.python

import java.io.{File, PrintWriter}
import java.net.InetAddress
import java.nio.file.Files

import org.apache.spark.SparkUserAppException
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.python.PythonUtils
import org.apache.spark.deploy.PythonRunner
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.util.{RedirectThread, Utils}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Functions to execute Python-Spark code
 *
 * Copyright note: code is mostly copied from org.apache.spark.deploy.PythonRunner
 * adaptions are additional
 */
object PythonHelper extends Logging {

  /**
   * Execute python code within a given Spark context/session.
   * This executes python as a subprocess and then has it connect back to the py4j-Gateway.
   *
   * @param pythonCode python code as string.
   * @param entryPointObj py4j gateway entrypoint java object available in python code as gateway.entry_point.
   *                      This is used to transfer SparkContext to python and can hold additional custom parameters.
   *                      entryPointObj must at least implement trait SparkEntryPoint.
   */
  def exec[T<:SparkEntryPoint](entryPointObj: T, pythonCode: String) {
    val pythonFile = File.createTempFile("pythontransform",".py")
    val pyFiles = ""
    val otherArgs = Seq[String]()
    val session = entryPointObj.session
    val sparkConf = session.sparkContext.getConf
    val secret = Utils.createSecret(sparkConf)
    val pythonExec = sparkConf.get(PYSPARK_DRIVER_PYTHON)
      .orElse(sparkConf.get(PYSPARK_PYTHON))
      .orElse(sys.env.get("PYSPARK_DRIVER_PYTHON"))
      .orElse(sys.env.get("PYSPARK_PYTHON"))
      .getOrElse("python")

    // prepare tempfile with python code to execute
    val pw = new PrintWriter(pythonFile)
    pw.write(pythonCode)
    pw.close()
    pythonFile.deleteOnExit()

    // Format python file paths before adding them to the PYTHONPATH
    val formattedPythonFile = PythonRunner.formatPath(pythonFile.getAbsolutePath)
    val formattedPyFiles = resolvePyFiles(PythonRunner.formatPaths(pyFiles))

    // Launch a Py4J gateway server for the process to connect to; this will let it see our
    // Java system properties and such
    val localhost = InetAddress.getLoopbackAddress
    val gatewayServer = new py4j.GatewayServer.GatewayServerBuilder()
      .authToken(secret)
      .javaPort(0)
      .javaAddress(localhost)
      .callbackClient(py4j.GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret)
      .entryPoint(entryPointObj)
      .build()
    val thread = new Thread(new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions {
        gatewayServer.start()
      }
    })
    thread.setName("py4j-gateway-init")
    thread.setDaemon(true)
    thread.start()

    // Wait until the gateway server has started, so that we know which port it is bound to.
    // `gatewayServer.start()` will start a new thread and run the server code there, after
    // initializing the socket, so the thread started above will end as soon as the server is
    // ready to serve connections.
    thread.join()
    logInfo("py4j gateway started")

    // Build up a PYTHONPATH that includes the Spark assembly (where this class is), the
    // python directories in SPARK_HOME (if set), and any files in the pyFiles argument
    val pathElements = new ArrayBuffer[String]
    pathElements ++= formattedPyFiles
    pathElements += PythonUtils.sparkPythonPath
    pathElements += sys.env.getOrElse("PYTHONPATH", "")
    val pythonPath = PythonUtils.mergePythonPaths(pathElements: _*)

    // Launch Python process
    val builder = new ProcessBuilder((Seq(pythonExec, formattedPythonFile) ++ otherArgs).asJava)
    val env = builder.environment()
    env.put("PYTHONPATH", pythonPath)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    env.put("PYTHONUNBUFFERED", "YES") // value is needed to be set to a non-empty string
    env.put("PYSPARK_GATEWAY_PORT", "" + gatewayServer.getListeningPort)
    env.put("PYSPARK_GATEWAY_SECRET", secret)
    // pass conf spark.pyspark.python to python process, the only way to pass info to
    // python process is through environment variable.
    sparkConf.get(PYSPARK_PYTHON).foreach(env.put("PYSPARK_PYTHON", _))
    sys.env.get("PYTHONHASHSEED").foreach(env.put("PYTHONHASHSEED", _))
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    logInfo("starting python process")
    try {
      val process = builder.start()
      new RedirectThread(process.getInputStream, System.out, "redirect output").start()
      val exitCode = process.waitFor()
      if (exitCode != 0) {
        throw SparkUserAppException(exitCode)
      } else logInfo("python process ended successfully")
    } finally {
      gatewayServer.shutdown()
    }
  }

  /**
   * Resolves the ".py" files. ".py" file should not be added as is because PYTHONPATH does
   * not expect a file. This method creates a temporary directory and puts the ".py" files
   * if exist in the given paths.
   */
  private def resolvePyFiles(pyFiles: Array[String]): Array[String] = {
    lazy val dest = Utils.createTempDir(namePrefix = "localPyFiles")
    pyFiles.flatMap { pyFile =>
      // In case of client with submit, the python paths should be set before context
      // initialization because the context initialization can be done later.
      // We will copy the local ".py" files because ".py" file shouldn't be added
      // alone but its parent directory in PYTHONPATH. See SPARK-24384.
      if (pyFile.endsWith(".py")) {
        val source = new File(pyFile)
        if (source.exists() && source.isFile && source.canRead) {
          Files.copy(source.toPath, new File(dest, source.getName).toPath)
          Some(dest.getAbsolutePath)
        } else {
          // Don't have to add it if it doesn't exist or isn't readable.
          None
        }
      } else {
        Some(pyFile)
      }
    }.distinct
  }

  trait SparkEntryPoint {
    def session: SparkSession
    def getJavaSparkContext: JavaSparkContext = new JavaSparkContext(session.sparkContext)
    def getSQLContext: SQLContext = session.sqlContext
  }
}
