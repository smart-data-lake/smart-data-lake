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
package io.smartdatalake.util.python

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import jep.{Interpreter, JepConfig, MainInterpreter, PyConfig, SharedInterpreter}
import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, Formats}

import java.io.File
import java.util.concurrent.{Callable, ExecutionException, ExecutorService, Executors}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.{Failure, Success, Try}

/**
 * A Python interpreter embedded into the JVM with jep (Java Embedded Python).
 *
 * A jep interpreter can only be used by the thread which created it, but SDLB runs Actions in parallel threads.
 * JepInterpreter therefore owns a dedicated thread, which creates the interpreter and executes all calls to it one
 * after the other. Note that a function passed to [[exec]] must not call [[exec]] again, as this would deadlock.
 *
 * The native part of jep and the Python runtime can only be initialized once per JVM, so there is at most one
 * JepInterpreter per JVM, see [[JepInterpreter.get]].
 */
final class JepInterpreter private(val environment: PythonEnvironment) extends SmartDataLakeLogger {

  private val executor: ExecutorService = Executors.newSingleThreadExecutor { (runnable: Runnable) =>
    val thread = new Thread(runnable, "sdlb-python")
    thread.setDaemon(true)
    thread
  }

  private val interpreter: Interpreter = runOnThread(new SharedInterpreter())

  /**
   * Execute a function with the interpreter on the thread of the interpreter.
   */
  def exec[T](func: Interpreter => T): T = runOnThread(func(interpreter))

  private def runOnThread[T](func: => T): T = {
    if (executor.isShutdown) throw new IllegalStateException("JepInterpreter is closed")
    try executor.submit(new Callable[T] { override def call(): T = func }).get()
    catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private[python] def close(): Unit = synchronized {
    if (!executor.isShutdown) {
      Try(runOnThread(interpreter.close())).failed.foreach(e => logger.warn(s"Closing Python interpreter failed: ${e.getMessage}"))
      executor.shutdown()
    }
  }
}

object JepInterpreter extends SmartDataLakeLogger {

  private var instance: Option[JepInterpreter] = None
  private var initializedEnvironment: Option[PythonEnvironment] = None

  /**
   * Get the JepInterpreter of this JVM, creating it on first use.
   *
   * @param pythonExecutable Python executable of the environment to use. It is only considered on first use, as the
   *                         Python runtime can only be initialized once per JVM.
   *                         Default is Environment.pythonPath (environment variable SDL_PYTHON_PATH), or python3
   *                         on the PATH.
   */
  def get(pythonExecutable: Option[String] = None): JepInterpreter = synchronized {
    instance.getOrElse {
      val environment = initializedEnvironment.getOrElse {
        val env = PythonEnvironment.probe(resolvePythonExecutable(pythonExecutable))
        initializeMainInterpreter(env)
        initializedEnvironment = Some(env)
        env
      }
      if (pythonExecutable.exists(_ != environment.executable)) {
        logger.warn(s"Python runtime is already initialized with ${environment.executable}, ignoring $pythonExecutable")
      }
      val newInstance = new JepInterpreter(environment)
      instance = Some(newInstance)
      newInstance
    }
  }

  /**
   * True if a Python environment with jep can be found and the interpreter could be created.
   */
  def isAvailable: Boolean = unavailableReason.isEmpty

  /**
   * The reason why the Python interpreter can not be created, or None if it is available.
   */
  def unavailableReason: Option[String] = synchronized {
    Try(get()) match {
      case Success(_) => None
      case Failure(e) =>
        logger.info(s"Python interpreter with jep is not available: ${e.getMessage}")
        Some(e.getMessage)
    }
  }

  /**
   * Close the interpreter of this JVM. A new interpreter is created by the next call to [[get]], but the Python
   * runtime stays initialized with the same environment.
   */
  def close(): Unit = synchronized {
    instance.foreach(_.close())
    instance = None
  }

  private def resolvePythonExecutable(pythonExecutable: Option[String]): String = {
    val isWindows = sys.props.get("os.name").exists(_.toLowerCase.startsWith("windows"))
    pythonExecutable
      .orElse(Environment.pythonPath)
      .getOrElse(if (isWindows) "python" else "python3")
  }

  private def initializeMainInterpreter(env: PythonEnvironment): Unit = {
    logger.info(s"Initializing Python runtime of ${env.executable} with jep ${env.jepLibrary}")
    // jep's native library is linked against libpython, which is normally not on the library path of the JVM.
    // Loading it upfront by its absolute path satisfies this dependency.
    env.libPython.foreach(System.load)
    MainInterpreter.setInitParams(PyConfig.python().setHome(env.home))
    MainInterpreter.setJepLibraryPath(env.jepLibrary)
    SharedInterpreter.setConfig(new JepConfig().addIncludePaths(env.path: _*))
  }
}

/**
 * The paths of a Python environment needed to embed it with jep.
 *
 * @param executable Python executable of the environment
 * @param home       Python home, the installation directory of the Python runtime (sys.base_prefix)
 * @param jepLibrary absolute path of the native library of jep
 * @param libPython  absolute path of the shared Python library, if Python was built with it
 * @param path       module search path (sys.path) of the environment
 */
case class PythonEnvironment(executable: String, home: String, jepLibrary: String, libPython: Option[String], path: Seq[String])

object PythonEnvironment {

  private val probeCode =
    """import importlib.util, json, os, sys, sysconfig
      |# jep can not be imported outside of an embedded interpreter, only locate it
      |jep_spec = importlib.util.find_spec("jep")
      |if jep_spec is None:
      |    sys.exit("Python package jep is not installed")
      |if importlib.util.find_spec("sqlglot") is None:
      |    sys.exit("Python package sqlglot is not installed")
      |jep_dir = list(jep_spec.submodule_search_locations)[0]
      |jep_libs = [os.path.join(jep_dir, f) for f in ("libjep.so", "libjep.jnilib", "libjep.dylib", "jep.dll")]
      |if os.name == "nt":
      |    lib_python = os.path.join(sys.base_prefix, f"python{sys.version_info.major}{sys.version_info.minor}.dll")
      |elif sysconfig.get_config_var("Py_ENABLE_SHARED"):
      |    lib_python = os.path.join(sysconfig.get_config_var("LIBDIR"), sysconfig.get_config_var("INSTSONAME") or sysconfig.get_config_var("LDLIBRARY"))
      |else:
      |    lib_python = None
      |print(json.dumps({
      |    "home": sys.base_prefix,
      |    "jepLibrary": next((f for f in jep_libs if os.path.exists(f)), None),
      |    "libPython": lib_python if lib_python and os.path.exists(lib_python) else None,
      |    "path": [p for p in sys.path if p],
      |}))
      |""".stripMargin

  /**
   * Determine the paths of the Python environment by running its executable.
   */
  def probe(executable: String): PythonEnvironment = {
    implicit val formats: Formats = DefaultFormats
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    val exitCode = Try(Process(Seq(executable, "-c", probeCode)).!(ProcessLogger(l => stdout.append(l), l => stderr.append(l).append("\n"))))
    val hint = s"The SQL engine needs a Python environment with sqlglot and jep installed, see sdl-sql/pyproject.toml. " +
      s"Set the environment variable SDL_PYTHON_PATH to its Python executable, see Environment.pythonPath."
    exitCode match {
      case Success(0) =>
        val json = JsonMethods.parse(stdout.toString)
        val jepLibrary = (json \ "jepLibrary").extractOpt[String]
          .getOrElse(throw ConfigurationException(s"Native library of jep not found in Python environment of $executable. $hint"))
        PythonEnvironment(executable, (json \ "home").extract[String], jepLibrary, (json \ "libPython").extractOpt[String],
          (json \ "path").extract[Seq[String]].filter(new File(_).exists))
      case Success(code) =>
        throw ConfigurationException(s"Python environment of $executable is not usable (exit code $code): ${stderr.toString.trim}. $hint")
      case Failure(e) =>
        throw ConfigurationException(s"Python executable $executable could not be started: ${e.getMessage}. $hint", throwable = e)
    }
  }
}
