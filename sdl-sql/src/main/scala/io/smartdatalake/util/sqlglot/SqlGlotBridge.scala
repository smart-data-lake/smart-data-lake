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
package io.smartdatalake.util.sqlglot

import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.python.JepInterpreter
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{DefaultFormats, Extraction, Formats, JNothing, JNull, JValue}

import java.lang.ref.Cleaner
import java.util.concurrent.ConcurrentLinkedQueue
import scala.io.Source

/**
 * Information about a DataFrame in the Python registry of the bridge.
 *
 * @param id      id of the DataFrame in the registry
 * @param columns column names of the DataFrame
 * @param alias   alias of the DataFrame, used to reference its columns in a join condition
 */
case class DataFrameInfo(id: Long, columns: Seq[String], alias: String)

/**
 * A field of the schema inferred by SQLGlot. `dataType` is a JSON object with either `type` (SQL of a simple type),
 * `struct` (list of fields), `array` (element type) or `map` (list of key and value type).
 */
case class SqlGlotField(name: String, dataType: JValue)

/**
 * Exception raised by SQLGlot, with the Python traceback.
 */
class SqlGlotException(message: String, val pythonTraceback: String) extends RuntimeException(message)

/**
 * Remote control of SQLGlot DataFrames, which are created and manipulated in the embedded Python interpreter.
 * See sdlb_sql/bridge.py for the Python side.
 *
 * DataFrames are released in Python when the corresponding Scala object is garbage collected, see [[registerForRelease]].
 */
class SqlGlotBridge private(interpreter: JepInterpreter) extends SmartDataLakeLogger {

  private implicit val formats: Formats = DefaultFormats

  // ids of DataFrames to be released, sent to Python with the next call
  private val pendingReleases = new ConcurrentLinkedQueue[java.lang.Long]()

  // Note that the source must be read on this thread: SqlGlotBridge.get holds the lock of the companion object, and
  // initializing its lazy val on the thread of the interpreter would deadlock.
  private val source = SqlGlotBridge.bridgeSource
  interpreter.exec { interp =>
    interp.set("_sdlb_bridge_source", source)
    interp.exec(
      """import sys, types
        |_sdlb_bridge = types.ModuleType("sdlb_sql.bridge")
        |_sdlb_bridge.__file__ = "sdlb_sql/bridge.py"
        |sys.modules["sdlb_sql.bridge"] = _sdlb_bridge
        |exec(compile(_sdlb_bridge_source, _sdlb_bridge.__file__, "exec"), _sdlb_bridge.__dict__)
        |_sdlb_call = _sdlb_bridge.call
        |del _sdlb_bridge_source
        |""".stripMargin)
  }

  /**
   * Call an operation of the Python bridge, and return its result.
   */
  def call(op: String, args: (String, Any)*): JValue = {
    val releases = Iterator.continually(pendingReleases.poll()).takeWhile(_ != null).map(_.longValue).toSeq
    val argsJson = Serialization.write(Extraction.decompose(args.toMap))
    val responseJson = interpreter.exec { interp =>
      if (releases.nonEmpty) interp.invoke("_sdlb_call", "release", Serialization.write(Map("ids" -> releases)))
      interp.invoke("_sdlb_call", op, argsJson).asInstanceOf[String]
    }
    val response = JsonMethods.parse(responseJson)
    (response \ "error") match {
      case JNothing | JNull => response \ "result"
      case error =>
        val traceback = (response \ "traceback").extractOpt[String].getOrElse("")
        logger.debug(s"SQLGlot operation $op failed: $traceback")
        throw new SqlGlotException(s"SQLGlot operation $op failed: ${error.extract[String]}", traceback)
    }
  }

  /**
   * Call an operation of the Python bridge creating a DataFrame.
   */
  def callDataFrame(op: String, args: (String, Any)*): DataFrameInfo = call(op, args: _*).extract[DataFrameInfo]

  /**
   * Release the Python DataFrame with the given id when `owner` is garbage collected.
   */
  def registerForRelease(owner: AnyRef, id: Long): Unit = {
    SqlGlotBridge.cleaner.register(owner, () => pendingReleases.add(java.lang.Long.valueOf(id)))
  }

  // typed operations

  def schema(df: Long): Seq[SqlGlotField] = call("schema", "df" -> df).children.map { f =>
    SqlGlotField((f \ "name").extract[String], f \ "type")
  }

  def toSql(df: Long, dialect: Option[String], optimized: Boolean, pretty: Boolean): String =
    call("to_sql", "df" -> df, "dialect" -> dialect, "optimized" -> optimized, "pretty" -> pretty).extract[String]

  def registerView(name: String, df: Long): Unit = call("register_view", "name" -> name, "df" -> df)

  def transpile(sql: String, read: Option[String], write: Option[String]): String =
    call("transpile", "sql" -> sql, "read" -> read, "write" -> write).extract[String]

  /**
   * Release all DataFrames, tables and views.
   */
  def reset(): Unit = {
    pendingReleases.clear()
    call("reset")
  }
}

object SqlGlotBridge {

  private[sqlglot] val cleaner: Cleaner = Cleaner.create()

  private lazy val bridgeSource: String = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream("sdlb_sql/bridge.py"))
      .getOrElse(throw new IllegalStateException("Python code sdlb_sql/bridge.py not found on classpath"))
    val source = Source.fromInputStream(stream, "UTF-8")
    try source.mkString finally source.close()
  }

  private var instance: Option[(JepInterpreter, SqlGlotBridge)] = None

  /**
   * Get the bridge of the JepInterpreter of this JVM, see [[JepInterpreter.get]].
   */
  def get(pythonExecutable: Option[String] = None): SqlGlotBridge = synchronized {
    val interpreter = JepInterpreter.get(pythonExecutable)
    instance.filter(_._1 eq interpreter).map(_._2).getOrElse {
      val bridge = new SqlGlotBridge(interpreter)
      instance = Some((interpreter, bridge))
      bridge
    }
  }

  /**
   * Close the bridge and the Python interpreter of this JVM, releasing all DataFrames.
   */
  def close(): Unit = synchronized {
    instance = None
    JepInterpreter.close()
  }
}
