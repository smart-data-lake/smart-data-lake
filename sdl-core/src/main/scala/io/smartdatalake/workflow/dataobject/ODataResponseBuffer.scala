/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer

/**
 * [[ODataResponseBufferSetup]] contains configurations related to the response buffer
 * @param tempFileDirectoryPath : Path to the temporary directory in which the temporary files can be stored
 * @param memoryToFileSwitchThresholdNumOfChars : Max number of character which can be stored in memory before switching to file buffering. (If set to 0 then no swichting will occur)
 */
case class ODataResponseBufferSetup(
                                       tempFileDirectoryPath: Option[String] = None
                                     , memoryToFileSwitchThresholdNumOfChars: Option[Long] = None
                                   ) {

  /* Late-Arriving property
   */
  private var actionName: String = ""
  def getActionName: String = this.actionName
  def setActionName(tableName: String): Unit = this.actionName = tableName

}

/**
 * [[ODataResponseBuffer]] is the base class for all other response buffer implementations
 * @param setup: The setup object which contains the configuration for this response buffer
 */
abstract class ODataResponseBuffer(setup: ODataResponseBufferSetup, context: ActionPipelineContext) {

  /**
   * Contains the number of characters already stored in this response buffer
   */
  protected var storedCharCount: Long = 0

  protected var responseCount: Int = 0

  /**
   * Adds the provided response to the buffer. This method must be overriden and called by any sub class
   * @param response : The response to be added
   */
  def addResponse(response: String): Unit = {
    storedCharCount += response.length
    responseCount += 1
  }

  def addResponses(responses: Iterable[String]): Unit = {
    responses.foreach(r => addResponse(r))
  }

  def getResponseCount : Int = {
    this.responseCount
  }

  /**
   * @return the number of characters stored in this buffer
   */
  def getStoredCharacterCount : Long = storedCharCount

  /**
   * Creates and returns a DataFrame referencing the stored responses. Each response is one record.
   * @return : The Dataframe referencing the stored responses
   */
  def getDataFrame: DataFrame

  /**
   * Deletes all buffered responses and leaves the buffer empty.
   */
  def cleanUp() : Unit = {
    this.storedCharCount = 0
  }

  /**
   * Switches this buffer to next stage and copies all already buffered responses, to the new instance.
   * @return The new response buffer instance.
   */
  def switchIfNecessary() : ODataResponseBuffer
}

/**
 * ResponseBuffer implementation which uses memory to store all responses.
 * @param setup: The setup object which contains the configuration for this response buffer
 */
class ODataResponseMemoryBuffer(setup: ODataResponseBufferSetup, context: ActionPipelineContext, ioc: ODataIOC) extends ODataResponseBuffer(setup, context) {

  /**
   * The buffered responses. Every response corresponds to one element in this array.
   */
  private var responses: ArrayBuffer[String] = ArrayBuffer.empty[String]

  /**
   * Adds a new response to the buffer
   * @param response : The response to be added
   */
  override def addResponse(response: String): Unit = {
    if (responses == null) {
      responses = ArrayBuffer(response)
    }
    else {
      responses.append(response)
    }
    super.addResponse(response)
  }

  /**
   * Creates a DataFrame based on the buffered responses
   *  @return : The Dataframe referencing the stored responses
   */
  override def getDataFrame: DataFrame = {
    val session = context.sparkSession
    import session.implicits._
    val dataFrame = responses.toSeq.toDF("responseString")
    dataFrame
  }

  override def cleanUp(): Unit = {
    responses.clear()
    super.cleanUp()
  }

  /**
   * Returns the array containing the buffered responses
   * @return the array containing the buffered responses
   */
  def getResponseBuffer : ArrayBuffer[String] = {
    responses
  }

  /**
   * Checks if switch threshold is defined and if it is reached. If so this method creates the new response buffer and
   * initializes it with the currently buffered responses. If no threshold is set or has not been reached yet, this
   * method returns the current instance with no further action.
   *  @return The old or new response buffer instance.
   */
  override def switchIfNecessary(): ODataResponseBuffer = {
    var result : ODataResponseBuffer = this
    if (setup != null){
      val dirPath = setup.tempFileDirectoryPath.getOrElse("")
      val threshold = setup.memoryToFileSwitchThresholdNumOfChars.getOrElse(-1L)

      if (dirPath != "" && threshold > 0L && this.getStoredCharacterCount > threshold) {

        result = ioc.newODataResponseFileBuffer(setup.getActionName, setup, context)
        result.addResponses(this.getResponseBuffer)
      }
    }
    result
  }
}





class ODataResponseFileBuffer(tableName: String, setup:ODataResponseBufferSetup, context: ActionPipelineContext, ioc: ODataIOC) extends ODataResponseBuffer(setup, context) {

  private var dirInitialized : Boolean = false
  private implicit val filesystem: FileSystem = ioc.newHadoopFsWithConf(ioc.newHadoopPath("//"), context)
  private val temporaryTargetDirectoryPath = ioc.newHadoopPath(setup.tempFileDirectoryPath.get, tableName + s"_${ioc.getInstantNow.getEpochSecond}")

  def initTemporaryDirectory(): Unit = {
    if (!dirInitialized) {
      clearTemporaryDirectory()
      makeTempDirIfNotExists()
      dirInitialized = true
    }
  }

  def getFileSystem : FileSystem = {
    filesystem
  }

  def makeTempDirIfNotExists() : Unit = {
    filesystem.mkdirs(temporaryTargetDirectoryPath)
  }

  /**
   * Deletes all buffered responses and leaves the buffer empty.
   */
  override def cleanUp(): Unit =
  {
    super.cleanUp()
    this.clearTemporaryDirectory()
  }

  def clearTemporaryDirectory() : Unit = {
    if (filesystem.exists(temporaryTargetDirectoryPath))
      filesystem.delete(temporaryTargetDirectoryPath, true)
  }

  def writeToFile(fileName: String, content: String) : Unit = {
    initTemporaryDirectory()
    ioc.writeHadoopFile(ioc.newHadoopPath(temporaryTargetDirectoryPath, fileName), content, filesystem)
  }

  def generateFileName() : String = {
    s"${this.getResponseCount}.json"
  }

  /**
   * Adds the provided response to the buffer.
   *
   * @param response : The response to be added
   */
  override def addResponse(response: String): Unit = {

    this.writeToFile(this.generateFileName(), response)

    super.addResponse(response)
  }

  /**
   * Creates and returns a DataFrame referencing the stored responses. Each response is one record.
   *
   * @return : The Dataframe referencing the stored responses
   */
  override def getDataFrame: DataFrame = {
    val session = context.sparkSession
    val dataFrame = session.read.option("wholetext", value = true).text(this.temporaryTargetDirectoryPath.toString).withColumnRenamed("value", "responseString")
    dataFrame
  }

  /**
   * Switches this buffer to next stage and copies all already buffered responses, to the new instance - if necessary
   *
   * @return The new response buffer instance.
   */
  override def switchIfNecessary(): ODataResponseBuffer = {
    this
  }
}