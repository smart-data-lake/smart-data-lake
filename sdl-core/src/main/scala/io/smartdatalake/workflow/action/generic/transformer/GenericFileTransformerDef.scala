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

package io.smartdatalake.workflow.action.generic.transformer

import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{ConfigHolder, ParsableFromConfig}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.customlogic.OutputStreamFactory

import java.io.{FilterOutputStream, InputStream, OutputStream}
import scala.collection.mutable
import scala.util.Try

/**
 * Interface to implement file transformers creating one or more output files from one input file, used by
 * CustomFileAction and FileTransferAction.
 *
 * The lifecycle of a file transformer is split between driver and executors:
 * [[prepareOptions]] and [[transformPartitionValues]] are called on the driver, where the ActionPipelineContext is
 * available, while [[transformToFiles]] is called for every file, on the executors for CustomFileAction.
 * Implementations must therefore be serializable.
 */
trait GenericFileTransformer extends PartitionValueTransformer with ParsableFromConfig[GenericFileTransformer] with ConfigHolder with Serializable {
  def name: String

  def description: Option[String] = None

  /**
   * Optional function to implement validations in prepare phase.
   */
  def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = ()

  /**
   * Prepare the options passed to [[transform]]. This is executed on the driver, once per Action execution.
   */
  def prepareOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String, String])(implicit
      context: ActionPipelineContext
  ): Map[String, String] = Map()

  /**
   * Function to be implemented to create one or more output files from an input stream.
   * Note that the streams are closed by the caller.
   *
   * @param options  Options prepared by [[prepareOptions]]
   * @param fileName default name of the output file, derived from the input file name
   * @param outputs  factory to create output files, see [[OutputStreamFactory]]
   * @return exception if something goes wrong. Other files are still processed, but the Action fails once all files are processed.
   */
  def transformToFiles(options: Map[String, String], input: InputStream, fileName: String, outputs: OutputStreamFactory): Option[Exception]
}

/**
 * Interface to implement file transformers with options.
 * This trait extends GenericFileTransformer to pass a map of options, including evaluated runtimeOptions, as parameter
 * to the transform and transformPartitionValues function. This is mainly used by custom transformers.
 */
trait OptionsFileTransformer extends GenericFileTransformer {
  def options: Map[String, String]
  def runtimeOptions: Map[String, String]

  /**
   * Optional function to define the transformation of input to output partition values.
   * @param options Options specified in the configuration for this transformation, including evaluated runtimeOptions
   */
  def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String, String])(implicit
      context: ActionPipelineContext
  ): Option[Map[PartitionValues, PartitionValues]] = None

  final override def transformPartitionValues(
      actionId: ActionId,
      partitionValues: Seq[PartitionValues],
      executionModeResultOptions: Map[String, String]
  )(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] =
    transformPartitionValuesWithOptions(actionId, partitionValues, prepareOptions(actionId, partitionValues, executionModeResultOptions))

  final override def prepareOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String, String])(implicit
      context: ActionPipelineContext
  ): Map[String, String] =
    options ++ evaluateRuntimeOptions(actionId, name, runtimeOptions, partitionValues) ++ executionModeResultOptions
}

object GenericFileTransformer {

  /**
   * Apply a file transformer to one input file.
   *
   * @param defaultFileName    default name of the output file
   * @param adaptFileName      function to make a file name created by the transformer match the output DataObject
   * @param createOutputStream function to create the output stream for a file name returned by adaptFileName
   * @return names of the output files created, and the exception if the transformation failed.
   *         It is also treated as failure if the transformer created no output file.
   */
  def transformToFiles(transformer: GenericFileTransformer, options: Map[String, String], input: InputStream, defaultFileName: String,
                       adaptFileName: String => String, createOutputStream: String => OutputStream): (Seq[String], Option[Exception]) = {
    val outputs = new TrackingOutputStreamFactory(adaptFileName, createOutputStream)
    val result = try {
      Try(transformer.transformToFiles(options, input, defaultFileName, outputs)).fold({
        case e: Exception => Some(e)
        case e => throw e
      }, identity)
    } finally {
      outputs.closeAll()
    }
    val error = result.orElse(
      if (outputs.fileNames.isEmpty) Some(new IllegalStateException(s"file transformer ${transformer.name} created no output file for $defaultFileName"))
      else None
    )
    (outputs.fileNames, error)
  }

  /**
   * Apply a file transformer to create a sample file of the output. Only the first output file created by the
   * transformation is written to the sample file, further output files are discarded.
   * @return exception if the transformation failed
   */
  def transformToSampleFile(transformer: GenericFileTransformer, options: Map[String, String], input: InputStream, defaultFileName: String,
                            createSampleOutputStream: () => OutputStream): Option[Exception] = {
    var sampleCreated = false
    val createOutputStream = (_: String) => synchronized {
      if (sampleCreated) OutputStream.nullOutputStream()
      else {
        sampleCreated = true
        createSampleOutputStream()
      }
    }
    transformToFiles(transformer, options, input, defaultFileName, identity, createOutputStream)._2
  }

  /**
   * Throw an exception listing the failed files, if any.
   * @param failedFiles tuples of input file and error message
   * @param nbOfFiles   number of files processed
   */
  def throwIfTransformationsFailed(failedFiles: Seq[(String, String)], nbOfFiles: Int): Unit = {
    if (failedFiles.nonEmpty) {
      val maxListed = 10
      val failedList = failedFiles.take(maxListed).map { case (file, error) => s"$file: $error" }.mkString("\n  ")
      val moreMsg = if (failedFiles.size > maxListed) s"\n  ... and ${failedFiles.size - maxListed} more" else ""
      throw new IllegalStateException(s"file transformation failed for ${failedFiles.size} of $nbOfFiles files:\n  $failedList$moreMsg")
    }
  }

  /**
   * OutputStreamFactory remembering the output files created, and closing their streams exactly once.
   */
  private class TrackingOutputStreamFactory(adaptFileName: String => String, createOutputStream: String => OutputStream) extends OutputStreamFactory {
    private val streams = mutable.LinkedHashMap[String, OutputStream]()

    override def create(fileName: String): OutputStream = synchronized {
      require(fileName.nonEmpty && !fileName.contains("/") && !fileName.contains("\\"),
        s"output file name must not be empty or contain a directory, but is '$fileName'")
      val adaptedFileName = adaptFileName(fileName)
      require(!streams.contains(adaptedFileName), s"output file $adaptedFileName was already created by this transformation")
      val os = new CloseOnceOutputStream(createOutputStream(adaptedFileName))
      streams.put(adaptedFileName, os)
      os
    }

    def fileNames: Seq[String] = streams.keys.toSeq

    def closeAll(): Unit = synchronized {
      val errors = streams.values.flatMap(os => Try(os.close()).failed.toOption)
      errors.headOption.foreach(e => throw e)
    }
  }

  private class CloseOnceOutputStream(out: OutputStream) extends FilterOutputStream(out) {
    private var closed = false
    override def write(b: Array[Byte], off: Int, len: Int): Unit = out.write(b, off, len)
    override def close(): Unit = if (!closed) {
      closed = true
      super.close()
    }
  }
}
