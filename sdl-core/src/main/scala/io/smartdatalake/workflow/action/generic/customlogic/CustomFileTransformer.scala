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

package io.smartdatalake.workflow.action.generic.customlogic

import io.smartdatalake.util.hdfs.PartitionValues

import java.io.{InputStream, OutputStream}

/**
 * Interface to define a custom file transformation for CustomFileAction and FileTransferAction.
 *
 * There are two methods to define the transformation:
 *
 * 1) Overwrite [[transform]] to transform one input file into one output file (1:1).
 *
 * 2) Overwrite [[transformToFiles]] to create multiple output files from one input file (1:n), e.g. to unzip an archive.
 *
 * Implementations must be serializable, as CustomFileAction executes the transformation on Spark executors.
 */
trait CustomFileTransformer extends Serializable {

  /**
   * Function to be implemented to define the transformation between an input and an output stream (1:1).
   * Note that the streams are closed by the caller.
   *
   * @param options Options specified in the configuration for this transformation, including evaluated runtimeOptions
   * @param input   Input Stream of the file to be read
   * @param output  Output Stream of the file to be written
   * @return exception if something goes wrong. Other files are still processed, but the Action fails once all files are processed.
   */
  def transform(options: Map[String, String], input: InputStream, output: OutputStream): Option[Exception] =
    throw new NotImplementedError(s"${getClass.getName} must implement either transform or transformToFiles")

  /**
   * Function to be implemented to create one or more output files from an input stream (1:n).
   * Call `outputs.create(name)` for every output file to be written. The output files are created in the same
   * directory (partition) as the default output file.
   * Note that the streams are closed by the caller.
   *
   * The default implementation creates one output file with the default name and calls [[transform]].
   *
   * @param options  Options specified in the configuration for this transformation, including evaluated runtimeOptions
   * @param input    Input Stream of the file to be read
   * @param fileName default name of the output file, derived from the input file name
   * @param outputs  factory to create output files
   * @return exception if something goes wrong. Other files are still processed, but the Action fails once all files are processed.
   */
  def transformToFiles(options: Map[String, String], input: InputStream, fileName: String, outputs: OutputStreamFactory): Option[Exception] =
    transform(options, input, outputs.create(fileName))

  /**
   * Optional function to define the transformation of input to output partition values.
   * For example this enables to standardize partition values extracted from the file path, e.g. year=2002/month=12 to dt=200212.
   * All output files of an input file are written to the same output partition.
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   *
   * @param options         Options specified in the configuration for this transformation
   * @param partitionValues partition values to be transformed
   * @return Map of input to output partition values. This allows to map partition values forward and backward, which is needed in execution modes.
   *         Return None if mapping is 1:1.
   */
  def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues, PartitionValues]] = None
}

/**
 * Factory to create output files of a file transformation.
 */
trait OutputStreamFactory {

  /**
   * Create a new output file.
   * @param fileName name of the output file, without directory. It is created in the directory (partition) of the
   *                 default output file. If it doesn't match the file name pattern of the output DataObject, the extension
   *                 of the pattern is appended.
   * @return Output Stream of the new file. It is closed by the caller, but can also be closed by the transformation.
   */
  def create(fileName: String): OutputStream
}
