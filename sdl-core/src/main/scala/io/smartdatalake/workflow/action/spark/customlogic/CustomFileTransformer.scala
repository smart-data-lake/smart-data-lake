/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.action.spark.customlogic

import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.{CustomCodeUtil, SmartDataLakeLogger}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.slf4j.Logger

/**
  * Interface to define custom file transformation for CustomFileAction
  */
trait CustomFileTransformer extends Serializable {

  /**
   * Function to be implemented to define the transformation between an input and output stream
   *
   * @param options Options specified in the configuration for this transformation
   * @param input Hadoop Input Stream of the file to be read
   * @param output Hadoop Output Stream of the file to be written
   * @return exception if something goes wrong and processing should not be stopped for all files
   */
  def transform(options: Map[String,String], input: FSDataInputStream, output: FSDataOutputStream): Option[Exception]
}

/**
 * Configuration of custom file transformation between one input and one output (1:1)
 *
 * @param className Optional class name to load transformer code from
 * @param scalaFile Optional file where scala code for transformation is loaded from
 * @param scalaCode Optional scala code for transformation
 * @param options Options to pass to the transformation
 */
case class CustomFileTransformerConfig( className: Option[String] = None, scalaFile: Option[String] = None, scalaCode: Option[String] = None, options: Option[Map[String,String]] = None) {
  require(className.isDefined || scalaFile.isDefined || scalaCode.isDefined, "Either className or scalaFile must be defined for CustomDfTransformer")

  val impl : CustomFileTransformer = className.map {
    clazz => CustomCodeUtil.getClassInstanceByName[CustomFileTransformer](clazz)
  }.orElse{
    scalaFile.map {
      file =>
        implicit val defaultHadoopConf: Configuration = new Configuration()
        val fnTransform = CustomCodeUtil.compileCode[(Map[String,String], FSDataInputStream, FSDataOutputStream, Logger) => Option[Exception]](HdfsUtil.readHadoopFile(file))
        new CustomFileTransformerWrapper( fnTransform )
    }
  }.orElse{
    scalaCode.map {
      code =>
        val fnTransform = CustomCodeUtil.compileCode[(Map[String,String], FSDataInputStream, FSDataOutputStream, Logger) => Option[Exception]](code)
        new CustomFileTransformerWrapper( fnTransform )
    }
  }.get

  def transform(input: FSDataInputStream, output: FSDataOutputStream): Option[Exception] = {
    impl.transform(options.getOrElse(Map()), input, output)
  }
}

class CustomFileTransformerWrapper(val fnExec: (Map[String,String], FSDataInputStream, FSDataOutputStream, Logger) => Option[Exception])
extends CustomFileTransformer with SmartDataLakeLogger {
  override def transform(options: Map[String,String], input: FSDataInputStream, output: FSDataOutputStream): Option[Exception] = {
    fnExec(options, input, output, logger)
  }
}
