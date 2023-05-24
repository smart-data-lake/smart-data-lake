/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import org.apache.hadoop.io.compress.{CompressionInputStream, CompressionOutputStream, Compressor, DefaultCodec}

import java.io.{InputStream, OutputStream}
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}

/**
 * Codec to read and write zipped Csv-files with Hadoop
 * Note that only the first file entry of a Zip-Archive is read, and only Zip-files with one Entry named "data.csv" can be created.
 * Attention: reading with custom codec in Spark is only implemented for writing files, and not for reading files.
 * Usage in Csv/RelaxedCsvFileDataObject:
 * csv-options {
 *   compression =  io.smartdatalake.workflow.dataobject.ZipCsvCodec
 * }
 */
class ZipCsvCodec extends ZipCodec("data.csv")

class ZipCodec(entryName: String) extends DefaultCodec {
  override def createInputStream(in: InputStream): CompressionInputStream = {
    val zipIs = new ZipInputStream(in)
    new ZipDecompressorStream(zipIs)
  }
  override def createOutputStream(out: OutputStream, compressor: Compressor): ZipCompressorStream = {
    val zipOs = new ZipOutputStream(out)
    new ZipCompressorStream(zipOs, entryName)
  }
  override def getDefaultExtension: String = ".zip"
}

class ZipDecompressorStream(in: ZipInputStream) extends CompressionInputStream(in) {
  in.getNextEntry
  private val oneByte = new Array[Byte](1)
  override def read(): Int = {
    if (read(oneByte, 0, oneByte.length) == -(1)) -1
    else oneByte(0) & 0xff
  }
  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    in.read(b, off, len)
  }
  override def resetState(): Unit = {}
}

class ZipCompressorStream(out: ZipOutputStream, entryName: String) extends CompressionOutputStream(out) {
  out.putNextEntry(new ZipEntry(entryName))
  override def write(b: Int): Unit = out.write(b)
  override def write(data: Array[Byte], offset: Int, length: Int): Unit = {
    out.write(data, offset, length)
  }
  override def finish(): Unit = {
    out.closeEntry()
    out.finish()
  }
  override def resetState(): Unit = {}
}
