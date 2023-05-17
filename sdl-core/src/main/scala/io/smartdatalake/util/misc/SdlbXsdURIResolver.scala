/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.misc

import io.smartdatalake.util.hdfs.HdfsUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.xml.XsdSchemaConverter
import org.apache.ws.commons.schema.XmlSchemaCollection
import org.apache.ws.commons.schema.resolver.CollectionURIResolver
import org.xml.sax.InputSource

import java.io.StringReader
import java.net.{MalformedURLException, URI}

/**
 * SdlbXsdURIResolver resolves URIs included in XSD schemas by using Hadoop Filesystem.
 * It can be used by org.apache.ws.xmlschema to parse XSD schemas distributed over multiple files from Hadoop filesystem.
 */
class SdlbXsdURIResolver(fs: FileSystem) extends CollectionURIResolver {
  override def resolveEntity(targetNamespace: String, schemaLocation: String, baseUri: String): InputSource = {

    // get path to read
    // copied from DefaultURIResolver and converted to Scala / Hadoop
    val path = if (baseUri != null) {
      try {
        var baseFile: Path = null
        var newBaseUri: String = baseUri
        try {
          val uri = new URI(newBaseUri)
          baseFile = new Path(uri)
          if (!fs.exists(baseFile)) baseFile = new Path(newBaseUri)
        } catch {
          case ex: Throwable => baseFile = new Path(newBaseUri)
        }
        if (fs.exists(baseFile)) newBaseUri = baseFile.toUri.toString
        else if (collectionBaseURI.isDefined) {
          baseFile = new Path(collectionBaseURI.get)
          if (fs.exists(baseFile)) newBaseUri = baseFile.toUri.toString
        }
        new Path(new Path(newBaseUri), schemaLocation)
      } catch {
        case e1: MalformedURLException => throw new RuntimeException(e1)
      }
    } else new Path(schemaLocation)

    // create InputSource
    val inputStream = fs.open(path)
    new InputSource(inputStream)
  }

  private var collectionBaseURI: Option[String] = None
  override def setCollectionBaseURI(uri: String): Unit = {
    collectionBaseURI = Some(uri)
  }
  override def getCollectionBaseURI: String = {
    collectionBaseURI.orNull
  }
}

object SdlbXsdURIResolver {
  def readXsd(xsdFile: Path, maxRecursion: Int)(implicit fs: FileSystem): StructType = {

    // setup schema collection with our own URI resolver to resolve referenced schemas
    val uriResolver = new SdlbXsdURIResolver(fs)
    val xmlSchemaCollection = new XmlSchemaCollection()
    xmlSchemaCollection.setSchemaResolver(uriResolver)
    xmlSchemaCollection.setBaseUri(xsdFile.getParent.toString)

    // read from hadoop and convert
    val xsdString = HdfsUtil.readHadoopFile(xsdFile)
    val xmlSchema = xmlSchemaCollection.read(new StringReader(xsdString))
    new XsdSchemaConverter(xmlSchema, maxRecursion).getStructType
  }
}