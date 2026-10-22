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
package io.smartdatalake.util.spark

import java.io.{OutputStream, Writer}
import javax.xml.stream.{XMLEventWriter, XMLOutputFactory, XMLStreamWriter}
import javax.xml.transform.Result

/**
 * Compatibility wrapper for Spark 4 XML writes when an unshaded Woodstox implementation
 * is used on the classpath.
 *
 * Spark 4 may set Hadoop-shaded Woodstox property names. This wrapper remaps them to
 * their unshaded equivalents before delegating to the real output factory.
 */
class WoodstoxXMLOutputFactory extends XMLOutputFactory {

  import WoodstoxXMLOutputFactory._

  private val delegate: XMLOutputFactory = createDelegateFactory()

  override def createXMLStreamWriter(stream: Writer): XMLStreamWriter =
    delegate.createXMLStreamWriter(stream)

  override def createXMLStreamWriter(stream: OutputStream): XMLStreamWriter =
    delegate.createXMLStreamWriter(stream)

  override def createXMLStreamWriter(stream: OutputStream, encoding: String): XMLStreamWriter =
    delegate.createXMLStreamWriter(stream, encoding)

  override def createXMLStreamWriter(result: Result): XMLStreamWriter =
    delegate.createXMLStreamWriter(result)

  override def createXMLEventWriter(stream: OutputStream): XMLEventWriter =
    delegate.createXMLEventWriter(stream)

  override def createXMLEventWriter(stream: OutputStream, encoding: String): XMLEventWriter =
    delegate.createXMLEventWriter(stream, encoding)

  override def createXMLEventWriter(stream: Writer): XMLEventWriter =
    delegate.createXMLEventWriter(stream)

  override def createXMLEventWriter(result: Result): XMLEventWriter =
    delegate.createXMLEventWriter(result)

  override def setProperty(name: String, value: Any): Unit =
    delegate.setProperty(remapProperty(name), value)

  override def getProperty(name: String): AnyRef =
    delegate.getProperty(remapProperty(name))

  override def isPropertySupported(name: String): Boolean =
    delegate.isPropertySupported(remapProperty(name))

  private def remapProperty(name: String): String =
    shadedToUnshadedPropertyMap.getOrElse(name, name)

  private def createDelegateFactory(): XMLOutputFactory = {
    val oldFactoryProp = System.getProperty(XMLOutputFactoryClassProperty)

    // Avoid recursion when this wrapper is configured as the default factory.
    if (oldFactoryProp == classOf[WoodstoxXMLOutputFactory].getName) {
      System.clearProperty(XMLOutputFactoryClassProperty)
    }

    try XMLOutputFactory.newFactory()
    finally {
      if (oldFactoryProp != null) {
        System.setProperty(XMLOutputFactoryClassProperty, oldFactoryProp)
      }
    }
  }
}

object WoodstoxXMLOutputFactory {

  private val XMLOutputFactoryClassProperty = "javax.xml.stream.XMLOutputFactory"

  private val shadedToUnshadedPropertyMap: Map[String, String] = Map(
    "org.apache.hadoop.shaded.com.ctc.wstx.outputValidateStructure" -> "com.ctc.wstx.outputValidateStructure",
    "org.apache.hadoop.shaded.com.ctc.wstx.outputValidateNames" -> "com.ctc.wstx.outputValidateNames"
  )
}

