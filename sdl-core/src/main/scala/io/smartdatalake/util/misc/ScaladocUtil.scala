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

package io.smartdatalake.util.misc

import com.github.takezoe.scaladoc.{Scaladoc => ScaladocAnnotation}
import scaladoc.Markup._
import scaladoc.{Markup, Scaladoc, Tag}

import scala.reflect.runtime.universe.Annotation

private[smartdatalake] object ScaladocUtil {

  def formatScaladocWithTags(doc: scaladoc.Scaladoc, filter: scaladoc.Tag => Boolean = _ => true): String = {
    doc.tags.filter(filter).flatMap(formatScaladocTag).mkString("\n\n")
  }

  private def formatScaladocTag(tag: Tag): Option[String] = {
    tag match {
      case x: Tag.Description => Some(s"${formatScaladocMarkup(x.makrup)}")
      case x: Tag.Constructor => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Param => Some(s"${x.getClass.getSimpleName.toUpperCase} ${x.name}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.TypeParam => Some(s"${x.getClass.getSimpleName.toUpperCase} [${x.name}]: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Returns => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Throws => Some(s"${x.getClass.getSimpleName.toUpperCase} ${x.exceptionType}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.See => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Note => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Example => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.UseCase => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Todo => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Deprecated => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Migration => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.OtherTag => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.Author => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocString(x.text)}")
      case x: Tag.Version => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocString(x.text)}")
      case x: Tag.Since => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocString(x.text)}")
      case x: Tag.Group => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${x.id}")
      case x: Tag.GroupName => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${x.value}")
      case x: Tag.GroupDescription => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${formatScaladocMarkup(x.markup)}")
      case x: Tag.GroupPriority => Some(s"${x.getClass.getSimpleName.toUpperCase}: ${x.value}")
      case Tag.Documentable => None
      case Tag.InheritDoc => None
    }
  }

  def formatScaladocString(str: String): String = {
    str.replaceAll(raw"(\\r)?\\n", "\n") // convert & standardize line separator
      .replaceAll(raw"\n\h*\*\h*", "\n") // remove trailing asterisk
      .trim // remove leading and trailing line separators
  }

  def formatScaladocMarkup(markup: Markup): String = {
    markup match {
      case x: Heading => s"\n\n${x.trimmed.plainString}\n\n"
      case x: Paragraph => s"\n\n${x.trimmed.plainString}"
      case x: CodeBlock => s"\n${x.trimmed.plainString}\n"
      case x: Span => s" ${x.trimmed.plainString}"
      case x: Document =>
        val contentStr = x.elements.map(formatScaladocMarkup).mkString("")
        formatScaladocString(contentStr)
          .replaceAll(raw"\{\{\{", "```") // convert wiki code block to markup code block
          .replaceAll(raw"}}}", "```"); // convert wiki code block to markup code block
    }
  }

  def extractScalaDoc(annotations: Seq[Annotation]): Option[Scaladoc] = {
    import scala.reflect.runtime.universe._
    val annotation = annotations.find(_.tree.tpe =:= typeOf[ScaladocAnnotation])
    val rawScalaDoc = annotation.flatMap(_.tree.children.last.children.collectFirst{case Literal(Constant(name: String)) => name}) // In scala 2.12 this is an AssignOrNamedArg, in Scala 2.13 a NamedArg... we need to be dynamic...
    rawScalaDoc.map { d =>
      val s = scaladoc.Scaladoc.fromString(d)
      s.right.getOrElse(throw new IllegalStateException(s"Could not extract Scaladoc from '$d': ${s.left.e}"))
    }
  }

  def getClassScalaDoc(className: String): Option[Scaladoc] = {
    val cls = getClass.getClassLoader.loadClass(className)
    val tpe = mirror.classSymbol(cls).toType
    val annotations = tpe.typeSymbol.annotations
    extractScalaDoc(annotations)
  }
  private lazy val mirror = scala.reflect.runtime.currentMirror
}
