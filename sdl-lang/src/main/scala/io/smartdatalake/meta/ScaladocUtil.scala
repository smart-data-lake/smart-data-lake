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

package io.smartdatalake.meta

import scaladoc.{Markup, Scaladoc, Tag}
import com.github.takezoe.scaladoc.{Scaladoc => ScaladocAnnotation}

import scala.reflect.runtime.universe.{Annotation, AssignOrNamedArg, typeOf}

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
    formatScaladocString(markup.trimmed.plainString)
      .replaceAll(raw"\{\{\{", "```") // convert wiki code block to markup code block
      .replaceAll(raw"}}}", "```"); // convert wiki code block to markup code block
  }

  def extractScalaDoc(annotations: Seq[Annotation]): Option[Scaladoc] = {
    val rawScaladoc = annotations.find(_.tree.tpe =:= typeOf[ScaladocAnnotation])
      .flatMap(_.tree.children.collectFirst { case x: AssignOrNamedArg => x.rhs.toString })
    rawScaladoc.map(d => scaladoc.Scaladoc.fromString(d).right.get)
  }
}
