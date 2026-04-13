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
package io.smartdatalake.util.misc

import com.github.takezoe.scaladoc.{Scaladoc => ScaladocAnnotation}
import scaladoc.Markup._ // https://github.com/andyglow/scaladoc
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

  // Remove leading spaces in code blocks
  def dedentCodeBlock(code: String): String = {
    val lines = code.stripMargin.linesIterator.toList
    val nonEmptyLines = lines.filter(line => line.trim.nonEmpty && ! List("{{{", "}}}").contains(line.trim))
    val firstLineIndentation = if (nonEmptyLines.isEmpty) (0, 0) else spacesAndTabs(nonEmptyLines.head) // Assume nicely formatted code
    val dedentedLines = lines.map(removeSpacesAndTabs(_, firstLineIndentation))
    dedentedLines.mkString("\n")
  }

  // Leading number of (spaces, tabs)
  def spacesAndTabs(line: String): (Int, Int) = {
    line.takeWhile(Seq(' ', '\t').contains(_)).foldLeft((0, 0))((spacesTabs, char) => {
      if (char == ' ') (spacesTabs._1 + 1, spacesTabs._2) else (spacesTabs._1, spacesTabs._2 + 1)
    })
  }

  def removeSpacesAndTabs(line: String, spacesTabs: (Int, Int)): String = {
    require(spacesTabs._1 >= 0 && spacesTabs._2 >= 0, "Indentation error. The line has either too many spaces or too many tabs")
    if (line.isEmpty) ""
    else if (List("{{{","}}}").contains(line.trim)) line.trim
    else line.head match {
      case c if ((0, 0) == spacesTabs) => line
      case ' ' => removeSpacesAndTabs(line.tail, (spacesTabs._1 - 1, spacesTabs._2))
      case '\t' => removeSpacesAndTabs(line.tail, (spacesTabs._1, spacesTabs._2 - 1))
      case _ => throw new Exception("The line doesn't have enough indentation characters to remove the entire common indentation")
    }
  }

  def formatScaladocLinkTag(captureGroup1: String, captureGroup2: String): String = {
    var parsedLink = ""

    // Do not wrap urls in inline code blocks
    if (captureGroup1.contains("https://")){
      val splitHyperref = captureGroup1.split(" ")
      if (splitHyperref.length > 1){
        // If the Url contains an alias (pretty name), preserve it
        parsedLink = s"[${splitHyperref.drop(1).mkString(" ")}](${splitHyperref(0)})"
      }else{
        parsedLink = captureGroup1
      }
    } else {
      parsedLink = s"`${captureGroup1}`"
    }

    s"${parsedLink}${if (captureGroup2 != null) captureGroup2 else " "}"
  }

  def formatScaladocString(str: String): String = {
    // Remove link square brackets (including plural s handling)
    // If the link is followed by a single s, remove the space
    val bracketRemovalPattern = raw"\[\[(.+?)\]\] (\.|s|,)?".r
    bracketRemovalPattern.replaceAllIn(str, m =>
      formatScaladocLinkTag(m.group(1), m.group(2))
    )
      .replaceAll(raw"\.\n(?!\n)", ".  \n") // Add carriage return for Markdown formatting
      .replaceAll(raw"(\\r)?\\n", "\n") // convert & standardize line separator
      .replaceAll(raw"\n\h*\*\h*", "\n") // remove trailing asterisk
      .replace("->", "\u2192") // Prettify right arrow
      .trim // remove leading and trailing line separators
  }

  def formatScaladocMarkup(markup: Markup): String = {
    markup match {
      case x: Heading => s"\n\n${x.trimmed.plainString}\n\n"
      case x: Paragraph => s"\n\n${x.trimmed.plainString}"
      case x: CodeBlock => dedentCodeBlock(s"\n${x.plainString}\n")
      case x: Span => s" ${x.trimmed.plainString}"
      case x: Document =>
        val contentStr = x.elements.map(formatScaladocMarkup).mkString("")
        formatScaladocString(contentStr)
          .replaceAll(raw"\{\{\{\n?", "```\n") // convert wiki code block to markup code block
          .replaceAll(raw"(.*)\n?}}}", "$1\n```\n") // convert wiki code block to markup code block
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
