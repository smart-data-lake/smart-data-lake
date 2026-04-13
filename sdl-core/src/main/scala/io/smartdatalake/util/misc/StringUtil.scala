/*
 * sdl-core - Build your data lake the smart way.
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

import java.text.Normalizer

/**
 * Provides utility functions for [[String]]s.
 */
object StringUtil {

  /**
   * Transforms a name in CamelCase to lowercase with underscores, i.e. TestString -> test_string
   *
   * @param x [[String]] to transform
   * @return transformed [[String]]
   */
  def strCamelCase2LowerCaseWithUnderscores(x: String): String = {
    val normalized = "([A-Z]+[^A-Z_]*)|[^A-Z_]+".r.findAllMatchIn(x).map(_.group(0).toLowerCase.filter(_ != '_'))
      .filter(_.nonEmpty).mkString("_")
    // preserve leading underscores
    x.takeWhile(_ == '_') + normalized
  }

  /**
   * Transforms name with dashs and underscores to CamelCase.
   */
  def strToCamelCase(x: String): String = {
    val parts = x.split("[_\\- ]")
    parts.map(_.capitalize).mkString
  }

  /**
   * Transforms name with dashs and underscores to LowerCamelCase.
   */
  def strToLowerCamelCase(x: String): String = {
    val camelCase = strToCamelCase(x)
    // lowercase first letter
    camelCase.head.toLower +: camelCase.tail
  }

  /**
   * Transform a string with UTF8 chars (e.g. diacritics, umlauts) to ASCII chars (best effort)
   */
  def normalizeToAscii(x: String): String = {
    // replace umlauts
    val normalizedUmlauts = x
      .replace("Ä", "Ae")
      .replace("Ö", "Oe")
      .replace("Ü", "Ue")
      .replace("ä", "ae")
      .replace("ö", "oe")
      .replace("ü", "ue")
    // decompose diacritics (e.g. accents) into separate UTF characters
    val normalizedUtf = Normalizer.normalize(normalizedUmlauts, Normalizer.Form.NFD)
    // remove all non-ascii characters
    normalizedUtf.replaceAll("[^\\p{ASCII}]", "")
  }

  /**
   * Remove all hyphen and blanks from a string with underscores
   */
  def replaceNonSqlWithUnderscores(x: String): String = {
    x.replaceAll("[^a-zA-Z0-9_]+", "_")
  }

  /**
   * Remove all chars from a string which dont belong to lowercase SQL standard naming characters
   */
  def removeNonStandardSQLNameChars(x: String): String = {
    x.toLowerCase.replaceAll("[^a-zA-Z0-9_]", "")
  }

}
