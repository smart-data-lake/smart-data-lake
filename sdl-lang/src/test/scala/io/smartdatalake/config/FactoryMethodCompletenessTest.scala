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
package io.smartdatalake.config

import io.smartdatalake.meta.GenericTypeUtil
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe._
import scala.util.Try

/**
 * Asserts that every SDL configuration type can actually be instantiated from a Hocon config.
 *
 * [[ConfigParser]] resolves the companion object of a type by reflection and then looks up its `fromConfig`
 * method by name and signature. Neither the compiler nor any other test verifies that this companion object
 * exists, so a missing or misspelled `fromConfig` only surfaces when a user writes `type = <TheType>` in their
 * configuration. This test closes that gap by running the very same lookup [[ConfigParser]] uses against all
 * types that are published in the json schema.
 *
 * This test lives in sdl-lang because sdl-lang depends on every other module, so a single run covers the
 * whole codebase.
 *
 * Types that are intentionally not instantiable from config must be marked with [[ExcludeFromSchemaExport]],
 * which also keeps them out of the json schema.
 */
class FactoryMethodCompletenessTest extends AnyFunSuite {

  test("every parsable config type has a companion object with a valid fromConfig method") {

    val candidates = GenericTypeUtil.typeDefs(GenericTypeUtil.getReflections)
      .filter(_.isFinal) // concrete case classes, e.g. what a user can write as `type = ...`
      .map(_.tpe)
      .filter(_ <:< typeOf[ParsableFromConfig[_]])
      .filterNot(_ <:< typeOf[ExcludeFromSchemaExport])
      // skip helper types declared inside a test class or method, they are never parsed from config
      .filter(_.typeSymbol.owner.isPackage)

    // guard against a broken classpath scan silently turning this test into a no-op
    assert(candidates.size > 100, s"expected more than 100 parsable types, but the scan found ${candidates.size}")

    // this is the same lookup ConfigParser does when it instantiates a type named in a `type` attribute
    val errors = candidates.toSeq.sortBy(_.typeSymbol.fullName).flatMap { tpe =>
      Try(FactoryMethodExtractor.extract(tpe.typeSymbol.companion.asModule)).failed.toOption
        .map(e => s"${tpe.typeSymbol.fullName}: ${e.getMessage}")
    }

    assert(errors.isEmpty, s"${errors.size} of ${candidates.size} type(s) can not be parsed from config. " +
      s"Add a companion object 'object <Type> extends FromConfigFactory[<BaseType>]' with a " +
      s"'fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): <Type>' method, " +
      s"or mark the type with ExcludeFromSchemaExport if it is not meant to be parsed from config:\n" +
      errors.mkString("\n"))
  }
}
