/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake

import configs.Configs
import io.smartdatalake.config.SdlConfigObject.{ActionObjectId, ConnectionId, DataObjectId}
import io.smartdatalake.util.webservice.KeycloakConfig
import io.smartdatalake.workflow.action.customlogic._
import io.smartdatalake.workflow.dataobject.{SplunkParams, WebserviceFileDataObject}
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

package object config {

  /**
   * A [[Configs]] reader that reads [[Either]] values.
   *
   * @param aReader the reader for the [[Left]] value.
   * @param bReader the reader for the [[Right]] value.
   * @tparam A the [[Left]] value type.
   * @tparam B the [[Right]] value type.
   * @return A [[Configs]] containing a [[Left]] or, if it can not be parsed, a [[Right]] value of the corresponding type.
   */
  implicit def eitherReader[A, B](implicit aReader: Configs[A], bReader: Configs[B]): Configs[Either[A, B]] = {
    Configs.fromTry { (c, p) =>
      aReader.get(c, p).map(Left(_)).orElse(bReader.get(c, p).map(Right(_))).valueOrThrow(_.configException)
    }
  }

  /**
   * A [[Configs]] reader that reads [[StructType]] values.
   *
   * This reader parses a [[StructType]] from a DDL string.
   */
  implicit val structTypeReader: Configs[StructType] = Configs.fromTry { (c, p) =>
    StructType.fromDDL(c.getString(p))
  }

  // --------------------------------------------------------------------------------
  // Config readers to circumvent problems related to a bug:
  // The problem is that kxbmap sometimes can not find the correct config reader for
  // some non-trivial types, e.g. List[CustomCaseClass] or Option[CustomCaseClass]
  // see: https://github.com/kxbmap/configs/issues/44
  // TODO: check periodically if still needed, should not be needed with scala 2.13+
  // --------------------------------------------------------------------------------

  implicit val customDfCreatorConfigReader: Configs[CustomDfCreatorConfig] = Configs.derive[CustomDfCreatorConfig]

  implicit val customDfTransformerConfigReader: Configs[CustomDfTransformerConfig] = Configs.derive[CustomDfTransformerConfig]

  implicit val customDfsTransformerConfigReader: Configs[CustomDfsTransformerConfig] = Configs.derive[CustomDfsTransformerConfig]

  implicit val customFileTransformerConfigReader: Configs[CustomFileTransformerConfig] = Configs.derive[CustomFileTransformerConfig]

  // --------------------------------------------------------------------------------

  /**
   * A [[Configs]] reader that reads [[KeycloakConfig]] values.
   */
  implicit val keyCloakConfigReader: Configs[Option[KeycloakConfig]] = Configs.fromConfigTry { c =>
    WebserviceFileDataObject.getKeyCloakConfig(c)
  }

  /**
   * A [[Configs]] reader that reads [[SplunkParams]] values.
   *
   * SplunkParams have special semantics for Duration which are covered with this reader.
   */
  implicit val splunkParamsReader: Configs[SplunkParams] = Configs.fromConfigTry { c =>
    SplunkParams.fromConfig(c)
  }

  /**
   * A [[Configs]] reader that reads [[DataObjectId]] values.
   */
  implicit val connectionIdReader: Configs[ConnectionId] = Configs.fromTry { (c, p) =>
    ConnectionId(c.getString(p))
  }

  /**
   * A [[Configs]] reader that reads [[DataObjectId]] values.
   */
  implicit val dataObjectIdReader: Configs[DataObjectId] = Configs.fromTry { (c, p) =>
    DataObjectId(c.getString(p))
  }

  /**
   * A [[Configs]] reader that reads [[ActionObjectId]] values.
   */
  implicit val actionObjectIdReader: Configs[ActionObjectId] = Configs.fromTry { (c, p) =>
    ActionObjectId(c.getString(p))
  }
}
