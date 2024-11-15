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

package io.smartdatalake.util.webservice

import com.github.tomakehurst.wiremock.client.{WireMock => w}
import io.smartdatalake.testutils.TestUtil
import org.apache.spark.sql.types.{DataType, StructType}
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.FunSuite

class OpenApiUtilTest extends FunSuite {

  test("parsing schema for path with operationId") {
    val specJson =
      """
        |{
        |    "paths": {
        |        "/ping": {
        |            "get": {
        |                "operationId": "getPing",
        |                "responses": {
        |                    "200": {
        |                        "description": "OK",
        |                        "content": {
        |                            "application/json": {
        |                                "schema": {
        |                                    "type": "object",
        |                                    "properties": {
        |                                        "id": {
        |                                            "type": "integer",
        |                                            "description": "The user ID."
        |                                        },
        |                                        "username": {
        |                                            "type": "string",
        |                                            "description": "The user name."
        |                                        }
        |                                    }
        |                                }
        |                            }
        |                        }
        |                    }
        |                }
        |            }
        |        }
        |    }
        |}
        |""".stripMargin

    val specPaths = OpenApiUtil.extractOperationsFromJson(parse(specJson))
    val (contentType, schema) = specPaths.find(_.operationId == "getPing").get.responseSchema("application/json")
    val expected = StructType.fromDDL("id long, username string")
    assert(DataType.equalsIgnoreNullability(schema, expected))
  }

  test("parsing schema for path without operationId") {
    val specJson =
      """
        |{
        |    "paths": {
        |        "/ping": {
        |            "get": {
        |                "responses": {
        |                    "200": {
        |                        "description": "OK",
        |                        "content": {
        |                            "application/json": {
        |                                "schema": {
        |                                    "type": "object",
        |                                    "properties": {
        |                                        "id": {
        |                                            "type": "integer",
        |                                            "description": "The user ID."
        |                                        },
        |                                        "username": {
        |                                            "type": "string",
        |                                            "description": "The user name."
        |                                        }
        |                                    }
        |                                }
        |                            }
        |                        }
        |                    }
        |                }
        |            }
        |        }
        |    }
        |}
        |""".stripMargin

    val specPaths = OpenApiUtil.extractOperationsFromJson(parse(specJson))
    val (contentType, schema) = specPaths.find(_.operationId == "/ping:get").get.responseSchema("application/json")
    val expected = StructType.fromDDL("id long, username string")
    assert(DataType.equalsIgnoreNullability(schema, expected))
  }


  test("parsing schema for path using local components") {
    val specJson =
      """
        |{
        |    "paths": {
        |        "/ping": {
        |            "get": {
        |                "responses": {
        |                    "200": {
        |                        "description": "OK",
        |                        "content": {
        |                            "application/json": {
        |                                "schema": {
        |                                    "$ref": "#/components/schemas/User"
        |                                }
        |                            }
        |                        },
        |                        "components": {
        |                            "schemas": {
        |                                "User": {
        |                                    "type": "object",
        |                                    "properties": {
        |                                        "id": {
        |                                            "type": "integer",
        |                                            "description": "The user ID."
        |                                        },
        |                                        "username": {
        |                                            "type": "string",
        |                                            "description": "The user name."
        |                                        }
        |                                    }
        |                                }
        |                            }
        |                        }
        |                    }
        |                }
        |            }
        |        }
        |    }
        |}
        |""".stripMargin

    val specPaths = OpenApiUtil.extractOperationsFromJson(parse(specJson))
    val (contentType, schema) = specPaths.find(_.operationId == "/ping:get").get.responseSchema("application/json; charset=utf-8")
    val expected = StructType.fromDDL("id long, username string")
    assert(DataType.equalsIgnoreNullability(schema, expected))
  }

  test("parsing schema for path using global components") {
    val specJson =
      """
        |{
        |    "paths": {
        |        "/ping": {
        |            "get": {
        |                "responses": {
        |                    "200": {
        |                        "description": "OK",
        |                        "content": {
        |                            "application/json; charset=utf-8": {
        |                                "schema": {
        |                                    "$ref": "#/components/schemas/User"
        |                                }
        |                            }
        |                        }
        |                    }
        |                }
        |            }
        |        }
        |    },
        |    "components": {
        |        "schemas": {
        |            "User": {
        |                "type": "object",
        |                "properties": {
        |                    "id": {
        |                        "type": "integer",
        |                        "description": "The user ID."
        |                    },
        |                    "username": {
        |                        "type": "string",
        |                        "description": "The user name."
        |                    }
        |                }
        |            }
        |        }
        |    }
        |}
        |""".stripMargin

    val specPaths = OpenApiUtil.extractOperationsFromJson(parse(specJson))
    val (contentType, schema) = specPaths.find(_.operationId == "/ping:get").get.responseSchema("application/json")
    val expected = StructType.fromDDL("id long, username string")
    assert(DataType.equalsIgnoreNullability(schema, expected))
  }

  test("Reading OpenApi specification test") {
    val port = 8080
    val httpsPort = 8443
    val host = "127.0.0.1"
    val server = TestUtil.startWebservice(host, port, httpsPort)

    val specJson =
      """
        |{
        |    "paths": {
        |        "/ping": {
        |            "get": {
        |                "operationId": "getPing",
        |                "responses": {
        |                    "200": {
        |                        "description": "OK",
        |                        "content": {
        |                            "application/json": {
        |                                "schema": {
        |                                    "type": "object",
        |                                    "properties": {
        |                                        "id": {
        |                                            "type": "integer",
        |                                            "description": "The user ID."
        |                                        },
        |                                        "username": {
        |                                            "type": "string",
        |                                            "description": "The user name."
        |                                        }
        |                                    }
        |                                }
        |                            }
        |                        }
        |                    }
        |                }
        |            }
        |        }
        |    }
        |}
        |""".stripMargin

    w.stubFor(w.get(w.urlMatching("/v3/api-docs"))
      .withHeader("Accept", w.equalTo("application/json"))
      .willReturn(w.aResponse().withBody(specJson))
    )

    val (contentType, schema) = OpenApiUtil.queryOperationSchema("http://localhost:8080", "getPing", responseContentType = "application/json")
    val expected = StructType.fromDDL("id long, username string")
    assert(DataType.equalsIgnoreNullability(schema, expected))

    server.stop()
  }

}
