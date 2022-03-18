/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.meta.dagexporter

import org.scalatest.FunSuite

class DagExporterTest extends FunSuite {

  test("testMain") {
    val expectedOutput =
      """{
        |  "actionId6" : {
        |    "metadata" : {
        |      "feed" : "actionId",
        |      "tags" : [ ]
        |    },
        |    "inputIds" : [ "dataObjectParquet9", "dataObjectParquet10" ],
        |    "outputIds" : [ "dataObjectCsv5" ]
        |  },
        |  "actionId8" : {
        |    "metadata" : {
        |      "feed" : "actionId",
        |      "tags" : [ ]
        |    },
        |    "inputIds" : [ "dataObjectParquet12" ],
        |    "outputIds" : [ "dataObjectParquet13" ]
        |  },
        |  "actionId3" : {
        |    "metadata" : {
        |      "feed" : "actionId",
        |      "tags" : [ ]
        |    },
        |    "inputIds" : [ "dataObjectCsv3" ],
        |    "outputIds" : [ "dataObjectParquet7" ]
        |  },
        |  "actionId2" : {
        |    "metadata" : {
        |      "feed" : "actionId",
        |      "tags" : [ ]
        |    },
        |    "inputIds" : [ "dataObjectParquet6" ],
        |    "outputIds" : [ "dataObjectParquet7" ]
        |  },
        |  "actionId5" : {
        |    "metadata" : {
        |      "feed" : "actionId",
        |      "tags" : [ ]
        |    },
        |    "inputIds" : [ "dataObjectCsv4" ],
        |    "outputIds" : [ "dataObjectParquet9", "dataObjectParquet10" ]
        |  },
        |  "actionId1" : {
        |    "metadata" : {
        |      "feed" : "actionId",
        |      "tags" : [ ]
        |    },
        |    "inputIds" : [ "dataObjectCsv1", "dataObjectCsv2" ],
        |    "outputIds" : [ "dataObjectParquet6" ]
        |  },
        |  "actionId4" : {
        |    "metadata" : {
        |      "feed" : "actionId",
        |      "tags" : [ ]
        |    },
        |    "inputIds" : [ "dataObjectParquet7", "dataObjectParquet8" ],
        |    "outputIds" : [ "dataObjectParquet9" ]
        |  },
        |  "actionId7" : {
        |    "metadata" : {
        |      "feed" : "actionId",
        |      "tags" : [ ]
        |    },
        |    "inputIds" : [ "dataObjectParquet11" ],
        |    "outputIds" : [ "dataObjectParquet12" ]
        |  }
        |}""".stripMargin

    val actualOutput = DagExporter.exportConfigDagToJSON(DagExporterConfig(getClass.getResource("/dagexporter/dagexporterTest.conf").getPath))
    assert(actualOutput == expectedOutput)
  }

}
