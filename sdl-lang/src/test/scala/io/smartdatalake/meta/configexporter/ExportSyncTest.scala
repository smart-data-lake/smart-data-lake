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
package io.smartdatalake.meta.configexporter

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import io.smartdatalake.app.BackendClient
import io.smartdatalake.config.{ConfigLoader, ConfigurationException}
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.exporter.{ExportType, FileExportWriter, HadoopExportWriter, HadoopStateSyncEndpoint, StateRunId}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.testutils.WebserviceTestUtil
import io.smartdatalake.workflow.ActionDAGRunState
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.json4s.jackson.JsonMethods
import org.json4s.{JInt, JNothing, JString}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import scala.io.Source
import scala.util.Using

class ExportSyncTest extends AnyFunSuite with BeforeAndAfterAll {

  private val configPath = getClass.getResource("/exportsync/exportSyncTest.conf").getPath
  private val do1 = DataObjectId("do1")
  private val do2 = DataObjectId("do2")
  private val other3 = DataObjectId("other3")

  private val port = 8080 // must match global.uiBackend.baseUrl in exportSyncTest.conf
  private var wireMockServer: WireMockServer = _

  override def beforeAll(): Unit = {
    wireMockServer = WebserviceTestUtil.startWebservice("localhost", port, 8443)
  }

  override def afterAll(): Unit = {
    wireMockServer.stop()
  }

  private def backendPath(operation: String) = urlPathEqualTo("/api/v1/" + operation)

  private def newDir(name: String): Path = Files.createTempDirectory(s"exportsync-$name")

  private def doc(id: DataObjectId, version: Long) = s"""{"dataObjectId":"${id.id}","version":$version}"""

  private def writeDocs(writer: FileExportWriter, tpe: ExportType.Value, id: DataObjectId, versions: Long*): Unit = {
    versions.foreach(v => writer.writeVersion(tpe, doc(id, v), id, v))
  }

  private def baseConfig(source: String, target: String) = ExportSyncConfig(configPaths = Seq(configPath), schemaSource = Some(source), target = target, types = ExportType.dataObjectTypes)

  test("select versions") {
    import ExportSync.selectVersions
    assert(selectVersions(Seq(1L, 3L, 2L, 5L), Seq(2L), SyncVersions.Latest) == Seq(5L))
    assert(selectVersions(Seq(1L, 3L, 2L, 5L), Seq(2L), SyncVersions.Newer) == Seq(3L, 5L))
    assert(selectVersions(Seq(1L, 3L, 2L, 5L), Seq(2L), SyncVersions.All) == Seq(1L, 3L, 5L))
    assert(selectVersions(Seq(1L, 2L), Seq(2L), SyncVersions.Latest) == Seq())
    assert(selectVersions(Seq(1L, 2L), Seq(), SyncVersions.Newer) == Seq(1L, 2L))
    assert(selectVersions(Seq(StateRunId(1, 2), StateRunId(2, 1)), Seq(StateRunId(1, 1)), SyncVersions.Newer) == Seq(StateRunId(1, 2), StateRunId(2, 1)))
  }

  test("sync localfile to localfile with versions newer, latest and all") {
    val (srcDir, tgtDir) = (newDir("src"), newDir("tgt"))
    val (src, tgt) = (FileExportWriter(srcDir), FileExportWriter(tgtDir))
    writeDocs(src, ExportType.Schema, do1, 100, 200, 300, 400)
    writeDocs(tgt, ExportType.Schema, do1, 200)
    val config = baseConfig(s"localfile:$srcDir", s"localfile:$tgtDir").copy(types = Seq(ExportType.Schema))

    val resultLatest = ExportSync.sync(config.copy(versions = SyncVersions.Latest))
    assert(tgt.listVersions(ExportType.Schema, do1) == Seq(200L, 400L))
    assert(resultLatest.transferred == 1)

    ExportSync.sync(config.copy(versions = SyncVersions.Newer))
    assert(tgt.listVersions(ExportType.Schema, do1) == Seq(200L, 400L)) // nothing newer than 400

    ExportSync.sync(config.copy(versions = SyncVersions.All))
    assert(tgt.listVersions(ExportType.Schema, do1) == Seq(100L, 200L, 300L, 400L))
    assert(tgt.readVersion(ExportType.Schema, do1, 300).contains(doc(do1, 300)))
    // index stays sorted by version, so the latest document is the one with the highest version
    assert(tgt.readIndex(do1, "schema").last == "do1.schema.400.json")
    assert(tgt.getLatestData(do1, "schema").contains(doc(do1, 400)))

    // second run has nothing to transfer
    val resultAgain = ExportSync.sync(config.copy(versions = SyncVersions.All))
    assert(resultAgain.transferred == 0)
  }

  test("sync newer versions filtered by type and DataObject") {
    val (srcDir, tgtDir) = (newDir("src"), newDir("tgt"))
    val (src, tgt) = (FileExportWriter(srcDir), FileExportWriter(tgtDir))
    Seq(do1, do2, other3).foreach { id =>
      writeDocs(src, ExportType.Schema, id, 100, 200)
      writeDocs(src, ExportType.Stats, id, 100)
      writeDocs(src, ExportType.Lineage, id, 100)
    }
    ExportSync.main(Array("-c", configPath, "--schemaSource", s"localfile:$srcDir", "-t", s"localfile:$tgtDir", "--types", "schema,stats", "-i", "do.*", "-e", "do2"))
    assert(tgt.listVersions(ExportType.Schema, do1) == Seq(100L, 200L))
    assert(tgt.listVersions(ExportType.Stats, do1) == Seq(100L))
    assert(tgt.listVersions(ExportType.Lineage, do1).isEmpty)
    assert(tgt.listVersions(ExportType.Schema, do2).isEmpty)
    assert(tgt.listVersions(ExportType.Schema, other3).isEmpty)
  }

  test("dry run does not write") {
    val (srcDir, tgtDir) = (newDir("src"), newDir("tgt"))
    writeDocs(FileExportWriter(srcDir), ExportType.Schema, do1, 100)
    val result = ExportSync.sync(baseConfig(s"localfile:$srcDir", s"localfile:$tgtDir").copy(dryRun = true))
    assert(result.transferred == 1)
    assert(FileExportWriter(tgtDir).listVersions(ExportType.Schema, do1).isEmpty)
  }

  test("sync to unversioned Hadoop path writes only the latest version") {
    val (srcDir, tgtDir) = (newDir("src"), newDir("tgt"))
    writeDocs(FileExportWriter(srcDir), ExportType.Lineage, do1, 100, 200)
    val config = baseConfig(s"localfile:$srcDir", tgtDir.toUri.toString).copy(types = Seq(ExportType.Lineage), versions = SyncVersions.All)
    val result = ExportSync.sync(config)
    assert(result.transferred == 1)
    assert(result.missingInSource == 2) // do2, other3
    val tgt = HadoopExportWriter(new HadoopPath(tgtDir.toUri), new Configuration())
    assert(tgt.readLatest(ExportType.Lineage, do1).contains(doc(do1, 200)))
    assert(tgt.listAllVersions(ExportType.Lineage, latestOnly = false).map(_.keySet).contains(Set(do1)))
    assert(FileExportWriter(srcDir).listAllVersions(ExportType.Lineage, latestOnly = false).contains(Map(do1 -> Seq(100L, 200L))))
    // the version of the target is the modification time of its file, the content is identical nevertheless
    val resultAgain = ExportSync.sync(config)
    assert(resultAgain.transferred == 0)
  }

  test("documents of DataObjects not in the configuration are missing in source") {
    val (srcDir, tgtDir) = (newDir("src"), newDir("tgt"))
    Files.write(srcDir.resolve("unknown.lineage.json"), doc(DataObjectId("unknown"), 100).getBytes("UTF-8"))
    val result = ExportSync.sync(baseConfig(srcDir.toUri.toString, s"localfile:$tgtDir").copy(types = Seq(ExportType.Lineage)))
    assert(result == ExportSync.SyncResult(missingInSource = 3))
    assert(FileExportWriter(tgtDir).listAllVersions(ExportType.Lineage, latestOnly = false).contains(Map()))
  }

  test("error handling with stopOnError") {
    val (srcDir, tgtDir) = (newDir("src"), newDir("tgt"))
    writeDocs(FileExportWriter(srcDir), ExportType.Schema, do1, 100)
    writeDocs(FileExportWriter(srcDir), ExportType.Schema, do2, 100)
    // make target for do1 fail: a directory instead of the document file
    Files.createDirectories(tgtDir.resolve("do1.schema.100.json"))
    val config = baseConfig(s"localfile:$srcDir", s"localfile:$tgtDir").copy(types = Seq(ExportType.Schema))
    intercept[Exception](ExportSync.sync(config))
    val result = ExportSync.sync(config.copy(stopOnError = false))
    assert(result.failed == 1)
    assert(FileExportWriter(tgtDir).listVersions(ExportType.Schema, do2) == Seq(100L))
  }

  private lazy val stateTemplate = Using(Source.fromResource("stateFileV2.json"))(_.mkString).get

  private def stateJson(app: String, runId: Int, attemptId: Int, isFinal: Boolean = true): String = {
    val state = ActionDAGRunState.fromJson(stateTemplate)
    state.copy(appConfig = state.appConfig.copy(applicationName = Some(app)), runId = runId, attemptId = attemptId, isFinal = isFinal).toJson
  }

  test("sync state between state directories") {
    Environment._hadoopFileStateStoreIndexAppend = Some(true)
    val (srcDir, tgtDir) = (newDir("statesrc"), newDir("statetgt"))
    val conf = new Configuration()
    val src = HadoopStateSyncEndpoint(srcDir.toString, conf)
    src.writeState(stateJson("app1", 1, 1))
    src.writeState(stateJson("app1", 2, 1))
    src.writeState(stateJson("app1", 3, 1, isFinal = false))
    src.writeState(stateJson("app2", 1, 1))
    val tgt = HadoopStateSyncEndpoint(tgtDir.toString, conf)
    tgt.writeState(stateJson("app1", 1, 1))

    val config = baseConfig(s"localfile:${newDir("src")}", s"localfile:${newDir("tgt")}")
      .copy(types = Seq(ExportType.State), stateSource = Some(s"localfile:$srcDir"), stateTarget = Some(tgtDir.toString), applicationRegex = "app1")
    val result = ExportSync.sync(config)
    assert(result.transferred == 1) // run 3 is not final
    assert(tgt.listApplications() == Seq("app1"))
    assert(tgt.listRuns("app1") == Seq(StateRunId(1, 1), StateRunId(2, 1)))
    assert(ActionDAGRunState.fromJson(tgt.readState("app1", StateRunId(2, 1)).get).runId == 2)
    // final states are added to the index of the state directory
    val index = Using(Source.fromFile(tgtDir.resolve("index.json").toFile))(_.getLines().toSeq).get
    assert(index.size == 2)
    Environment._hadoopFileStateStoreIndexAppend = None
  }

  test("state requires stateSource if source is not uiBackend") {
    val config = baseConfig("localfile:./a", "uiBackend").copy(types = Seq(ExportType.State))
    intercept[ConfigurationException](ExportSync.sync(config))
  }

  test("upload newer versions to uiBackend") {
    val srcDir = newDir("src")
    writeDocs(FileExportWriter(srcDir), ExportType.Schema, do1, 100, 200)
    wireMockServer.resetAll()
    stubFor(get(backendPath("dataobject/schema/do1/tstamps"))
      .withQueryParam("repo", equalTo("test"))
      .willReturn(aResponse().withStatus(200).withBody("[100]")))
    stubFor(get(backendPath("dataobject/schema/do1"))
      .withQueryParam("tstamp", equalTo("100"))
      .willReturn(aResponse().withStatus(200).withBody(doc(do1, 100))))
    stubFor(put(urlPathMatching("/api/v1/dataobject/.*")).willReturn(aResponse().withStatus(200)))

    val result = ExportSync.sync(baseConfig(s"localfile:$srcDir", "uiBackend").copy(types = Seq(ExportType.Schema)))
    assert(result.transferred == 1)
    verify(1, putRequestedFor(backendPath("dataobject/schema/do1")).withQueryParam("tstamp", equalTo("200")))
    verify(0, putRequestedFor(backendPath("dataobject/schema/do1")).withQueryParam("tstamp", equalTo("100")))
  }

  test("download newer versions from uiBackend") {
    val tgtDir = newDir("tgt")
    wireMockServer.resetAll()
    stubFor(get(backendPath("dataobject/stats/do1/tstamps"))
      .willReturn(aResponse().withStatus(200).withBody("[100,200]")))
    Seq(100, 200).foreach { v =>
      stubFor(get(backendPath("dataobject/stats/do1"))
        .withQueryParam("tstamp", equalTo(v.toString))
        // the UI backend returns statistics wrapped
        .willReturn(aResponse().withStatus(200).withBody(s"""{"stats":${doc(do1, v)}}""")))
    }
    // all other requests are answered with 404 by WireMock, which means there are no versions

    val result = ExportSync.sync(baseConfig("uiBackend", s"localfile:$tgtDir").copy(types = Seq(ExportType.Stats)))
    assert(result.transferred == 2)
    val tgt = FileExportWriter(tgtDir)
    assert(tgt.listVersions(ExportType.Stats, do1) == Seq(100L, 200L))
    assert(tgt.readVersion(ExportType.Stats, do1, 200).map(JsonMethods.parse(_)).contains(JsonMethods.parse(doc(do1, 200))))
    assert(tgt.listVersions(ExportType.Stats, do2).isEmpty)
  }

  test("list versions of all DataObjects of uiBackend at once") {
    val (srcDir, tgtDir) = (newDir("src"), newDir("tgt"))
    writeDocs(FileExportWriter(srcDir), ExportType.Schema, do1, 100, 200)
    wireMockServer.resetAll()
    // the target needs only the latest version per DataObject
    stubFor(get(backendPath("dataobjects/schema/tstamps")).withQueryParam("latest", equalTo("true"))
      .willReturn(aResponse().withStatus(200).withBody("""{"do1":[100]}""")))
    stubFor(put(urlPathMatching("/api/v1/dataobject/.*")).willReturn(aResponse().withStatus(200)))
    stubFor(get(backendPath("dataobject/schema/do1")).withQueryParam("tstamp", equalTo("100"))
      .willReturn(aResponse().withStatus(200).withBody(doc(do1, 100))))

    val uploadResult = ExportSync.sync(baseConfig(s"localfile:$srcDir", "uiBackend").copy(types = Seq(ExportType.Schema)))
    assert(uploadResult.transferred == 1)
    verify(1, putRequestedFor(backendPath("dataobject/schema/do1")).withQueryParam("tstamp", equalTo("200")))
    verify(0, getRequestedFor(urlPathMatching("/api/v1/dataobject/.*/tstamps")))

    // the source needs all versions
    wireMockServer.resetAll()
    stubFor(get(backendPath("dataobjects/schema/tstamps")).withQueryParam("latest", absent())
      .willReturn(aResponse().withStatus(200).withBody("""{"do1":[200,100],"do2":[]}""")))
    Seq(100, 200).foreach { v =>
      stubFor(get(backendPath("dataobject/schema/do1")).withQueryParam("tstamp", equalTo(v.toString))
        .willReturn(aResponse().withStatus(200).withBody(doc(do1, v))))
    }
    val downloadResult = ExportSync.sync(baseConfig("uiBackend", s"localfile:$tgtDir").copy(types = Seq(ExportType.Schema)))
    assert(downloadResult.transferred == 2)
    assert(FileExportWriter(tgtDir).listVersions(ExportType.Schema, do1) == Seq(100L, 200L))
    verify(0, getRequestedFor(urlPathMatching("/api/v1/dataobject/.*/tstamps")))
  }

  test("upload state to uiBackend, download is not supported") {
    val srcDir = newDir("statesrc")
    val src = HadoopStateSyncEndpoint(srcDir.toString, new Configuration())
    src.writeState(stateJson("app1", 1, 1))
    src.writeState(stateJson("app1", 2, 1))
    wireMockServer.resetAll()
    stubFor(get(backendPath("workflow")).withQueryParam("application", equalTo("app1"))
      .willReturn(aResponse().withStatus(200).withBody("""[{"name":"app1","runId":1,"attemptId":1,"status":"SUCCEEDED"}]""")))
    stubFor(post(backendPath("state")).willReturn(aResponse().withStatus(200)))

    val uploadResult = ExportSync.sync(baseConfig(s"localfile:${newDir("src")}", "uiBackend").copy(types = Seq(ExportType.State), stateSource = Some(srcDir.toString)))
    assert(uploadResult.transferred == 1)
    verify(1, postRequestedFor(backendPath("state")))

    val ex = intercept[ConfigurationException](ExportSync.sync(baseConfig("uiBackend", s"localfile:${newDir("tgt")}")
      .copy(types = Seq(ExportType.State), stateSource = Some("uiBackend"), stateTarget = Some(newDir("statetgt").toString))))
    assert(ex.getMessage.contains("not supported"))
  }

  private def runsJson(runIds: Range) = runIds.map(id => s"""{"name":"app1","runId":$id,"attemptId":1}""").mkString("[", ",", "]")

  test("list runs of uiBackend page by page") {
    wireMockServer.resetAll()
    stubFor(get(backendPath("workflow")).withQueryParam("limit", equalTo("200")).withQueryParam("beforeRunId", absent())
      .willReturn(aResponse().withStatus(200).withBody(runsJson(300 until 100 by -1))))
    stubFor(get(backendPath("workflow")).withQueryParam("beforeRunId", equalTo("101")).withQueryParam("beforeAttemptId", equalTo("1"))
      .willReturn(aResponse().withStatus(200).withBody(runsJson(100 until 95 by -1))))
    stubFor(get(backendPath("workflow")).withQueryParam("limit", equalTo("1"))
      .willReturn(aResponse().withStatus(200).withBody(runsJson(300 to 300))))
    val client = BackendClient(Seq(configPath))
    assert(client.listRuns("app1").map(_.runId).sorted == (96 to 300))
    assert(client.latestRun("app1").contains(StateRunId(300, 1)))
  }

  test("list runs of uiBackend not supporting paging") {
    wireMockServer.resetAll()
    // the cursor is ignored and the latest 200 runs are returned for every request
    stubFor(get(backendPath("workflow")).willReturn(aResponse().withStatus(200).withBody(runsJson(300 until 100 by -1))))
    val client = BackendClient(Seq(configPath))
    assert(client.listRuns("app1").map(_.runId).sorted == (101 to 300))
  }

  private val dagexporterPath = getClass.getResource("/dagexporter").getPath
  private val descriptionPath = getClass.getResource("/dagexporter/description").getPath

  private def tempConf(content: String): String = {
    val file = Files.createTempFile("exportsync", ".conf")
    Files.write(file, content.getBytes("UTF-8"))
    file.toString
  }

  test("export config json") {
    val sdlConfig = ConfigLoader.loadConfigFromFilesystem(Seq(dagexporterPath), new Configuration())
    val actualJsonOutput = JsonMethods.parse(ExportSync.exportConfigJson(sdlConfig, Seq(dagexporterPath)))
    assert((actualJsonOutput \ "actions").children.size === 8)
    assert((actualJsonOutput \ "dataObjects").children.size === 15)
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_origin" \ "lineNumber" === JInt(112))
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_origin" \ "endLineNumber" === JNothing)
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_origin" \ "path" === JString("dagexporterTest.conf"))
    // column descriptions are not part of the exported config, they are applied to the catalog by
    // CatalogSchemaUpdater and merged into the schemas exported by a dry-run, see ColumnDescriptionParserTest.
    assert(actualJsonOutput \ "dataObjects" \ "dataObjectParquet6" \ "_columnDescriptions" === JNothing)
    assert(((actualJsonOutput \ "actions" \ "actionId6" \ "transformers")(0) \ "_parameters")(0) \ "name" === JString("session"))
    assert((actualJsonOutput \ "actions" \ "actionId8" \ "transformers")(0) \ "_sourceDoc" ===
      JString("Documentation for TestTransformer.  \nThis should be exported by ExportSync!"))
  }

  test("write config json to a local and a Hadoop directory") {
    val (localDir, hadoopDir) = (newDir("configlocal"), newDir("confighadoop"))
    ExportSync.main(Array("-c", dagexporterPath, "--types", "configJson", "-t", s"localfile:$localDir"))
    assert(Files.exists(localDir.resolve("exportedConfig.json")))
    ExportSync.main(Array("-c", dagexporterPath, "--types", "configJson", "-t", hadoopDir.toUri.toString))
    assert(Files.exists(hadoopDir.resolve("exportedConfig.json")))
  }

  test("upload config and descriptions to uiBackend") {
    val descriptionConf = tempConf(s"""global.descriptionPath = "$descriptionPath"""")
    wireMockServer.resetAll()
    stubFor(put(backendPath("config")).willReturn(aResponse().withStatus(200)))
    stubFor(get(backendPath("descriptions/list")).willReturn(aResponse().withStatus(200)
      .withBody("""[{"name":"/dataObjects/old.md","type":"text/markdown","size":1,"last_modified":"2026-01-01T00:00:00Z"}]""")))
    stubFor(post(urlPathMatching("/api/v1/descriptions/.*")).willReturn(aResponse().withStatus(200)))
    stubFor(delete(urlPathMatching("/api/v1/descriptions/.*")).willReturn(aResponse().withStatus(200)))

    val config = ExportSyncConfig(configPaths = Seq(configPath, descriptionConf), target = "uiBackend", types = Seq(ExportType.ConfigJson, ExportType.Description))
    val result = ExportSync.sync(config)
    assert(result.transferred == 3) // config, upload of dataObjectParquet6.md, deletion of old.md
    verify(1, putRequestedFor(backendPath("config")).withQueryParam("repo", equalTo("test")))
    verify(1, postRequestedFor(backendPath("descriptions/dataObjects/dataObjectParquet6.md")))
    verify(1, deleteRequestedFor(backendPath("descriptions/dataObjects/old.md")))
  }

  test("update search index of uiBackend") {
    wireMockServer.resetAll()
    stubFor(put(backendPath("config")).willReturn(aResponse().withStatus(200)))
    stubFor(post(backendPath("search/index")).willReturn(aResponse().withStatus(200).withBody("""{"built":true}""")))
    val config = ExportSyncConfig(configPaths = Seq(configPath), target = "uiBackend", types = Seq(ExportType.ConfigJson))
    val result = ExportSync.sync(config)
    assert(result == ExportSync.SyncResult(transferred = 1))
    verify(1, postRequestedFor(backendPath("search/index")).withQueryParam("version", equalTo(ExportSync.getAppVersion)))
    // disabled, or dry run
    ExportSync.sync(config.copy(updateSearchIndex = false))
    ExportSync.sync(config.copy(dryRun = true))
    verify(1, postRequestedFor(backendPath("search/index")))
    // not for types which are not indexed
    ExportSync.sync(config.copy(types = Seq(ExportType.Lineage), schemaSource = Some(s"localfile:${newDir("src")}")))
    verify(1, postRequestedFor(backendPath("search/index")))
  }

  test("update search index of latest configuration version when synchronizing schemas only") {
    val srcDir = newDir("src")
    writeDocs(FileExportWriter(srcDir), ExportType.Schema, do1, 100)
    wireMockServer.resetAll()
    stubFor(put(urlPathMatching("/api/v1/dataobject/.*")).willReturn(aResponse().withStatus(200)))
    stubFor(post(backendPath("search/index")).willReturn(aResponse().withStatus(200).withBody("""{"built":true}""")))
    val result = ExportSync.sync(baseConfig(s"localfile:$srcDir", "uiBackend").copy(types = Seq(ExportType.Schema)))
    assert(result.transferred == 1)
    verify(1, postRequestedFor(backendPath("search/index")).withQueryParam("version", absent()))
  }

  test("update column lineage index of uiBackend if lineage was transferred") {
    val srcDir = newDir("src")
    writeDocs(FileExportWriter(srcDir), ExportType.Lineage, do1, 100)
    wireMockServer.resetAll()
    stubFor(put(urlPathMatching("/api/v1/dataobject/.*")).willReturn(aResponse().withStatus(200)))
    stubFor(post(backendPath("lineage/index")).willReturn(aResponse().withStatus(200)))
    val config = baseConfig(s"localfile:$srcDir", "uiBackend").copy(types = Seq(ExportType.Lineage))
    assert(ExportSync.sync(config) == ExportSync.SyncResult(transferred = 1, missingInSource = 2))
    verify(1, postRequestedFor(backendPath("lineage/index")))
    // disabled
    ExportSync.sync(config.copy(updateLineageIndex = false))
    verify(1, postRequestedFor(backendPath("lineage/index")))
    // nothing transferred
    stubFor(get(backendPath("dataobject/lineage/do1/tstamps")).willReturn(aResponse().withStatus(200).withBody("[100]")))
    stubFor(get(backendPath("dataobject/lineage/do1")).willReturn(aResponse().withStatus(200).withBody(doc(do1, 100))))
    assert(ExportSync.sync(config).transferred == 0)
    verify(1, postRequestedFor(backendPath("lineage/index")))
  }

  test("search index not supported or failing") {
    wireMockServer.resetAll()
    stubFor(put(backendPath("config")).willReturn(aResponse().withStatus(200)))
    // a UI backend without search index answers 404, which is no error
    val config = ExportSyncConfig(configPaths = Seq(configPath), target = "uiBackend", types = Seq(ExportType.ConfigJson), stopOnError = false)
    assert(ExportSync.sync(config) == ExportSync.SyncResult(transferred = 1))
    // a failing update is
    stubFor(post(backendPath("search/index")).willReturn(aResponse().withStatus(500)))
    assert(ExportSync.sync(config) == ExportSync.SyncResult(transferred = 1, failed = 1))
    // local directories have no search index
    assert(ExportSync.sync(config.copy(target = s"localfile:${newDir("tgt")}")) == ExportSync.SyncResult(transferred = 1))
  }

  test("descriptions need global.descriptionPath and a target supporting files") {
    intercept[ConfigurationException](ExportSync.sync(ExportSyncConfig(configPaths = Seq(configPath), target = "uiBackend", types = Seq(ExportType.Description))))
    val descriptionConf = tempConf(s"""global.descriptionPath = "$descriptionPath"""")
    intercept[ConfigurationException](ExportSync.sync(ExportSyncConfig(configPaths = Seq(configPath, descriptionConf), target = s"localfile:${newDir("tgt")}", types = Seq(ExportType.Description))))
  }

  test("schemaSource defaults to global.dataObjectsSchemaSource") {
    val (srcDir, tgtDir) = (newDir("src"), newDir("tgt"))
    writeDocs(FileExportWriter(srcDir), ExportType.Schema, do1, 100)
    val schemaSourceConf = tempConf(s"""global.dataObjectsSchemaSource = "localfile:$srcDir"""")
    val result = ExportSync.sync(ExportSyncConfig(configPaths = Seq(configPath, schemaSourceConf), target = s"localfile:$tgtDir", types = Seq(ExportType.Schema)))
    assert(result.transferred == 1)
    // without global.dataObjectsSchemaSource, schemaSource is required
    intercept[ConfigurationException](ExportSync.sync(ExportSyncConfig(configPaths = Seq(configPath), target = s"localfile:$tgtDir", types = Seq(ExportType.Schema))))
  }
}
