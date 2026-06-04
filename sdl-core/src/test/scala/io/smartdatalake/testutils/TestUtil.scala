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
package io.smartdatalake.testutils

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.typesafe.config.ConfigFactory
import io.smartdatalake.app.{GlobalConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigParser, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.spark.SDLSparkExtension
import io.smartdatalake.util.spark.dataset.Equality
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.{RuntimeInfo, SDLExecutionId}
import io.smartdatalake.workflow.connection.{Connection, ScalaConnection, SparkClassicConnection}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject._
import io.smartdatalake.workflow.dataobject.file.FileRefDataObject
import io.smartdatalake.workflow.dataobject.generic.{Table, TableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.sshd.common.file.nativefs.NativeFileSystemFactory
import org.apache.sshd.server.SshServer
import org.apache.sshd.server.auth.password.PasswordAuthenticator
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider
import org.apache.sshd.server.session.ServerSession
import org.apache.sshd.server.subsystem.SubsystemFactory
import org.apache.sshd.sftp.server.SftpSubsystemFactory
import org.scalacheck.{Arbitrary, Gen}

import java.io.File
import java.nio.file.Files
import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Utility methods for testing.
 */
object TestUtil extends SmartDataLakeLogger with Equality {

  // extract keystore file from resource jar for wiremock server
  private lazy val wiremockKeyStoreFile = {
    val resource = "test_keystore.pkcs12"
    val keyStorePath = Files.createTempDirectory("test").resolve(resource)
    val inputStream = Option(getClass.getResourceAsStream("/" + resource))
      .getOrElse(throw new RuntimeException(s"Could not find resource $resource in classpath"))
    Files.copy(inputStream, keyStorePath)
    inputStream.close()
    keyStorePath.toString
  }

// TODO: merge with io.smartdatalake.util.spark.GetSession, to avoid code duplication.
//  Note that GetSession is in main sources, so it cannot depend on test sources,
//  but maybe we can move the common code to a separate object in main sources.
  def sparkSessionBuilder(additionalSparkProperties: Map[String, StringOrSecret] = Map()): SparkSession.Builder = {
    // create builder
    val builder = additionalSparkProperties.foldLeft(SparkSession.builder()) {
      case (builder, config) => builder.config(config._1, config._2.resolve())
    }
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.shuffle.partitions", "2")
      // .config("spark.ui.enabled", "false") // we use this as webservice to test WebserviceFileDataObject
      // add nodata spark extension
      .withExtensions(new SDLSparkExtension)
    // Configure hive metastore location
    // Note that "builder.enableHiveSupport()" is not needed to work with hive metastore. In fact enableHiveSupport doesn't work with JDK11+.
    val tmpDirOnFS = Files.createTempDirectory("derby-").toFile
    tmpDirOnFS.deleteOnExit()
    sys.props.put("derby.system.home", tmpDirOnFS.getAbsolutePath)
    builder.master("local")
  }

  // create SparkSession if needed
  lazy val session: SparkSession =
    SparkSubFeed._defaultSparkSession
      .getOrElse {
        val session = sparkSessionBuilder().getOrCreate()
        SparkSubFeed._defaultSparkSession = Some(session)
        session
      }

  val defaultSparkConnection: SparkClassicConnection = {
    implicit val dummyRegistry: InstanceRegistry = new InstanceRegistry
    // parse from config, so that connection._config value is filled for agent config serialization tests...
    ConfigParser.parseConfigObject[Connection](
      ConfigFactory.parseString(s"type = SparkClassicConnection, id = ${Environment.defaultEngineConnectionId}, master = local")
    ).asInstanceOf[SparkClassicConnection]
  }
  val defaultScalaConnection: ScalaConnection = {
    implicit val dummyRegistry: InstanceRegistry = new InstanceRegistry
    // parse from config, so that connection._config value is filled for agent config serialization tests...
    ConfigParser.parseConfigObject[Connection](
      ConfigFactory.parseString(s"type = ScalaConnection, id = ${Environment.defaultEngineConnectionId}")
    ).asInstanceOf[ScalaConnection]
  }

  def getDefaultActionPipelineContext(implicit instanceRegistry: InstanceRegistry): ActionPipelineContext = {
    // set a default spark connection in global config, to easily get spark engine connection in unit tests
    // note that also context.currentAction needs to be set; this is done in the unit test through sdlb.prepare/init/exec.
    val globalConfig = GlobalConfig(defaultSparkConnectionId = Some("default-spark"))
    // create context
    ActionPipelineContext(
      feed = "feedTest",
      application = "appTest",
      executionId = SDLExecutionId.executionId1,
      instanceRegistry = instanceRegistry,
      referenceTimestamp = Some(LocalDateTime.now()),
      appConfig = SmartDataLakeBuilderConfig("feedTest", Some("appTest")),
      phase = ExecutionPhase.Init,
      globalConfig = globalConfig
    )
  }

  // write DataFrame to table
  def prepareHiveTable(table: Table, path: String, df: DataFrame, partitionCols: Seq[String] = Seq()): Unit =
    if (partitionCols.isEmpty) df.write.mode(SaveMode.Overwrite).option("path", path).saveAsTable(s"${table.fullName}")
    else df.write.mode(SaveMode.Overwrite).option("path", path).partitionBy(partitionCols.toIndexedSeq: _*).saveAsTable(s"${table.fullName}")

  def copyResourceToFile(resource: String, tgtFile: File): Unit = {
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(resource)
    assert(inputStream != null, s"resource file $resource not found")
    FileUtils.copyInputStreamToFile(inputStream, tgtFile)
  }

  def setupSSHServer(port: Int, usr: String, pwd: String): SshServer = {
    val sshd = SshServer.setUpDefaultServer()
    sshd.setFileSystemFactory(new NativeFileSystemFactory())
    sshd.setPort(port)
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(Files.createTempDirectory("sshd").resolve("hostkey.ser")))
    sshd.setSubsystemFactories(List(new SftpSubsystemFactory().asInstanceOf[SubsystemFactory]).asJava)
    sshd.setPasswordAuthenticator(new PasswordAuthenticator() {
      override def authenticate(user: String, password: String, session: ServerSession): Boolean = user == usr && password == pwd
    })
    sshd.start()
    // Thread.sleep(1000000)
    // return
    sshd
  }

  /**
   * Setup simple webserver with given ports Different stubs are generated automatically to answer
   * different URLs with predefined return codes
   *
   * @param host
   *   bind address, usually localhost / 127.0.0.1
   * @param port
   *   port for http calls
   * @param httpsPort
   *   port for https calls
   * @return
   *   instance of [[WireMockServer]]
   */
  def startWebservice(host: String, port: Int, httpsPort: Int): WireMockServer = {
    configureFor(host, port)
    val wireMockServer =
      new WireMockServer(
        wireMockConfig()
          .port(port)
          .httpsPort(httpsPort)
          .bindAddress(host)
          .keystorePath(wiremockKeyStoreFile)
          .keystorePassword("mytruststorepassword")
          .asynchronousResponseEnabled(false)
      )
    wireMockServer
      .start()
    wireMockServer
  }

  def setupWebserviceStubs(): Unit = {
    stubFor(post(urlEqualTo("/good/post/no_auth"))
        .willReturn(aResponse().withBody("{{request.path.[0]}}"))
    )

    stubFor(get(urlEqualTo("/good/no_auth/"))
        .willReturn(aResponse().withStatus(200))
    )

    stubFor(get(urlMatching("/good/basic_auth/"))
        .withHeader("Authorization", equalTo("Basic ZnMxOmZyZWl0YWcyMDE3x"))
        .willReturn(ok("request looks good"))
    )

    stubFor(get(urlMatching("/good/client_id/"))
        .withHeader("Authorization", equalTo("Basic ZnMxOmZyZWl0YWcyMDE3x"))
        .willReturn(ok("request looks good"))
    )

    stubFor(get(urlMatching("/good/token/"))
        .withHeader("Authorization", equalTo("Bearer ZnMxOmZyZWl0YWcyMDE3x"))
        .willReturn(ok("request looks good"))
    )

    stubFor(get(urlMatching("/bad/*/"))
        .willReturn(aResponse.withStatus(404))
    )
  }

  // a few data frames
  val nullableStringField: StructField = StructField("nullable", StringType, nullable = true)
  val notNullableStringField: StructField = StructField("notnullable", StringType, nullable = false)
  val nullableStructField: StructField = StructField("structnull", StructType(nullableStringField :: notNullableStringField :: Nil), nullable = true)
  val notNullableStructField: StructField = StructField("structnotnull", StructType(nullableStringField :: notNullableStringField :: Nil), nullable = false)
  val nullableArrayField: StructField =
    StructField("arrwithnull", ArrayType(StructType(nullableStringField :: notNullableStringField :: Nil), containsNull = true))
  val notNullableArrayField: StructField =
    StructField("arrwithoutnull", ArrayType(StructType(nullableStringField :: notNullableStringField :: Nil), containsNull = false))
  val nullableMapField: StructField =
    StructField("mapwithnull", MapType(IntegerType, StructType(nullableStringField :: notNullableStringField :: Nil), valueContainsNull = true))
  val notNullableMapField: StructField =
    StructField("mapwithoutnull", MapType(IntegerType, StructType(nullableStringField :: notNullableStringField :: Nil), valueContainsNull = false))

  def arbitraryDataFrame(schema: StructType, nbRecords: Int = 100)(implicit session: SparkSession): DataFrame = {
    val nbOfArrayRecords = 3
    import scala.jdk.CollectionConverters._
    def arbitraryValue(dataType: DataType): Any =
      dataType match {
        case IntegerType   => Arbitrary.arbInt.arbitrary.sample.get
        case LongType      => Arbitrary.arbLong.arbitrary.sample.get
        case StringType    => Arbitrary.arbString.arbitrary.sample.get
        case FloatType     => Arbitrary.arbFloat.arbitrary.sample.get
        case DoubleType    => Arbitrary.arbDouble.arbitrary.sample.get
        case TimestampType => new Timestamp(Gen.choose(0L,
            Instant.now().toEpochMilli).sample.get) // arbDate creates dates too far in the past (negative millis), we use a custom generator therefore...
        case d: StructType => arbitraryRow(d.fields)
        case d: ArrayType  => (1 to nbOfArrayRecords).map(_ => arbitraryValue(d.elementType))
      }

    def arbitraryRow(fields: Array[StructField]): Row = {
      val colValues = fields.map(f => arbitraryValue(f.dataType)).toList
      Row.fromSeq(colValues)
    }

    val rows = (1 to nbRecords).map(_ => arbitraryRow(schema.fields)).asJava
    session.createDataFrame(rows, schema)
  }

  def getMetrics(runtimeInfo: RuntimeInfo, dataObjectId: DataObjectId): MetricsMap =
    runtimeInfo.results.find(_.dataObjectId == dataObjectId).get.metrics.getOrElse(Map())

  private lazy val pathToDeleteOnExit: mutable.Buffer[String] = {
    val buffer = mutable.Buffer[String]()
    // register hook to delete directories and files registered in buffer
    Runtime.getRuntime.addShutdownHook(new Thread(() => buffer.foreach(p => FileUtils.deleteQuietly(new File(p)))))
    buffer
  }

  def deleteOnExit(path: String): Unit =
    pathToDeleteOnExit.append(path)

  def createParquetDataObject(id: String)(implicit instanceRegistry: InstanceRegistry): ParquetFileDataObject = {
    val tempDir = Files.createTempDirectory("sdlb-test")
    val tempPath = tempDir.toAbsolutePath.toString
    TestUtil.deleteOnExit(tempPath)
    val dataObject = ParquetFileDataObject(id, path = tempPath)
    instanceRegistry.register(dataObject)
    dataObject
  }

  def registerDataObject[A <: DataObject](dataObject: A)(implicit instanceRegistry: InstanceRegistry, context: ActionPipelineContext): A = {
    dataObject match {
      case tableDataObject: TableDataObject  => tableDataObject.dropTable
      case fileDataObject: FileRefDataObject => fileDataObject.deleteAll
      case _                                 => ()
    }
    instanceRegistry.register(dataObject)
    dataObject
  }
}
