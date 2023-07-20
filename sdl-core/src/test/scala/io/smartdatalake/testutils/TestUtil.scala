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
package io.smartdatalake.testutils

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import io.smartdatalake.app.{GlobalConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.util.misc.{SerializableHadoopConfiguration, SmartDataLakeLogger}
import io.smartdatalake.util.secrets.StringOrSecret
import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
import io.smartdatalake.workflow.action.SDLExecutionId
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.workflow.dataobject.{HiveTableDataObject, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
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
import java.math.BigDecimal
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDateTime}
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Utility methods for testing.
 */
object TestUtil extends SmartDataLakeLogger {

  def sparkSessionBuilder(additionalSparkProperties: Map[String,StringOrSecret] = Map()) : SparkSession.Builder = {
    // create builder
    val builder = additionalSparkProperties.foldLeft(SparkSession.builder()) {
      case (builder, config) => builder.config(config._1, config._2.resolve())
    }
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.shuffle.partitions", "2")
    //.config("spark.ui.enabled", "false") // we use this as webservice to test WebserviceFileDataObject
    // Configure hive metastore location
    // Note that "builder.enableHiveSupport()" is not needed to work with hive metastore. In fact enableHiveSupport doesn't work with JDK11+.
    val tmpDirOnFS = Files.createTempDirectory("derby-").toFile
    tmpDirOnFS.deleteOnExit()
    sys.props.put("derby.system.home", tmpDirOnFS.getAbsolutePath)
    builder.master("local")
  }

  // create SparkSession if needed
  lazy val session : SparkSession = sparkSessionBuilder().getOrCreate

  def getDefaultActionPipelineContext(implicit instanceRegistry: InstanceRegistry): ActionPipelineContext = {
    getDefaultActionPipelineContext(session) // initialize with Spark session incl. Hive support
  }

  def getDefaultActionPipelineContext(sparkSession: SparkSession)(implicit instanceRegistry: InstanceRegistry): ActionPipelineContext = {
    val defaultHadoopConf = new SerializableHadoopConfiguration(new Configuration())
    val globalConfig = GlobalConfig()
    val context = ActionPipelineContext("feedTest", "appTest", SDLExecutionId.executionId1, instanceRegistry, Some(LocalDateTime.now()), SmartDataLakeBuilderConfig("feedTest", Some("appTest")), phase = ExecutionPhase.Init, serializableHadoopConf = defaultHadoopConf, globalConfig = globalConfig)
    // reuse existing spark session
    globalConfig._sparkSession = Some(sparkSession)
    context
  }

  // write DataFrame to table
  def prepareHiveTable( table: Table, path: String, df: DataFrame, partitionCols: Seq[String] = Seq() ): Unit = {
    if (partitionCols.isEmpty) df.write.mode(SaveMode.Overwrite).option("path", path).saveAsTable(s"${table.fullName}")
    else df.write.mode(SaveMode.Overwrite).option("path", path).partitionBy(partitionCols:_*).saveAsTable(s"${table.fullName}")
  }

  // returns HiveTableDataObject which is created and provided data frame written to
  def createHiveTable(db: Option[String] = Some("default"),
                      schemaMin: Option[StructType] = None,
                      tableName: String, dirPath: String,
                      df: DataFrame, partitionCols: Seq[String] = Seq(), primaryKeyColumns: Option[Seq[String]] = None
                     )(implicit instanceRegistry: InstanceRegistry, context: ActionPipelineContext): HiveTableDataObject = {
    val table = Table(db=db,name=tableName,primaryKey=primaryKeyColumns)
    val path = dirPath+s"$tableName"
    val hTabDo = HiveTableDataObject(id=s"${tableName}DO",path=Some(path),schemaMin=schemaMin.map(SparkSchema),table=table)
    hTabDo.dropTable
    instanceRegistry.register(hTabDo)
    prepareHiveTable(table,path,df,partitionCols)
    hTabDo
  }

  def copyResourceToFile( resource: String, tgtFile: File): Unit = {
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(resource)
    assert(inputStream!=null, s"resource file $resource not found")
    FileUtils.copyInputStreamToFile(inputStream, tgtFile)
  }

  def setupSSHServer( port: Int, usr: String, pwd: String): SshServer = {
    val sshd = SshServer.setUpDefaultServer()
    sshd.setFileSystemFactory(new NativeFileSystemFactory())
    sshd.setPort(port)
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(Files.createTempDirectory("sshd").resolve("hostkey.ser")))
    sshd.setSubsystemFactories(List(new SftpSubsystemFactory().asInstanceOf[SubsystemFactory]).asJava)
    sshd.setPasswordAuthenticator(new PasswordAuthenticator() {
      override def authenticate(user: String, password: String, session: ServerSession): Boolean = user == usr && password == pwd
    })
    sshd.start()
    //Thread.sleep(1000000)
    // return
    sshd
  }

  /**
   * Setup simple webserver with given ports
   * Different stubs are generated automatically to answer different URLs with predefined return codes
   *
   * @param host bind address, usually localhost / 127.0.0.1
   * @param port port for http calls
   * @param httpsPort port for https calls
   * @return instance of [[WireMockServer]]
   */
  def setupWebservice(host: String, port: Int, httpsPort: Int): WireMockServer = {
    configureFor(host,port)
    val keystoreFile = this.getClass.getResource("/test_keystore.pkcs12").getFile
    val wireMockServer =
      new WireMockServer(
        wireMockConfig()
          .port(port)
          .httpsPort(httpsPort)
          .bindAddress(host)
          .keystorePath(keystoreFile)
          .keystorePassword("mytruststorepassword")
      )
    wireMockServer
      .start()

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

    wireMockServer

  }

  def printFailedTestResult(testName: String, arguments: Seq[DataFrame] = Seq())(actual: DataFrame)(expected: DataFrame): Unit = {
    def printDf(df: DataFrame): Unit = {
      logger.error(df.schema.simpleString)
      df.printSchema()
      df.orderBy(df.columns.head, df.columns.tail:_*).show(false)
    }

    logger.error(s"!!!! Test $testName Failed !!!")
    logger.error("   Arguments ")
    arguments.foreach(printDf)
    logger.error("   Actual ")
    printDf(actual)
    logger.error("   Expected ")
    printDf(expected)
    logger.error(s"  Do schemata equal? ${actual.schema.fields.toSet == expected.schema.fields.toSet}")
    logger.error(s"  Do cardinalities equal? ${actual.count() == expected.count()}")
    logger.error("   symmetric Difference ")
    actual.symmetricDifference(expected, "actual").show(false)
  }

  def testArgumentExpectedMapWithComment[K, V](experiendum: K => V, argExpMapComm: Map[(String, K), V]): Unit = {

    def logFailure(argument: K, actual: V, expected: V, comment: String): Unit = {
      logger.error("Test case failed !")
      logger.error(s"   argument = $argument")
      logger.error(s"   actual   = $actual")
      logger.error(s"   expected = $expected")
      logger.error(s"   comment  = $comment")
    }

    def checkKey(x: (String,K)): Boolean = x match {
      case (comment, argument) =>
        val actual = experiendum(argument)
        val expected = argExpMapComm(x)
        val result = actual == expected
        if (!result) {
          logFailure(argument, actual, expected, comment)
        }
        result
      case _ => throw new Exception(s"Something went wrong: checkKey called with parameter x=$x")
    }
    val results: Set[Boolean] = argExpMapComm.keySet.map(checkKey)
    assert(results.forall(p => p))
  }

  def testArgumentExpectedMap[K, V](experiendum: K => V, argExpMap: Map[K, V]): Unit = {
    def addEmptyComment(x : (K, V)): ((String, K), V) = x match {
      case (k, v) => (("", k), v)
    }
    val argExpMapWithReason: Map[(String, K), V] = argExpMap.map(addEmptyComment)
    testArgumentExpectedMapWithComment(experiendum, argExpMapWithReason)
  }


  // a few data frames
  def dfComplex: DataFrame = {
    import session.implicits._
    val rowsComplex: Seq[(Int, Seq[(String, String, Seq[String])])] = Seq(
      (1, Seq(("a", "A", Seq("a", "A")))),
      (2, Seq(("b", "B", Seq("b", "B")))),
      (3, Seq(("c", "C", Seq("c", "C")))),
      (4, Seq(("d", "D", Seq("d", "D")))),
      (5, Seq(("e", "E", Seq("e", "E"))))
    )
    rowsComplex.toDF("id","value")
  }

  def dfEmptyNilSchema: DataFrame = session.createDataFrame(Seq.empty[Row].asJava, StructType(Nil))
  def dfEmptyWithSchema: DataFrame = {
    import session.implicits._
    Seq.empty[String].toDF()
  }


  val nullableStringField: StructField = StructField("nullable", StringType, nullable = true)
  val notNullableStringField: StructField = StructField("notnullable", StringType, nullable = false)
  val nullableStructField: StructField = StructField("structnull", StructType(nullableStringField :: notNullableStringField :: Nil), nullable = true)
  val notNullableStructField: StructField = StructField("structnotnull", StructType(nullableStringField :: notNullableStringField :: Nil), nullable = false)
  val nullableArrayField: StructField = StructField("arrwithnull", ArrayType(StructType(nullableStringField :: notNullableStringField :: Nil), containsNull = true))
  val notNullableArrayField: StructField = StructField("arrwithoutnull", ArrayType(StructType(nullableStringField :: notNullableStringField :: Nil), containsNull = false))
  val nullableMapField: StructField = StructField("mapwithnull", MapType(IntegerType, StructType(nullableStringField :: notNullableStringField :: Nil), valueContainsNull = true))
  val notNullableMapField: StructField = StructField("mapwithoutnull", MapType(IntegerType, StructType(nullableStringField :: notNullableStringField :: Nil), valueContainsNull = false))
  def dfEmptyWithStructuredSchema: DataFrame = {
    session.createDataFrame(session.sparkContext.makeRDD(Seq.empty[Row]),
      StructType(
        nullableStringField
          :: notNullableStringField
          :: nullableStructField
          :: notNullableStructField
          :: notNullableArrayField
          :: nullableArrayField
          :: notNullableMapField
          :: nullableMapField
          :: Nil
      )
    )
  }

  def dfComplexWithNull: DataFrame = {
    import session.implicits._
    val rowsComplexWithNull: Seq[(Option[Int], Option[Seq[(String, String, Seq[String])]])] = Seq(
      (Some(1), Some(Seq(("a", "A", Seq("a", "A"))))),
      (Some(2), Some(Seq(("b", "B", Seq("b", "B"))))),
      (Some(3), Some(Seq(("c", "C", null: Seq[String])))),
      (Some(4), Some(Seq(("d", "D", Seq("d", "D"))))),
      (Some(5), None),
      (None, None)
    )
    rowsComplexWithNull.toDF("id", "value")
  }

  def dfHierarchy: DataFrame = {
    import session.implicits._
    val rowsHierarchy: Seq[(String, String)] = Seq(("a","ab"), ("a","ac"), ("ac","aca"), ("b","ba"),
      ("c","ca"), ("ca","caa"), ("ca","cab"), ("c","cb"), ("cb","X"), ("c","cc"), ("cc","X"), ("X","Y"), ("Y","Z"))
    rowsHierarchy.toDF("parent", "child")
  }

  def makeRowManyTypes(r: (Boolean,Int,Int,Int,Int,String,String,String,String,String,String,Double,Double,String,String,String) ): Row = {
    Row(r._1,r._2.byteValue(),r._3.shortValue(),r._4,r._5.longValue(), // BooleanType - LongType
      Decimal(new BigDecimal(r._6),2,0),
      Decimal(new BigDecimal(r._7),4,0),
      Decimal(new BigDecimal(r._8),10,0),
      Decimal(new BigDecimal(r._9),11,0),
      Decimal(new BigDecimal(r._10),4,3),
      Decimal(new BigDecimal(r._11),38,1),
      r._12.floatValue(),r._13,Date.valueOf(r._14),Timestamp.valueOf(r._15),r._16) // FloatType - StringType
  }
  val rowsManyTypes: List[(Boolean,Int,Int,Int,Int,String,String,String,String,String,String,Double,Double,String,String,String)] = List(
    (false,0,0,0,0,"0","0","0","0","0.0","0.0",0.0,0.0,"1970-01-01","1970-01-01 02:34:56.789","zero"),
    (true,127,32767,Int.MaxValue,Int.MaxValue,"99","9999","9999999999","99999999999","1.234","1234567890123456789012345678901234567.8",
      Float.MaxValue,Double.MaxValue,"2020-02-29","2020-02-29 12:34:56.789","maximal")
  )
  def dfManyTypes: DataFrame = {
    val schemaManyTypes: StructType = StructType(
      StructField("_boolean", BooleanType, nullable = true) ::
        StructField("_byte", ByteType, nullable = true) ::
        StructField("_short", ShortType, nullable = true) ::
        StructField("_integer", IntegerType, nullable = true) ::
        StructField("_long", LongType, nullable = true) ::
        StructField("_decimal_2_0" , DecimalType(2, 0), nullable = true) ::
        StructField("_decimal_4_0" , DecimalType(4, 0), nullable = true) ::
        StructField("_decimal_10_0", DecimalType(10, 0), nullable = true) ::
        StructField("_decimal_11_0", DecimalType(11, 0), nullable = true) ::
        StructField("_decimal_4_3", DecimalType(4, 3), nullable = true) ::
        StructField("_decimal_38_1", DecimalType(38, 1), nullable = true) ::
        StructField("_float", FloatType, nullable = true) ::
        StructField("_double", DoubleType, nullable = true) ::
        StructField("_date", DateType, nullable = true) ::
        StructField("_timestamp", TimestampType, nullable = true) ::
        StructField("_string", StringType, nullable = true) ::
        Nil)
    session.createDataFrame(rows=rowsManyTypes.map(makeRowManyTypes).asJava, schema=schemaManyTypes): DataFrame
  }

  def dfNonUnique: DataFrame = {
    import session.implicits._
    val rowsNonUnique: Seq[(String, String)] = Seq(("1let", "unilet"),
      ("2let", "doublet"), ("2let", "doublet"),
      ("3let", "triplet"), ("3let", "triplet"),("3let", "triplet"),
      ("4let", "quatriplet"), ("4let", "quatriplet"), ("4let", "quatriplet"), ("4let", "quatriplet"))
    rowsNonUnique.toDF("id", "value")
  }

  def dfNonUniqueWithNull: DataFrame = {
    import session.implicits._
    val rowsNonUnique: Seq[(String, Option[String])] = Seq(("0let", None),
      ("1let", Some("unilet")),
      ("2let", Some("doublet")), ("2let", Some("doublet")),
      ("3let", Some("triplet")), ("3let", Some("triplet")), ("3let", Some("triplet")),
      ("4let", Some("quatriplet")), ("4let", Some("quatriplet")), ("4let", Some("quatriplet")), ("4let", Some("quatriplet")))
    rowsNonUnique.toDF("id", "value")
  }

  def dfTwoCandidateKeys: DataFrame = {
    import session.implicits._
    val rowsTwoCandidateKeys: Seq[(String, String, Int, Int, Int, Double)] = Seq(
      ("a", "a", 1, 1, 1, 17.3),
      ("a", "b", 1, 1, 2, 17.3),
      ("b", "a", 1, 2, 1, 42.0),
      ("b", "b", 1, 2, 2, -3.14),
      ("b", "c", 2, 1, 1, -3.14)
    )
    rowsTwoCandidateKeys.toDF("string_id1", "string_id2", "int_id1", "int_id2", "int_id3", "x")
  }

  def arbitraryDataFrame(schema: StructType, nbRecords: Int = 100)(implicit session: SparkSession): DataFrame = {
    val nbOfArrayRecords = 3
    import scala.collection.JavaConverters._
    def arbitraryValue(dataType: DataType): Any = {
      dataType match {
        case IntegerType => Arbitrary.arbInt.arbitrary.sample.get
        case LongType => Arbitrary.arbLong.arbitrary.sample.get
        case StringType => Arbitrary.arbString.arbitrary.sample.get
        case FloatType => Arbitrary.arbFloat.arbitrary.sample.get
        case DoubleType => Arbitrary.arbDouble.arbitrary.sample.get
        case TimestampType => new Timestamp(Gen.choose(0L, Instant.now().toEpochMilli).sample.get) // arbDate creates dates too far in the past (negative millis), we use a custom generator therefore...
        case d: StructType => arbitraryRow(d.fields)
        case d: ArrayType => (1 to nbOfArrayRecords).map( x => arbitraryValue(d.elementType))
      }
    }
    def arbitraryRow(fields: Array[StructField]): Row = {
      val colValues = fields.map( f => arbitraryValue(f.dataType))
      Row.fromSeq(colValues)
    }
    val rows = (1 to nbRecords).map( x => arbitraryRow(schema.fields)).asJava
    session.createDataFrame(rows, schema)
  }
}


