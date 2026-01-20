package io.smartdatalake.util

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.scalacheck.Gen
import org.scalacheck.Gen.{choose, nonEmptyListOf}
import org.slf4j.Logger

import java.io.FileNotFoundException
import java.text.SimpleDateFormat

trait TestTool extends Compare {
  ////////////////////////////////////////////
  ///// just some constants to test with /////
  ////////////////////////////////////////////
  protected final val zero: Short = 0.toShort
  protected final val one: Short = 1.toShort
  protected final val two: Short = 2.toShort
  protected final val three: Short = 3.toShort
  protected final val four: Short = 4.toShort
  protected final val ten: Short = 10.toShort
  protected final val oneOpt: Option[Short] = Some(one)
  protected final val twoOpt: Option[Short] = Some(two)

  protected final val myIntList: List[Int] = List(0, 1, 2, 3, 4, 1, 2, 3, 4, 2, 3, 4, 3, 4, 4)
  protected final val myIntVec: Vector[Int] = myIntList.toVector

  protected final val squareRootMap: Map[Double, Double] = Map(0d -> 0d, 0.0625d -> 0.25d, 0.25d -> 0.5d,
    1d -> 1d, 4d -> 2d, 16d -> 4d)

  protected def getCurrentTime: java.util.Date = java.util.Calendar.getInstance().getTime

  protected def getCurrentTimeString: String = new SimpleDateFormat("YY-MM-dd-HHmmss").format(getCurrentTime)

  /**
   *
   * @param cnt      number of doubles desired
   * @param minValue smallest double to return
   * @param maxValue largest double to return
   * @return a list of cnt-many equidistant doubles from minValue to maxValue
   */
  protected def getDoubleIter(cnt: Int, minValue: Double, maxValue: Double): Iterable[Double] = {
    if (cnt == 0) Iterable.empty[Double] else {
      val ns = 1 to cnt
      val scaleFactor = (maxValue - minValue) / (ns.max - ns.min)
      val shift = maxValue - scaleFactor * ns.max
      ns.map(n => scaleFactor * n + shift)
    }
  }


  /////////////////////////////////////////////////
  ///// generators for property based testing /////
  /////////////////////////////////////////////////
  protected final val testDouble: Gen[Double] = choose[Double](min = 0d - Math.scalb(1f, 16), max = Math.scalb(1f, 16))
  protected final val testFloat: Gen[Float] = choose[Float](min = 0f - Math.scalb(1f, 16), max = Math.scalb(1f, 16))
  protected final val testFloats: Gen[List[Float]] = nonEmptyListOf(g = testFloat)
  protected final val testPercentage: Gen[Double] = choose[Double](min = 0d, max = 1d)
  protected final val testMinCode: Gen[Byte] = choose[Byte](min = -128, max = 0)
  protected final val testMaxCode: Gen[Byte] = choose[Byte](min = 1, max = 127)
  protected final val testNatNum: Gen[Int] = choose[Int](min = 0, max = 1024)
  protected final val testNatNums: Gen[List[Int]] = nonEmptyListOf(g = testNatNum)
  protected final val testListNatNums: Gen[List[List[Int]]] = nonEmptyListOf(g = testNatNums)
  protected final val testPosInt: Gen[Int] = choose[Int](min = 1, max = 1024)
  protected final val testPosIntPair: Gen[(Int, Int)] = Gen.zip[Int, Int](g1 = testPosInt, g2 = testPosInt)
  protected final val testPosInts: Gen[List[Int]] = nonEmptyListOf(g = testPosInt)


  ////////////////////////////////
  ///// Testing with Structs /////
  ////////////////////////////////

  /**
   * creates Struct
   *
   * @param fields : fields as triple (name, data type, is nullable)
   * @return StructType
   */
  protected final def createStruct(fields: Array[(String, DataType, Boolean)]): StructType = StructType(
    fields.map(x => StructField(name = x._1, dataType = x._2: DataType, nullable = x._3))
  )

  /**
   * creates Struct with nullable fields
   *
   * @param fields : nullable fields as pair (name, data type)
   * @return StructType
   */
  protected final def createStruct(fields: Array[(String, DataType)]): StructType = createStruct(
    fields.map { case (fldName, dTyp) => (fldName, dTyp, true) }
  )

  /**
   * creates Struct with one field
   *
   * @param fieldName : name of field
   * @param fieldType : data type of field
   * @param nullable  : is field nullable ?
   * @return StructType
   */
  protected final def createStruct(fieldName: String, fieldType: DataType, nullable: Boolean = true): StructType = createStruct(Array((fieldName, fieldType, nullable)))

  def readResourceFile(filename: String): String = {
    val stream = Option(ClassLoader.getSystemClassLoader.getResourceAsStream(filename))
      .getOrElse(throw new FileNotFoundException(filename))
    val source = scala.io.Source.fromInputStream(stream)
    val content = source.getLines().mkString(sys.props("line.separator"))
    source.close
    // return value
    content
  }

  /**
   * testArgumentExpectedMap writes a log message
   * in case the expected value does not equal the actual
   *
   * @param experiendum map you want to test
   * @param argExpMap   map of (comment, input) -> expected output of provided map
   * @param logger      to write nice messages
   * @tparam K type of input values of map to test
   * @tparam V type of output values of map to test
   * @return booleans which indicate whether tests were successful
   */
  def testArgumentExpectedMap[K, V](experiendum: K => V, argExpMap: Map[K, V])
                                   (implicit logger: Logger): Map[K, Boolean] = {
    def addEmptyComment(x: (K, V)): ((String, K), V) = x match {
      case (k, v) => (("", k), v)
    }

    val argExpMapWithReason: Map[(String, K), V] = argExpMap.map(addEmptyComment)
    testArgumentExpectedMapWithComment(experiendum, argExpMapWithReason).map { case (k, v) => (k._2, v) }
  }

  /**
   * testArgumentExpectedMapWithComment writes a log message decorated with provided comment
   * in case the expected value does not equal the actual
   *
   * @param experiendum   map you want to test
   * @param argExpMapComm map of (comment, input) -> expected output of provided map
   * @param logger        to write nice messages
   * @tparam K type of input values of map to test
   * @tparam V type of output values of map to test
   * @return booleans which indicate whether tests were successful
   */
  def testArgumentExpectedMapWithComment[K, V](experiendum: K => V,
                                               argExpMapComm: Map[(String, K), V])
                                              (implicit logger: Logger): Map[(String, K), Boolean] = {
    def logFailureObject(argName: String, x: Any): Unit = {
      val printPrefix = s"   ${argName.padTo(8, " ").mkString("")} = "
      x match {
        case df: Dataset[_] =>
          logger.error(printPrefix)
          df.show(false)
        case x: Array[_] => logger.error(s"$printPrefix${x.mkString(", ")}")
        case x: Seq[_] => logger.error(s"$printPrefix${x.mkString(", ")}")
        //case x: scala.collection.GenSeq[_] => logger.error(s"$printPrefix${x.mkString(", ")}")
        case _ => logger.error(s"$printPrefix${x.toString}")
      }
    }

    def logFailure(argument: K, actual: V, expected: V, comment: String): Unit = {
      logger.error("Test case failed !")
      logFailureObject("argument", argument)
      logFailureObject("actual", actual)
      logFailureObject("expected", expected)
      if (comment.nonEmpty) logFailureObject("comment", comment)
    }

    def checkKey(x: (String, K)): Boolean = x match {
      case (comment, argument) =>
        val actual = experiendum(argument)
        val expected = argExpMapComm(x)
        val resultat = anyEqual(actual, expected)
        if (!resultat) logFailure(argument, actual, expected, comment)
        resultat
      case _ => throw new Exception(s"Something went wrong: checkKey called with parameter x=$x")
    }

    argExpMapComm.map { case (ck, _) => (ck, checkKey(ck)) }
  }

}
