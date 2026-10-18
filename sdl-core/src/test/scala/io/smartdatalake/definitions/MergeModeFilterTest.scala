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
package io.smartdatalake.definitions

import com.typesafe.config.ConfigFactory
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.config.{ConfigParser, ConfigurationException, InstanceRegistry}
import io.smartdatalake.testutils.plainScala.{MockScalaDataObject, ScalaTestUtil}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.{Action, HistorizeAction, UpsertAction}
import io.smartdatalake.workflow.connection.jdbc.DefaultJdbcCatalog
import io.smartdatalake.workflow.dataframe.DataFrameFunctions
import io.smartdatalake.workflow.dataframe.plainScala.{ScalaDataFrame, ScalaSubFeed}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}
import java.time.LocalDate

class MergeModeFilterTest extends AnyFunSuite {

  private val actionId = ActionId("test")
  private val functions = ScalaSubFeed.asInstanceOf[DataFrameFunctions]

  private def dialect(isSparkSql: Boolean = true, quoteFn: String => String = identity, stringRange: Boolean = true) = new MergeFilterDialect {
    override def quote(column: String): String = quoteFn(column)
    override def literal(value: Any): Option[String] = MergeModeFilter.renderLiteral(value, isSparkSql)
    override def supportsStringRange: Boolean = stringRange
  }

  private val df = ScalaDataFrame.fromData(Seq(Seq(1, "B", 10), Seq(2, "D", 20), Seq(3, "C", null)), Seq("id", "dt", "value"))

  test("renderLiteral escapes strings depending on dialect") {
    assert(MergeModeFilter.renderLiteral("it's", isSparkSql = true).contains("'it\\'s'"))
    assert(MergeModeFilter.renderLiteral("a\\b", isSparkSql = true).contains("'a\\\\b'"))
    assert(MergeModeFilter.renderLiteral("it's", isSparkSql = false).contains("'it''s'"))
    assert(MergeModeFilter.renderLiteral("a\\b", isSparkSql = false).contains("'a\\b'"))
  }

  test("renderLiteral renders typed literals and rejects unsupported values") {
    assert(MergeModeFilter.renderLiteral(42, isSparkSql = true).contains("42"))
    assert(MergeModeFilter.renderLiteral(42L, isSparkSql = true).contains("42"))
    assert(MergeModeFilter.renderLiteral(new java.math.BigDecimal("1E+3"), isSparkSql = true).contains("1000"))
    assert(MergeModeFilter.renderLiteral(true, isSparkSql = true).contains("TRUE"))
    assert(MergeModeFilter.renderLiteral(Date.valueOf("2026-01-02"), isSparkSql = true).contains("DATE '2026-01-02'"))
    assert(MergeModeFilter.renderLiteral(LocalDate.parse("2026-01-02"), isSparkSql = false).contains("DATE '2026-01-02'"))
    assert(MergeModeFilter.renderLiteral(Timestamp.valueOf("2026-01-02 10:11:12.5"), isSparkSql = true).contains("TIMESTAMP '2026-01-02 10:11:12.5'"))
    assert(MergeModeFilter.renderLiteral(Double.NaN, isSparkSql = true).isEmpty)
    assert(MergeModeFilter.renderLiteral(Seq(1), isSparkSql = true).isEmpty)
  }

  test("JDBC literals depend on the database") {
    val defaultCatalog = new DefaultJdbcCatalog(null, "jdbc:postgresql://host/db")
    assert(defaultCatalog.sqlLiteral("it's a\\b").contains("'it''s a\\b'"))
    assert(defaultCatalog.sqlLiteral(Date.valueOf("2026-01-02")).contains("DATE '2026-01-02'"))
    assert(defaultCatalog.sqlLiteral(true).isEmpty)
    val sqlServerCatalog = new DefaultJdbcCatalog(null, "jdbc:sqlserver://host")
    assert(sqlServerCatalog.sqlLiteral(Date.valueOf("2026-01-02")).contains("CAST('2026-01-02' AS DATE)"))
    assert(sqlServerCatalog.sqlLiteral(Timestamp.valueOf("2026-01-02 10:11:12")).contains("CAST('2026-01-02 10:11:12.0' AS DATETIME2)"))
  }

  test("range bounds are widened for timestamps and floating point numbers") {
    val ts = Timestamp.valueOf("2026-01-03 10:00:00")
    assert(MergeModeFilter.widenLower(ts).contains(Timestamp.valueOf("2026-01-01 10:00:00")))
    assert(MergeModeFilter.widenUpper(ts).contains(Timestamp.valueOf("2026-01-05 10:00:00")))
    // a float compared as double must be within the rendered bounds
    val f = 0.1f
    assert(MergeModeFilter.widenLower(f).get.asInstanceOf[Double] < f.toDouble)
    assert(MergeModeFilter.widenUpper(f).get.asInstanceOf[Double] > f.toDouble)
    assert(MergeModeFilter.widenLower(5).contains(5))
  }

  test("timestamp bounds near the limits of the timestamp range are unbounded") {
    assert(MergeModeFilter.widenUpper(Timestamp.valueOf("9999-12-31 00:00:00")).isEmpty)
    assert(MergeModeFilter.widenLower(Timestamp.valueOf("0001-01-02 00:00:00")).isEmpty)
    val dfTs = ScalaDataFrame.fromData(Seq(Seq(1, Timestamp.valueOf("2026-01-03 00:00:00")), Seq(2, Timestamp.valueOf("9999-12-31 00:00:00"))), Seq("id", "valid_to"))
    assert(MergeModeFilter().createPredicate(actionId, Seq("valid_to"), dfTs, Seq(), dialect())
      .contains("existing.valid_to >= TIMESTAMP '2026-01-01 00:00:00.0'"))
  }

  test("table alias is not replaced in quoted literals and identifiers") {
    assert(SaveModeMergeExpressions.replaceAlias("existing.a = 'existing.x' AND nonexisting.b = 1", "existing", "tgt")
      == "tgt.a = 'existing.x' AND nonexisting.b = 1")
    assert(SaveModeMergeExpressions.replaceAlias("existing.a = 'it\\'s existing.x' OR existing.b = 'a''existing.c'", "existing", "tgt")
      == "tgt.a = 'it\\'s existing.x' OR tgt.b = 'a''existing.c'")
    assert(SaveModeMergeExpressions.replaceAlias("(existing.`a b` > 1)", "existing", "tgt") == "(tgt.`a b` > 1)")
  }

  test("filter columns default to partition columns") {
    assert(MergeModeFilter().getFilterColumns(actionId, MockScalaDataObject("do1", partitions = Seq("dt"))) == Seq("dt"))
    assert(MergeModeFilter(Seq("x")).getFilterColumns(actionId, MockScalaDataObject("do1", partitions = Seq("dt"))) == Seq("x"))
    intercept[ConfigurationException](MergeModeFilter().getFilterColumns(actionId, MockScalaDataObject("do1")))
  }

  test("partition values usable for an exact predicate") {
    val filter = MergeModeFilter()
    assert(filter.getExactPartitionValues(Seq("dt"), df, Seq(PartitionValues(Map("dt" -> "B")))) == Seq(PartitionValues(Map("dt" -> "B"))))
    // an entry without filter column selects all values, no exact predicate possible
    assert(filter.getExactPartitionValues(Seq("dt"), df, Seq(PartitionValues(Map("dt" -> "B")), PartitionValues(Map("other" -> "x")))).isEmpty)
    // integral column with numeric string values
    assert(filter.getExactPartitionValues(Seq("id"), df, Seq(PartitionValues(Map("id" -> "1")))) == Seq(PartitionValues(Map("id" -> "1"))))
    // integral column with non-numeric values can not be compared exactly
    assert(filter.getExactPartitionValues(Seq("id"), df, Seq(PartitionValues(Map("id" -> "x")))).isEmpty)
    // partition values are reduced to filter columns
    assert(filter.getExactPartitionValues(Seq("dt"), df, Seq(PartitionValues(Map("dt" -> "B", "other" -> "x")))) == Seq(PartitionValues(Map("dt" -> "B"))))
  }

  test("exact predicate from partition values") {
    val filter = MergeModeFilter()
    assert(filter.createPredicate(actionId, Seq("dt"), df, Seq(PartitionValues(Map("dt" -> "B"))), dialect())
      .contains("existing.dt = 'B'"))
    assert(filter.createPredicate(actionId, Seq("dt"), df, Seq(PartitionValues(Map("dt" -> "B")), PartitionValues(Map("dt" -> "C"))), dialect())
      .contains("((existing.dt = 'B') OR (existing.dt = 'C'))"))
    assert(filter.createPredicate(actionId, Seq("dt"), df, Seq(PartitionValues(Map("dt" -> null))), dialect())
      .contains("existing.dt IS NULL"))
    // values are rendered with the type of the column
    assert(filter.createPredicate(actionId, Seq("id"), df, Seq(PartitionValues(Map("id" -> "1"))), dialect())
      .contains("existing.id = 1"))
  }

  test("range predicate for columns not covered by partition values") {
    val filter = MergeModeFilter()
    assert(filter.createPredicate(actionId, Seq("dt"), df, Seq(), dialect())
      .contains("existing.dt BETWEEN 'B' AND 'D'"))
    assert(filter.createPredicate(actionId, Seq("id"), df, Seq(), dialect())
      .contains("existing.id BETWEEN 1 AND 3"))
  }

  test("no range predicate on string columns if not supported by dialect") {
    val filter = MergeModeFilter()
    assert(filter.createPredicate(actionId, Seq("dt"), df, Seq(), dialect(stringRange = false)).isEmpty)
    assert(filter.createPredicate(actionId, Seq("dt", "id"), df, Seq(), dialect(stringRange = false))
      .contains("existing.id BETWEEN 1 AND 3"))
    // target column with non-binary collation
    assert(filter.createPredicate(actionId, Seq("dt"), df, Seq(), dialect(), collatedColumns = Set("dt")).isEmpty)
    assert(Seq("string", "varchar(10)", "text", "nvarchar").forall(MergeModeFilter.isStringType) && !MergeModeFilter.isStringType("int"))
    assert(MergeModeFilter.isCollated("string collate utf8_lcase") && !MergeModeFilter.isCollated("string collate utf8_binary") && !MergeModeFilter.isCollated("string"))
  }

  test("range predicate handles null values") {
    val filter = MergeModeFilter()
    assert(filter.createPredicate(actionId, Seq("value"), df, Seq(), dialect())
      .contains("(existing.value IS NULL OR existing.value BETWEEN 10 AND 20)"))
    val dfOnlyNull = ScalaDataFrame.fromData(Seq(Seq(1, "B"), Seq(2, null)), Seq("id", "dt")).filter(functions.col("id") === functions.lit(2))
    assert(filter.createPredicate(actionId, Seq("dt"), dfOnlyNull, Seq(), dialect())
      .contains("existing.dt IS NULL"))
  }

  test("exact and range predicates are combined, columns are quoted") {
    val filter = MergeModeFilter()
    assert(filter.createPredicate(actionId, Seq("dt", "id"), df, Seq(PartitionValues(Map("dt" -> "B"))), dialect(quoteFn = c => s"`$c`"))
      .contains("existing.`dt` = 'B' AND existing.`id` BETWEEN 1 AND 3"))
  }

  test("no predicate for empty DataFrame") {
    assert(MergeModeFilter().createPredicate(actionId, Seq("dt"), df.limit(0), Seq(), dialect()).isEmpty)
  }

  test("missing filter column fails with ConfigurationException") {
    intercept[ConfigurationException](MergeModeFilter().createPredicate(actionId, Seq("unknown"), df, Seq(), dialect()))
  }

  test("predicate is combined with an existing additional merge predicate") {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    val output = MockScalaDataObject("tgt", primaryKey = Some(Seq("id")))
    output.writeDataFrame(df, Seq()) // output table must exist
    val options = SaveModeMergeOptions(additionalMergePredicate = Some("existing.value > 0"))
    val (_, finalOptions) = MergeModeFilter(Seq("id"))
      .writeWithPredicate(actionId, output, df, Seq(), options, usePartitionValues = true)((d, o) => (d, o))
    assert(finalOptions.additionalMergePredicate.contains("(existing.value > 0) AND (existing.id BETWEEN 1 AND 3)"))
  }

  test("no predicate if output table does not exist yet") {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    implicit val context: ActionPipelineContext = ScalaTestUtil.getDefaultActionPipelineContext
    val output = MockScalaDataObject("tgt", primaryKey = Some(Seq("id")))
    val (_, finalOptions) = MergeModeFilter(Seq("id"))
      .writeWithPredicate(actionId, output, df, Seq(), SaveModeMergeOptions(), usePartitionValues = true)((d, o) => (d, o))
    assert(finalOptions.additionalMergePredicate.isEmpty)
  }

  private def parseAction(actionConfig: String, tgtPartitions: Seq[String] = Seq()): Action = {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    instanceRegistry.register(ScalaTestUtil.defaultScalaConnection)
    instanceRegistry.register(MockScalaDataObject("src"))
    instanceRegistry.register(MockScalaDataObject("tgt", partitions = tgtPartitions, primaryKey = Some(Seq("id"))))
    ConfigParser.parseConfigObject[Action](ConfigFactory.parseString(s"id = act, inputId = src, outputId = tgt, $actionConfig"))
  }

  test("parse mergeModeFilter from config") {
    val upsert = parseAction("type = UpsertAction, mergeModeFilter {}", tgtPartitions = Seq("dt")).asInstanceOf[UpsertAction]
    assert(upsert.mergeModeFilter.contains(MergeModeFilter()))
    val historize = parseAction("type = HistorizeAction, mergeModeFilter { columns = [rating] }").asInstanceOf[HistorizeAction]
    assert(historize.mergeModeFilter.contains(MergeModeFilter(Seq("rating"))))
    assert(parseAction("type = UpsertAction").asInstanceOf[UpsertAction].mergeModeFilter.isEmpty)
  }

  test("mergeModeFilter without columns on unpartitioned output fails when parsing config") {
    val ex = intercept[Exception](parseAction("type = UpsertAction, mergeModeFilter {}"))
    assert(Iterator.iterate[Throwable](ex)(_.getCause).takeWhile(_ != null).exists(_.getMessage.contains("mergeModeFilter needs columns")))
  }
}
