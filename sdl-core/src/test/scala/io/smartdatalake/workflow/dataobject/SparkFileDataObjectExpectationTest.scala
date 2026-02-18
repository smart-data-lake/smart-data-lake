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

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.util.dag.TaskFailedException
import io.smartdatalake.util.misc.LogUtil.getRootCause
import io.smartdatalake.workflow.dataobject.expectation.{ExpectationValidationException, UniqueKeyExpectation}
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class SparkFileDataObjectExpectationTest extends AnyFunSuite with BeforeAndAfter {

  protected implicit val session: SparkSession = TestUtil.session

  import session.implicits._

  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)

  before {
    instanceRegistry.clear()
  }

  test("ParquetFileDataObject: succeed with unique keys using UniqueKeyExpectation") {
    val tempDir = Files.createTempDirectory("sdlb-test")
    val tempPath = tempDir.toAbsolutePath.toString

    // Create ParquetFileDataObject with UniqueKeyExpectation
    val dataObject = ParquetFileDataObject(
      id = "test-parquet",
      path = tempPath,
      expectations = Seq(UniqueKeyExpectation("uniqueKeyTest", key = Seq("id"), approximate = false))
    )

    // Create test data with unique keys
    val df = Seq(
      (1, "alice", 100),
      (2, "bob", 200),
      (3, "charlie", 300)
    ).toDF("id", "name", "amount")

    // Write should succeed
    val metrics = dataObject.writeSparkDataFrame(df, Seq())(contextExec)
    
    // Verify metrics include expectation validation
    assert(metrics.contains("count"))
    assert(metrics("count") == 3)
    
    // Cleanup
    dataObject.deleteAll
    Files.delete(tempDir)
  }

  test("ParquetFileDataObject: fail with duplicate keys using UniqueKeyExpectation") {
    val tempDir = Files.createTempDirectory("sdlb-test")
    val tempPath = tempDir.toAbsolutePath.toString

    // Create ParquetFileDataObject with UniqueKeyExpectation
    val dataObject = ParquetFileDataObject(
      id = "test-parquet-dup",
      path = tempPath,
      expectations = Seq(UniqueKeyExpectation("uniqueKeyTest", key = Seq("id"), approximate = false))
    )

    // Create test data with duplicate keys
    val df = Seq(
      (1, "alice", 100),
      (2, "bob", 200),
      (1, "alice_duplicate", 300)  // Duplicate id=1
    ).toDF("id", "name", "amount")

    // Write should fail due to duplicate keys
    val ex = intercept[ExpectationValidationException] {
      dataObject.writeSparkDataFrame(df, Seq())(contextExec)
    }
    
    assert(ex.getMessage.contains("uniqueKeyTest"))
    assert(ex.getMessage.contains("failed"))
    
    // Cleanup
    dataObject.deleteAll()(contextInit)
    Files.delete(tempDir)
  }

  test("CsvFileDataObject: succeed with unique keys using UniqueKeyExpectation") {
    val tempDir = Files.createTempDirectory("sdlb-test")
    val tempPath = tempDir.toAbsolutePath.toString

    // Create CsvFileDataObject with UniqueKeyExpectation
    val dataObject = CsvFileDataObject(
      id = "test-csv",
      path = tempPath,
      expectations = Seq(UniqueKeyExpectation("uniqueKeyTest", key = Seq("customer_id"), approximate = false))
    )

    // Create test data with unique keys
    val df = Seq(
      ("C001", "Alice", "USA"),
      ("C002", "Bob", "UK"),
      ("C003", "Charlie", "Canada")
    ).toDF("customer_id", "name", "country")

    // Write should succeed
    val metrics = dataObject.writeSparkDataFrame(df, Seq())(contextExec)
    
    // Verify metrics include expectation validation
    assert(metrics.contains("count"))
    assert(metrics("count") == 3)
    
    // Cleanup
    dataObject.deleteAll
    Files.delete(tempDir)
  }

  test("ParquetFileDataObject: constraints validation with valid data") {
    val tempDir = Files.createTempDirectory("sdlb-test")
    val tempPath = tempDir.toAbsolutePath.toString

    // Create ParquetFileDataObject with constraint
    val dataObject = ParquetFileDataObject(
      id = "test-parquet-constraint",
      path = tempPath,
      constraints = Seq(Constraint("amountPositive", None, "amount > 0"))
    )

    // Create test data that satisfies constraint
    val df = Seq(
      (1, "alice", 100),
      (2, "bob", 200),
      (3, "charlie", 300)
    ).toDF("id", "name", "amount")

    // Write should succeed
    val metrics = dataObject.writeSparkDataFrame(df, Seq())(contextExec)
    assert(metrics.contains("records_written"))
    
    // Cleanup
    dataObject.deleteAll
    Files.delete(tempDir)
  }

  test("ParquetFileDataObject: constraints validation with invalid data") {
    val tempDir = Files.createTempDirectory("sdlb-test")
    val tempPath = tempDir.toAbsolutePath.toString

    // Create ParquetFileDataObject with constraint
    val dataObject = ParquetFileDataObject(
      id = "test-parquet-constraint-fail",
      path = tempPath,
      constraints = Seq(Constraint("amountPositive", None, "amount > 0"))
    )

    // Create test data that violates constraint
    val df = Seq(
      (1, "alice", 100),
      (2, "bob", -50),  // Violates constraint: amount must be > 0
      (3, "charlie", 300)
    ).toDF("id", "name", "amount")

    // Write should fail due to constraint violation
    val ex = intercept[Exception] {
      dataObject.writeSparkDataFrame(df, Seq())(contextExec)
    }
    
    assert(ex.getMessage.contains("Constraint") || ex.getMessage.contains("amountPositive"))
    
    // Cleanup
    dataObject.deleteAll()(contextInit)
    Files.delete(tempDir)
  }
}
