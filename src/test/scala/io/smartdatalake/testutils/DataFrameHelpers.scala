package org.apache.spark.sql

object DataFrameHelpers {
  def showString[T](df: Dataset[T], _numRows: Int, truncate: Int = 20, vertical: Boolean = false): String = {
    df.showString(_numRows, truncate, vertical)
  }
}