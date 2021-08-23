package org.example

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.udf

object UserDefinedFunctions extends Serializable {

  def changeColType(df: org.apache.spark.sql.DataFrame, col: String, newType: String) = {
    df.withColumn(col, df(col).cast(typeMatch(newType)))
  }

  def typeMatch(input: String) = {
    import org.apache.spark.sql.types._
    input match {
      case "Int" => IntegerType
      case "String" => StringType
      case "Date" => DateType
      case "Double" => DoubleType
      case "Timestamp" => TimestampType
    }
  }

}
