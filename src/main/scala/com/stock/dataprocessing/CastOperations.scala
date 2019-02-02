package com.stock.dataprocessing

import org.apache.spark.sql.functions.{col, regexp_replace, split}
import org.apache.spark.sql.types.{AnyDataType, ArrayType, DataType, DoubleType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

class CastOperations(df: DataFrame) { //TODO make tests
  private val sparkSqlType: Map[String,DataType] = Map("double" -> DoubleType)

  /**
    *
    * @param df dataframe with array column as string
    * @return dataframe with casted column array<double>
    *           replace array column which need to be represent in csv as string into array<double>
    */
  def castColumnToArray(colName: String, colType: String): CastOperations = {
    val mdf = df.withColumn(colName, split(regexp_replace(col(colName), "[\\[\\]]", ""), "\\,"))
                .withColumn(colName, col(colName).cast(ArrayType(sparkSqlType(colType))))
    CastOperations(mdf)
  }

  def dataFrame():DataFrame = df
}
object CastOperations{
  def apply(df: DataFrame): CastOperations = new CastOperations(df)
}
