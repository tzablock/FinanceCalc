package com.stock.dataprocessing

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class SparkFactory(spark: SparkSession) { //TODO make test
  def sparkSession(): SparkSession = spark

  private def checkType(v: Any): DataType = v match {
    case (s: String) :: tail => StringType
    case (i:Int) :: tail => IntegerType
    case (d: Double) :: tail => DoubleType
  }

  def createDF(m: Map[String,List[Any]]): DataFrame = {
    val schema = new StructType(m.map{case (k,v) => StructField(k,checkType(v))}.toArray)
    val values = m.values
    val rows = values.head.indices.map(i => Row.fromSeq(values.map(v => v(i)).toSeq)).toList
    spark.createDataFrame(spark.sparkContext.parallelize(rows),schema)
  }
}

object SparkFactory{
  def apply(): SparkFactory = new SparkFactory(createSparkSession())

  def createSparkSession():SparkSession = {
    SparkSession.builder()
      .appName("BusinessAnalyst")
      .master("local")
      .getOrCreate()   //todo take master etc from properties it from properties, add mode
  }
}

