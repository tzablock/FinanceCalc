package com.stock

import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BusinessAnalyst").master("local").getOrCreate()  // take it from properties
    import spark.implicits._
    val rd = spark.sparkContext.parallelize(List("[1,2,3,4]","ddd")).toDF
    rd.write.csv("/home/tzablock/IdeaProjects/coursera/StockPredictions/src/main/resources/examplessss")
  }
}
