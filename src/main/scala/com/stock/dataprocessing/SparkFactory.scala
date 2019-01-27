package com.stock.dataprocessing

import org.apache.spark.sql.SparkSession

class SparkFactory(spark: SparkSession) {
  def sparkSession(): SparkSession = spark
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

