package com.stock.core

import com.stock.core.BusinessCalculationUDFs._
import com.stock.dataprocessing.SparkFactory
import org.apache.spark.sql.functions.{col, regexp_replace, split}
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class BusinessCalculator(spark: SparkSession) {
  import spark.implicits._
  def calculateBusinessInternals(ds: DataFrame): DataFrame = {
    ds.withColumn("incBefTax",calculateIncomeBeforeTaxUDF($"rev",$"revCosts"))
      .withColumn("tax",calculateTaxUDF($"effectiveTaxRate",$"incBefTax"))
      .withColumn("netInc",calculateNetIncomeUDF($"incBefTax",$"tax"))
  }

  /**
    * On column equivalent (performance test)
    */
  def calculateBusinessInternalsCols(ds: DataFrame): DataFrame = {
    ds.withColumn("incBefTax",calculateIncomeBeforeTaxUDF($"rev",$"revCosts"))
      .withColumn("tax",calculateTaxUDF($"effectiveTaxRate",$"incBefTax"))
      .withColumn("netInc",calculateNetIncomeUDF($"incBefTax",$"tax"))
  }
  def preOperations(df: DataFrame): DataFrame = {
    df.withColumn("revCosts",split(regexp_replace(col("revCosts"),"[\\[\\]]",""),"\\,"))
      .withColumn("revCosts",col("revCosts").cast(ArrayType(DoubleType)))
  }
}
object BusinessCalculator{
  def apply(sf: SparkFactory): BusinessCalculator = new BusinessCalculator(sf.sparkSession())
}
