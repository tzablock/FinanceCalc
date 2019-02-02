package com.stock.core

import com.stock.core.BusinessCalculations._
import com.stock.dataprocessing.SparkFactory
import org.apache.spark.sql.functions.{col, regexp_replace, split}
import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec

class BusinessCalculator(spark: SparkSession) {  //TODO make tests
  import spark.implicits._

  /**
    * One of three company documents Income Statement (what is company net income and how it's created)
    * @param df dataframe
    * @return dataframe with complete company data
    *         calculating net income
    */
  final def calculateCompanyIncomeStatement(df: DataFrame): DataFrame = {
    df.withColumn("incBefTax",calculateIncomeBeforeTaxUDF($"rev",$"revCosts"))
      .withColumn("tax",calculateTaxCols($"effectiveTaxRate",$"incBefTax"))
      .withColumn("netInc",calculateNetIncomeCols($"incBefTax",$"tax"))
  }

  /**
    *         calculating equity (company value when we kill/sell company today on parts), company value shouldn't go under equity if company have positiv net Income
    */
  final def calculateCompanyBalanceSheet(df: DataFrame): DataFrame = {
    df.withColumn("equity",calculateEquityUDF($"assets",$"liabilities"))
  }

  /**
    *         calculate risk/margin of safety (difference between it's market price and equity)
    */
  final def calculateBusinessRisk(df: DataFrame): DataFrame = {
    df.withColumn("risk",calculateRisk($"businessMarketPrice",$"equity"))
  }
  @tailrec
  final def calculateCompoundIncomeForYears(df: DataFrame,yearsNum: Int): DataFrame = {
    if (yearsNum>0){
      calculateCompoundIncomeForYears(df.withColumn(yearsNum + " yearCompVal",calculateCompoundIncome(lit(1000.0),lit(yearsNum),$"estimatedValueIncrease")),yearsNum-1)
    } else {
      df
    }
  }
  //TODO calculate compound income (make it generic for netInc and as well for increase in values stocs)



  /**
    * On column equivalent (performance test)
    */
  @deprecated
  final def calculateCompanyIncomeStatementUDF(df: DataFrame): DataFrame = {
    df.withColumn("incBefTax",calculateIncomeBeforeTaxUDF($"rev",$"revCosts"))
      .withColumn("tax",calculateTaxUDF($"effectiveTaxRate",$"incBefTax"))
      .withColumn("netInc",calculateNetIncomeUDF($"incBefTax",$"tax"))
  }
}
object BusinessCalculator{
  def apply(sf: SparkFactory): BusinessCalculator = new BusinessCalculator(sf.sparkSession())
}
