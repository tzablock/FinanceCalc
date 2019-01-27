package com.stock.core

import com.stock.basicunits.Business
import com.stock.basicunits.Types.CurrencyType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
object BusinessCalculationUDFs {
  /**
    *
    * @param r Total Revenue
    * @param rc Cost of revenue total (have to be Seq because ArrayType is casted only on seq not array or list)
    * @return Income before taxes
    */
  private val calculateIncomeBeforeTax = (r: CurrencyType, rc: Seq[CurrencyType]) => r - rc.sum
  /**
    * On column equivalent (performance test)
    */
  private val calculateIncomeBeforeTaxCols = (r: Column, rc: Column) => r - sum(rc)
  /**
    *
    * @param etr effective tax rate
    * @param ibt income before tax
    * @return tax
    */
  private val calculateTax = (etr: Double,ibt: CurrencyType) => ibt * etr
  /**
    * On column equivalent (performance test)
    */
  private val calculateTaxCols = (etr: Double,ibt: CurrencyType) => ibt * etr

  /**
    *
    * @param ibt Income before taxes
    * @param t Taxes
    * @return Earnings
    */
  private val calculateNetIncome = (ibt: CurrencyType, t: CurrencyType) => ibt - t
  /**
    * On column equivalent (performance test)
    */
  private val calculateNetIncomeCols = (ibt: CurrencyType, t: CurrencyType) => ibt - t

  val calculateIncomeBeforeTaxUDF = udf(calculateIncomeBeforeTax)
  val calculateTaxUDF = udf(calculateTax)
  val calculateNetIncomeUDF = udf(calculateNetIncome)
}
