package com.stock.core

import com.stock.basicunits.Business
import com.stock.basicunits.Types.CurrencyType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.annotation.tailrec
object BusinessCalculations { //TODO make tests
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
  @deprecated
  private val calculateTax = (etr: Double,ibt: CurrencyType) => ibt * etr

  /**
    *
    * @param etr effective tax rate
    * @param ibt income before tax
    * @return tax
    */
  val calculateTaxCols = (etr: Column,ibt: Column) => ibt * etr

  /**
    * On column equivalent (performance test)
    */
  @deprecated
  private val calculateNetIncome = (ibt: CurrencyType, t: CurrencyType) => ibt - t

  /**
    *
    * @param ibt Income before taxes
    * @param t Taxes
    * @return Earnings
    */
  val calculateNetIncomeCols = (ibt: Column, t: Column) => ibt - t

  /**
    * @param ass assets
    * @param ass liabilities
    * @return equity
    */
  private val calculateEquity = (ass: Seq[CurrencyType], lia: Seq[CurrencyType]) => ass.sum - lia.sum

  /**
    * @param bmPrice business market price - price for which business is on sale now on market
    * @param equity - value of business when we kill it
    *               calculate risk/margin of safety
    */
  val calculateRisk = (bmPrice: Column,equity: Column) => bmPrice - equity
  /**
    * @param initAmt amount of money invested initially  (Double)
    * @param period time of investment  (Int)
    * @param interestRate interest rate in % annually (Double)
    * @return tax
    */
  @tailrec
  val calculateCompoundIncome: (Column,Column,Column) => Column = (initAmt: Column, period: Column, interestRate: Column) => {
    if (period == lit(1)){
      initAmt + interestRate*initAmt
    } else {
      calculateCompoundIncome(initAmt + interestRate*initAmt,period-1,interestRate)
    }
  }

  val calculateIncomeBeforeTaxUDF = udf(calculateIncomeBeforeTax)
  val calculateTaxUDF = udf(calculateTax)
  val calculateNetIncomeUDF = udf(calculateNetIncome)
  val calculateEquityUDF = udf(calculateEquity)
}
