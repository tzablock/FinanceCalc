package com.stock.basicunits

import com.stock.basicunits.Types.{CurrencyType, Percent}
/**
  *
  * @param rev Total Revenue
  * @param revCosts Cost of revenue total
  * @param incBefTax Income before taxes
  * @param effectiveTaxRate tax rate four our revenue
  * @param tax Taxes
  * @param netInc Net Income (for whole business)/Earnings (for stock)
  * @param bType Type of business (wc - whole company/s - stock)
  */
case class Business(rev: CurrencyType,revCosts: List[CurrencyType], incBefTax: Option[CurrencyType], effectiveTaxRate: Percent, tax: Option[CurrencyType], netInc: Option[CurrencyType], bType: String)

/**
  * Only for reading from csv (in schema for csv read we can't have collection type)
  */
case class CSVBusiness(bName: String, rev: CurrencyType,revCosts: String, effectiveTaxRate: Percent,assets:String,liabilities: String,businessMarketPrice: CurrencyType,estimatedValueIncrease: Percent, businessType: String)