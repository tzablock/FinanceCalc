package com.stock.basicunits

import com.stock.basicunits.Types.CurrencyType
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
case class Business(rev: CurrencyType,revCosts: List[CurrencyType], incBefTax: Option[CurrencyType], effectiveTaxRate: Double, tax: Option[CurrencyType], netInc: Option[CurrencyType], bType: String)

/**
  * Only for reading from csv (in schema for csv read we can't have collection type)
  */
case class CSVBusiness(rev: CurrencyType,revCosts: String, incBefTax: Option[CurrencyType], effectiveTaxRate: Double, tax: Option[CurrencyType], netInc: Option[CurrencyType], bType: String)