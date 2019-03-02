package com.stock.core

import com.stock.dataprocessing.SparkFactory
import org.scalatest.FunSuite

class BusinessCalculatorTest extends FunSuite{ //TODO why work here and not in code
  test("check calculateCompoundIncomeForYears for 5 years"){
    val bc = BusinessCalculator(SparkFactory())
    val df = SparkFactory().createDF(Map("name"->List("A","B","C"),"estimatedValueIncrease"->List(0.5,0.4,0.3)))
    val compDF = bc.calculateCompoundIncomeForYears(df,5)
    assert(compDF.count() === 3)
    assert(compDF.columns === Array("name","estimatedValueIncrease","5 yearCompVal","4 yearCompVal","3 yearCompVal","2 yearCompVal","1 yearCompVal"))
  }
}
