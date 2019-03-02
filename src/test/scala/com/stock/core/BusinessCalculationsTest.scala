package com.stock.core

import org.scalatest.{FlatSpec, FunSuite}
import org.apache.spark.sql.functions._


class BusinessCalculationsTest extends FunSuite {
  test("check calculateCompoundIncome"){
    val ev = 225.0
    val rc = BusinessCalculations.calculateCompoundIncome(lit(100.0),2,lit(0.5))
    val rv = rc.expr.eval()
    assert(rv == ev)
  }

}
