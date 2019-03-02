package com.stock.dataprocessing

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class SparkFactoryTest extends FunSuite{
  test("check createDF if produce DataFrame"){
    val sf = SparkFactory()
    val df = sf.createDF(Map("col1"->List(1,2,3),"col2"->List("a","b","c")))
    assert(df.schema === StructType(Array(StructField("col1",IntegerType),StructField("col2",StringType))))
    assert(df.count() === 3)
  }
}
