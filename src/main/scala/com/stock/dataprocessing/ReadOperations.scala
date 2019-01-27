package com.stock.dataprocessing

import com.stock.basicunits.Business
import org.apache.spark.sql.functions.{regexp_replace, split}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, StructType}

class ReadOperations(spark: SparkSession) {
  /**
    *
    * @param path path to file which contain data for read dataset in csv format
    * @tparam T case class for read dataset
    * @return read dataset
    *         Can't read of write any collection from csv, need to keep such column as string and than cast it to collection.
    *         Example:
    *         readDS[CSVBusiness]("/home/tzablock/IdeaProjects/coursera/StockPredictions/src/main/resources/example.csv")
    */
  def readDF[T: Encoder](path: String): DataFrame ={
    val schema = implicitly[Encoder[T]].schema
    spark.read.schema(schema)
              .option("header","true")  //todo take it from properties
              .csv(path)
  }
}
object ReadOperations{

  def apply(sf: SparkFactory): ReadOperations = new ReadOperations(sf.sparkSession())
}
