package com.stock

import com.stock.basicunits.{Business, CSVBusiness}
import com.stock.core.BusinessCalculator
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.stock.dataprocessing.{CastOperations, ReadOperations, SparkFactory}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.types.{ArrayType, DoubleType}

object BusinessAnalyst { //TODO make generic main class by trait and taking lass from argument
  def main(args: Array[String]): Unit = {
    val logger = Logger("FinancialAdvisorLogger")
    val exitCode = new BusinessAnalyst(logger).run()
    sys.exit(exitCode)
  }
}

class BusinessAnalyst(logger: Logger) {

  def run(): Int = {//TODO add spark tests
    try {
      implicit val csvBusinessEncoder: Encoder[CSVBusiness] = Encoders.product[CSVBusiness]
      implicit val businessEncoder: Encoder[Business] = Encoders.product[Business]

      val spark = SparkFactory()
      val castOperations = CastOperations()
      val readOperations = ReadOperations(spark)
      val businessCalculator = BusinessCalculator(spark)

      logger.info("Start reading dataset for input businesses.")
      val csvBusinesses = readOperations.readDF[CSVBusiness]("/home/tzablock/IdeaProjects/coursera/StockPredictions/src/main/resources/example.csv")
      logger.info("Stop reading dataset for input businesses.")
      val businesses = businessCalculator.preOperations(csvBusinesses)
      val calcBusinesses = businessCalculator.calculateBusinessInternals(businesses)
      calcBusinesses.show()//TODO presenting data
      0
    } catch {
      case e: Exception => {
        logger.error(e.getMessage,e)
        1
      }
    }
  }
}
