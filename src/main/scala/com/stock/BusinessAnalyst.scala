package com.stock

import com.stock.basicunits.{Business, CSVBusiness}
import com.stock.core.BusinessCalculator
import com.stock.dataprocessing.{CastOperations, ReadOperations, SparkFactory}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql._

object BusinessAnalyst { //TODO make generic main class by trait and taking lass from argument
  def main(args: Array[String]): Unit = {
    val logger = Logger("FinancialAdvisorLogger")
    val exitCode = new BusinessAnalyst(logger).run()
    sys.exit(exitCode)
  }
}

class BusinessAnalyst(logger: Logger) {

  def run(): Int = {//TODO add unit tests,spark tests and performance tests
    try {
      implicit val csvBusinessEncoder: Encoder[CSVBusiness] = Encoders.product[CSVBusiness]
      implicit val businessEncoder: Encoder[Business] = Encoders.product[Business]

      val spark = SparkFactory()
      val readOperations = ReadOperations(spark)
      val businessCalculator = BusinessCalculator(spark)

      logger.info("Start reading dataset for input businesses.")
      val csvBusinesses = readOperations.readDF[CSVBusiness]("/home/tzablock/IdeaProjects/coursera/StockPredictions/src/main/resources/example.csv") //TODO in future replace with companies taken from internet
      logger.info("Stop reading dataset for input businesses.")
      logger.info("Cast csv string column into array column.")
      val businesses = CastOperations(csvBusinesses).castColumnToArray("revCosts","double")
                                                    .castColumnToArray("assets","double")
                                                    .castColumnToArray("liabilities","double")
                                                    .dataFrame()
      logger.info("Start calculating company Income Statement.")
      val isBusinesses = businessCalculator.calculateCompanyIncomeStatement(businesses)
      logger.info("Start calculating company Balance Sheet.")
      val bsBusinesses = businessCalculator.calculateCompanyBalanceSheet(isBusinesses)
      logger.info("Start calculating company Risk.")
      val rBusinesses = businessCalculator.calculateBusinessRisk(bsBusinesses)
      logger.info("Start calculating company Flow Statement.")
      //TODO cash flow statement
      logger.info("Start calculating company compound value if invest 1000 chf after 15 years for estimated value increase.")
      val cvBusinesses = businessCalculator.calculateCompoundIncomeForYears(rBusinesses,15)
      //TODO marketValue, %  return value increase
      //TODO dividenta

      //TODO calculate company real value and other important metrics to buy it

      cvBusinesses.show()//TODO presenting data in some nice framework
      //TODO maintain in memory spark context
      //TODO life job running(calculations running)

      0
    } catch {
      case e: Exception => {
        logger.error(e.getMessage,e)
        1
      }
    }
  }
}
