import org.apache.spark.sql.functions._

when(lit(1) === 1,2).otherwise(3)