package com.stock.dataprocessing

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

class CastOperations {
  /**
    *
    * @param ds dataset to be casted and transformed
    * @param preTransform OPTIONAL function contains dataset transformations on input dataset
    * @tparam T output dataset type
    * @tparam T1 input dataset type
    * @return transformed and casted output dataset
    *         Example:castAndTransformDS[Business,CSVBusiness](csvBusinesses, operations)
    */
  def castAndTransformDS[T:Encoder](ds: DataFrame, preTransform: DataFrame => DataFrame = df => df): Dataset[T] = {
    preTransform.apply(ds).as[T]
  }
}
object CastOperations{
  def apply(): CastOperations = new CastOperations()
}
