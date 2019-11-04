package com.tokoko.jdbc

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import Utils.serialize

class DataFrameReaderImplicits(reader: DataFrameReader) {

  /*def option[T](key: String, value: T): DataFrameReader = {
    reader.option(key, serialize(value))
  }*/

  def jdbcv2(url: String, driver: String, query: String, predicates: List[String]): DataFrame = {
    reader
      .option("predicates", serialize(predicates))
      .jdbcv2(url, driver, query)
  }

  def jdbcv2(url: String, driver: String, query: String, schema: StructType): DataFrame = {
    reader
      .schema(schema)
      .jdbcv2(url, driver, query)
  }

  def jdbcv2(url: String, driver: String, query: String): DataFrame = {
    reader
      .format("com.tokoko.jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("query", query)
      .load()
  }
}
