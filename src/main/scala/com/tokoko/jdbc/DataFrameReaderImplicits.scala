package com.tokoko.jdbc

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import Utils.serialize

class DataFrameReaderImplicits(reader: DataFrameReader) {

  def option[T](key: String, value: T): DataFrameReader = {
    reader.option(key, serialize(value))
  }

  def jdbcv2(url: String, query: String, predicates: List[String]): DataFrame = {
    reader
      .option("predicates", serialize(predicates))
      //.option("driver", JDBCUtils.getDriver(url))
      .option("url", url)
      .option("query", query)
      .load()
  }

  def jdbcv2(url: String, query: String, schema: StructType): DataFrame = {
    reader
      .schema(schema)
      //.option("driver", JDBCUtils.getDriver(url))
      .option("url", url)
      .option("query", query)
      .load()
  }

  def jdbcv2(url: String, query: String): DataFrame = {
    reader
      .format("com.tbcbank.bigdata.framework.datasources.simplejdbc")
      //.option("driver", JDBCUtils.getDriver(url))
      .option("url", url)
      .option("query", query)
      .load()
  }
}
