package com.tokoko.jdbc

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.StructType

class JDBCDataReaderFactory(driver: String, url: String, query: String, schema: StructType, predicate: Option[String]) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new JDBCDataReader(driver, url, query, schema, predicate)
}
