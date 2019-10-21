package com.tokoko.jdbc

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}

class JDBCDataReaderFactory(driver: String, url: String, query: String, schema: String) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new JDBCDataReader(driver, url, query, schema)
}
