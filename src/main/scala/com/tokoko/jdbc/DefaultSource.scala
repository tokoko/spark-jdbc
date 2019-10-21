package com.tokoko.jdbc

import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class DefaultSource extends DataSourceV2 with ReadSupport {
  def createReader(options: DataSourceOptions) = new JDBCSourceReader(options)
}