package com.tokoko.jdbc

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, ReadSupportWithSchema}
import org.apache.spark.sql.types.StructType

class DefaultSource extends DataSourceV2 with ReadSupport with ReadSupportWithSchema {
  def createReader(options: DataSourceOptions) = new JDBCSourceReader(options, None)

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = new JDBCSourceReader(options, Some(schema))
}