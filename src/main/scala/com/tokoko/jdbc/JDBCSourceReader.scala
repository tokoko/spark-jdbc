package com.tokoko.jdbc

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.StructType
import Utils.{getSchema, getSchemaQuery}

class JDBCSourceReader(options: DataSourceOptions, schemaOpt: Option[StructType]) extends DataSourceReader {

  private val driver = options.asMap.get("driver")
  private val url = options.asMap.get("url")
  private val query = options.asMap.get("query")
  private lazy val predicates: List[String] = Utils.deserialize(options.asMap.get("predicates"))

  private val schema = schemaOpt.getOrElse(getSchema(driver, url, getSchemaQuery(query)))

  override def readSchema(): StructType = schema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]

    if (options.asMap().containsKey("predicates")) {
      predicates.foreach(
        p => factoryList.add(new JDBCDataReaderFactory(driver, url, query, schema, Some(p)))
      )
    } else {
      factoryList.add(new JDBCDataReaderFactory(driver, url, query, schema, None))
    }

    factoryList
  }

}
