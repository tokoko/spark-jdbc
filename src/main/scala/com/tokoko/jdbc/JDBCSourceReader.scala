package com.tokoko.jdbc

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.{DataType, StructType}

class JDBCSourceReader(options: DataSourceOptions) extends DataSourceReader {

  private val driver = options.asMap.get("driver")
  private val url = options.asMap.get("url")
  private val query = options.asMap.get("query")

  private val schema = {
    if (options.asMap.containsKey("schema")) {
      DataType.fromJson(options.asMap.get("schema")).asInstanceOf[StructType]
    } else {
      val driver = options.asMap.get("driver")
      val url = options.asMap.get("url")
      val query = options.asMap.get("query")
      if (Utils.isCommonTableExp(query)) {
        val first = query.substring(0, query.toLowerCase.indexOf("with"))
        val middle = query.substring(query.toLowerCase.indexOf("with"), query.length)
        var end = middle.substring(middle.indexOf(")") + 1, middle.length).replaceAll("""^\s+(?m)""", "")
        while (!end.toLowerCase.startsWith("select"))
          end = end.substring(end.indexOf(")") + 1, end.length).replaceAll("""^\s+(?m)""", "")
        Utils.getSchema(driver, url, s"$first${middle.replace(end, s"SELECT * FROM ($end) AS METADATA WHERE 1 = 0 ")}")
      }
      else
        Utils.getSchema(driver, url, s"$query WHERE 1 = 0")

    }
  }

  override def readSchema(): StructType = schema

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
    factoryList.add(new JDBCDataReaderFactory(driver, url, query, schema.json))
    factoryList
  }

}
