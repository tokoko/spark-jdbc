package com.tokoko.jdbc

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}

class JDBCDataReader(driver: String, url: String, query: String, schema: String) extends DataReader[Row] {
  var iterator: ResultSet = _
  var connection: Connection = _
  val schemaType: StructType = DataType.fromJson(schema).asInstanceOf[StructType]

  override def next(): Boolean = {
    if(iterator == null) {
      try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url)
        iterator = connection.createStatement.executeQuery(query)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    iterator.next
  }

  override def get(): Row = {
    val data = schemaType.map(
      f =>
        f.dataType match {
          case _: StringType => iterator.getString(f.name)
          case _: TimestampType => iterator.getTimestamp(f.name)
          case _: IntegerType => iterator.getInt(f.name)
          case _: ShortType => iterator.getShort(f.name)
          case _: LongType => iterator.getLong(f.name)
          case _: FloatType => iterator.getFloat(f.name)
          case _: DoubleType => iterator.getDouble(f.name)
          case _: BooleanType => iterator.getBoolean(f.name)
          case _: DateType => iterator.getDate(f.name)
          case _: ByteType => iterator.getByte(f.name)
          case _: DecimalType => iterator.getBigDecimal(f.name)
          case _ => iterator.getString(f.name)
        }
    )

    Row(data:_*)
  }

  override def close(): Unit = connection.close()
}
