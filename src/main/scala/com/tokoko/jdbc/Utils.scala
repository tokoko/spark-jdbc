package com.tokoko.jdbc

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.sql.DriverManager
import java.util.Base64

import org.apache.spark.sql.types._

object Utils {

  private def getSparkDataType(jdbcType: Int, precision: Int, scale: Int): DataType = {
    jdbcType match {
      case java.sql.Types.BIT => BooleanType
      case java.sql.Types.TINYINT => ByteType
      case java.sql.Types.SMALLINT => ShortType
      case java.sql.Types.INTEGER => IntegerType
      case java.sql.Types.BIGINT => LongType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.NUMERIC => DecimalType(precision, scale)
      case java.sql.Types.DECIMAL => DecimalType(precision, scale)
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.VARCHAR => StringType
      case java.sql.Types.DATE => DateType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NVARCHAR => StringType
      case _ => StringType
    }
  }

  def getSchema(driver: String, url: String, query: String): StructType = {
    val (metadata, connection) = try {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url)
      val iterator = connection.createStatement.executeQuery(query)
      (iterator.getMetaData, connection)
    } catch {
      case e: Exception => new Exception("Can't obtain schema"); (null, null)
    }

    val sparkSchema = (1 to metadata.getColumnCount)
      .map(i => {
        val name = metadata.getColumnName(i)
        val dataType = metadata.getColumnType(i)
        val precision = metadata.getPrecision(i)
        val scale = metadata.getScale(i)

        (name, getSparkDataType(dataType, precision, scale))
      })
      .foldLeft(new StructType)(
        (a, b) => a.add(b._1, b._2)
      )

    connection.close()

    sparkSchema
  }

  def isCommonTableExp(query: String): Boolean = {
    val seq = query.toLowerCase.split(" ").toSeq
    val asIndex = seq.indexWhere(p => p.endsWith("with")) + 2
    seq.size > asIndex && seq(asIndex).startsWith("as")
  }

  def getSchemaQuery(query: String): String = {
    if (isCommonTableExp(query)) {
      val first = query.substring(0, query.toLowerCase.indexOf("with"))
      val middle = query.substring(query.toLowerCase.indexOf("with"), query.length)
      var end = middle.substring(middle.indexOf(")") + 1, middle.length).replaceAll("""^\s+(?m)""", "")
      while (!end.toLowerCase.startsWith("select"))
        end = end.substring(end.indexOf(")") + 1, end.length).replaceAll("""^\s+(?m)""", "")
      s"$first${middle.replace(end, s"SELECT * FROM ($end) METADATA WHERE 1 = 0")}"
    }
    else
      s"select * from ($query) METADATA WHERE 1 = 0"
  }

  def getPredicateQuery(query: String, predicate: Option[String]): String = {
    predicate match {
      case Some(p) => s"select * from ($query) T WHERE $p"
      case None => query
    }
  }

  def serialize[T](obj: T): String = {
    val aos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(aos)
    oos.writeObject(obj)
    oos.close()
    val res = aos.toByteArray
    aos.close()

    Base64.getEncoder.encodeToString(res)
  }

  def deserialize[T](obj: String): T = {
    val bais = new ByteArrayInputStream(Base64.getDecoder.decode(obj))
    val ois = new ObjectInputStream(bais)
    val res = ois.readObject.asInstanceOf[T]
    bais.close()
    ois.close()
    res
  }

}