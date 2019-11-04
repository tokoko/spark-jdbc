import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Example extends App {
  val spark: SparkSession = SparkSession.builder()
    .appName("TestSession")
    .master("local[*]")
    .getOrCreate()

  val url = "jdbc:sqlserver://HOSTNAME;user=user;password=password123;"
  val query =
    """
      |WITH cte AS (
      | SELECT ID
      | FROM ListOfID
      |)
      |SELECT T.ID, T.NAME
      |FROM Table T
      | INNER JOIN cte as C ON T.ID = C.ID
      |""".stripMargin
  val schema = StructType(Seq(StructField("N", IntegerType)))
  val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

  val predicates = List(
    "T.ID < 1",
    "T.ID BETWEEN 1 AND 10",
    "T.ID > 10"
  )

  import com.tokoko.jdbc._

  spark.read
    .jdbcv2(url, driver, query, predicates)
}
