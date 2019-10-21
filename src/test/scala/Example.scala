import org.apache.spark.sql.SparkSession

object Example extends App {
  val spark: SparkSession = SparkSession.builder()
    .appName("TestSession")
    //.enableHiveSupport()
    .master("local[*]")
    .getOrCreate()


  val query = "SELECT * FROM ***"

  val url = "jdbc:sqlserver://***;user=***;password=***;"

  val df = spark.read
    //.option("schema", sp_schema.json)
    .format("com.tokoko.jdbc")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", url)
    .option("query", query)
    .load()

  df.show()

}
