# spark-jdbc
Spark JDBC DataSource API v2

----

## When to use spark-jdbc?
Official Spark repository already comes with a jdbc datasource api built-in, which should be preferred for most use cases. 
However, there are limitations that turn out to be significant in couple of use cases.

Spark jdbc api only accepts database table names and SELECT queries as an input, 
in other words something that can be wrapped inside of another SELECT * FROM {} query.
This makes sense for most cases, but makes it impossible to execute stored procedures, use common table expressions in your queries, 
declare variables beforehand for simplicity and so on.

## How to use it?
To use spark-jdbc, either build from the source or copy the contents of com.tokoko.jdbc directory to your codebase.
One way to call jdbc api is using standard DataFrameReader object with specified format.
```
val url = "jdbc:sqlserver://HOSTNAME;user=user;password=password123;"
val query = "SELECT * FROM Table"
val schema = StructType(Seq(StructField("N", IntegerType)))
val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

spark.read
    .format("com.tokoko.jdbc") // package where DefaultSource is defined
    .option("driver", driver)
    .option("url", url)
    .option("query", query)
    .schema(schema) // schema is optional unless calling a stored procedure        
    .load()
```

Or using implicit conversion defined in a package object 

```
val url = "jdbc:sqlserver://HOSTNAME;user=user;password=password123;"
val query = "SELECT * FROM Table"
val schema = StructType(Seq(StructField("N", IntegerType)))
val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

import com.tokoko.jdbc._

spark.read
    .jdbcv2(url, driver, query, schema)
```

----

#### Stored procedures

Calling stored procedures is exactly like running any other query, but you have to provide a schema, as there is no easy way to determine the schema automatically. 
Additionally, it almost never makes sense to try parallelizing a stored procedure call, because it would mean significantly increased workload on the database side.

----

#### CTE

Using a CTE is just as easy, there's no need to provide a schema, as it will be automatically determined.
You can also parallelize a query if you pass an additional list of predicates similar to spark jdbc predicates.

```
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
```