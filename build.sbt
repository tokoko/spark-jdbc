name := "spark-jdbc"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"
val jacksonVersion = "2.8.7"

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.4.0.jre8" % "provided"
)