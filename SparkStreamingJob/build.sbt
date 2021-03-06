name := "SparkStreamingJob"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.4.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.+"
