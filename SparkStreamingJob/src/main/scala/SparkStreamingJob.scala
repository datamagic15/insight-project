import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object SparkStreamingJob {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load.getConfig(args(0))
    val spark = SparkSession.
      builder().
      master(conf.getString("execution.mode")).
      appName("Get Streaming Department Traffic").
      getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    val lines = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", conf.getString("bootstrap.servers")).
      option("subscribe", "edgar-logs").
      option("includeTimestamp", true).
      load()

    val kafkaLines = lines.selectExpr("CAST(value AS STRING)", "timestamp").
      as[(String, Timestamp)]
    val logsDF = kafkaLines.where(split($"value", ",")(0) =!= "ip").
      select(
        split($"value", ",")(0).alias("IP"),
        split($"value", ",")(1).alias("DATE"),
        split($"value", ",")(2).alias("TIME"),
        concat(split($"value", ",")(1),
          lit(" "),
          split($"value", ",")(2)
        ).alias("TIMESTAMP"),
        split($"value", ",")(4).alias("CIK"),
        split($"value", ",")(4).alias("ACCESSION"),
        split($"value", ",")(4).alias("DOC"),
        split($"value", ",")(4).alias("CODE"),
        split($"value", ",")(4).alias("SIZE"),
        split($"value", ",")(4).alias("IDX"),
        split($"value", ",")(4).alias("NOREFER"),
        split($"value", ",")(4).alias("NOAGENT"),
        split($"value", ",")(4).alias("FIND"),
        split($"value", ",")(4).alias("CRAWLER")
      )

    val countsDF = logsDF.select($"TIMESTAMP", $"CIK").
      groupBy(
        window($"TIMESTAMP", "300 seconds", "30 seconds"), $"CIK"
      ).count()

    /*
    val logsDF = kafkaLines.where(split($"value", ",")(0) =!= "ip").
      select(split($"value", ",")(4).alias("CIK"), $"timestamp").
      groupBy(
        window($"timestamp", "20 seconds", "20 seconds"), $"CIK"
      ).
      count()
*/
      //.orderBy(desc("count"))



    val query = countsDF.
      writeStream.
      outputMode("update").
      format("console").
      trigger(Trigger.ProcessingTime("30 seconds")).
      start()


    /*
    val kafkaLines = lines.selectExpr("CAST(value AS STRING)", "timestamp").
      as[(String, Timestamp)]

    val departmentTraffic = kafkaLines.
      where(split(split($"value", " ")(6), "/")(1) === "department").
      select(split(split($"value", " ")(6), "/")(2).alias("department_name"), $"timestamp").
      groupBy(
        window($"timestamp", "20 seconds", "20 seconds"),$"department_name"
      ).
      count()

    val query = departmentTraffic.
      writeStream.
      outputMode("update").
      format("console").
      trigger(Trigger.ProcessingTime("20 seconds")).
      start()
    */
    query.awaitTermination()
  }
}
