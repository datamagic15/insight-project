import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, ForeachWriter, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.sql.DriverManager
import java.sql.Connection
import java.util.Properties

object SparkStreamingJob {
  def main(args: Array[String]): Unit = {

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://ec2-44-226-132-64.us-west-2.compute.amazonaws.com:3306/edgar"
    val table = "edgar_test"
    val username = sys.env("MYSQL_USER")
    val password = sys.env("MYSQL_PWD")

    val db_props = new Properties()
    db_props.setProperty("user", username)
    db_props.setProperty("password", password)
    db_props.setProperty("driver", driver)

    Class.forName(driver)

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
        split($"value", ",")(0).alias("ip"),
        split($"value", ",")(1).alias("date"),
        split($"value", ",")(2).alias("time"),
        unix_timestamp(
          concat(split($"value", ",")(1),
            lit(" "),
            split($"value", ",")(2) ), "yyyy-MM-dd HH:mm:ss").cast("timestamp").alias("event_time"),
        split($"value", ",")(4).alias("cik"),
        split($"value", ",")(4).alias("accession"),
        split($"value", ",")(4).alias("extention"),
        split($"value", ",")(4).alias("code"),
        split($"value", ",")(4).alias("size"),
        split($"value", ",")(4).alias("idx"),
        split($"value", ",")(4).alias("norefer"),
        split($"value", ",")(4).alias("noagent"),
        split($"value", ",")(4).alias("find"),
        split($"value", ",")(4).alias("crawler"),
        $"timestamp"
      )

    logsDF.printSchema


    val countsDF = logsDF.select($"event_time", $"cik" )
      .withWatermark("event_time", "900 seconds")
      .groupBy(
        window($"event_time", "300 seconds", "300 seconds"),
        $"cik"
      ).
      count().select("window.start","window.end","cik","count")

    val staticSumsDF = spark.read
      .parquet("hdfs://ec2-52-43-47-6.us-west-2.compute.amazonaws.com:9000/user/edgar/results/20170101")

/*    val joinStaticAndStream = spark
      .sql("select lc.window.start, lc.window.end, lc.cik, lc.count, sc.window.start, sc.window.end, sc.cik, sc.count " +
      "from log_counts lc join static_counts sc on " +
      "lc.cik = sc.cik and " +
      "date_format( lc.window.start, 'HH:mm:ss' ) =  date_format( sc.window.start, 'HH:mm:ss' ) " +
      "and date_format( lc.window.end, 'HH:mm:ss' ) =  date_format( sc.window.end, 'HH:mm:ss' ) ")
*/

    val joinStaticAndStream = countsDF.
      join(staticSumsDF,
      (countsDF.col("cik") === staticSumsDF.col("cik") ) &&
      (date_format( countsDF.col("start"), "HH:mm:ss")  === date_format( staticSumsDF.col("start"), "HH:mm:ss") ) &&
      (date_format( countsDF.col("end"), "HH:mm:ss")  === date_format( staticSumsDF.col("end"), "HH:mm:ss") )
    ).select(
      countsDF.col("start").alias("stream_start"),
      countsDF.col("end").alias("stream_end"),
      countsDF.col("cik").alias("stream_cik"),
      countsDF.col("count").alias("stream_count"),
      staticSumsDF.col("start"),
      staticSumsDF.col("end"),
      staticSumsDF.col("cik"),
      staticSumsDF.col("count")
    ).filter( col("stream_count") > col("count") )

    val query = joinStaticAndStream.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .start()

    query.awaitTermination()
      /*
      .withColumn("window_start", $"window.start")
      .withColumn("window_end", $"window.end")
      .drop($"window")
      .writeStream.
      foreachBatch{ (batchDF: DataFrame, batchId: Long) =>
        batchDF.write.mode("append").jdbc(url, table, db_props )
          //parquet("hdfs://ec2-52-43-47-6.us-west-2.compute.amazonaws.com:9000/user/results")
        //jdbc(url, table, db_props )
      }
      .outputMode("complete")
      .start()
*/
    //format("console").
      //trigger(Trigger.ProcessingTime("30 seconds")).
      //start()





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

    query.awaitTermination()
    */
  }
}
