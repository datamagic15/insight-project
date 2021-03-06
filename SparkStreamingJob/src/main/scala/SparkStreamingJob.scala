import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.util.Properties

object SparkStreamingJob {
  def main(args: Array[String]): Unit = {


    val driver = "com.mysql.jdbc.Driver"
    val db_host = sys.env("MYSQL_HOST")
    val edgar_db = sys.env("MYSQL_DB")
    val username = sys.env("MYSQL_USER")
    val password = sys.env("MYSQL_PWD")
    val url = "jdbc:mysql://" + db_host + ":3306/" + edgar_db
    val table = "edgar_alerts"

    val db_props = new Properties()
    db_props.setProperty("user", username)
    db_props.setProperty("password", password)
    db_props.setProperty("driver", driver)

    Class.forName(driver)

    val spark_master_node = sys.env("SPARK_MASTER")
    val hdfs_path = "hdfs://" + spark_master_node + ":9000"
    val spark_path = "spark://" + spark_master_node + ":7077"
    val bootstrap_server = sys.env("BOOTSTRAP_SERVER")


    val spark = SparkSession.
      builder().
      master(spark_path).
      appName("Edgar Alert Streaming Spark Job").
      getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    //spark.conf.set("spark.sql.shuffle.partitions", "2")

    val lines = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", bootstrap_server ).
      option("subscribe", "edgar-logs").
      load()

    val kafkaLines = lines.selectExpr("CAST(value AS STRING)").
      as[(String)]


    val logsDF = kafkaLines.where(split($"value", ",")(0) =!= "ip").
      select(
        split($"value", ",")(0).alias("ip"),
        split($"value", ",")(1).alias("date"),
        split($"value", ",")(2).alias("time"),
        unix_timestamp(
          concat(split($"value", ",")(1),
            lit(" "),
            split($"value", ",")(2) ), "yyyy-MM-dd HH:mm:ss").cast("timestamp").alias("event_time"),
        split($"value", ",")(4).cast("int").alias("cik"),
        split($"value", ",")(4).alias("accession"),
        split($"value", ",")(4).alias("extention"),
        split($"value", ",")(4).cast("int").alias("code"),
        split($"value", ",")(4).cast("int").alias("size"),
        split($"value", ",")(4).cast("int").alias("idx"),
        split($"value", ",")(4).cast("int").alias("norefer"),
        split($"value", ",")(4).cast("int").alias("noagent"),
        split($"value", ",")(4).cast("int").alias("find"),
        split($"value", ",")(4).cast("int").alias("crawler")
      )

    // Many records contain garbage in the CIK field.  But in these cases we can derive the CIK
    // from a substring of the accession column
    val fixCikDF = logsDF.
      withColumn("cik_new", when(length($"cik") > 9, substring($"accession",4,7)).
      otherwise($"cik")).
      drop("cik").
      withColumnRenamed("cik_new","cik").
      withColumn("cik", col("cik").cast("Integer"))

    fixCikDF.printSchema

    val staticSumsDF = spark.read
      .parquet(hdfs_path + "/user/edgar/batch_results")


    val countsDF = fixCikDF.select($"event_time", $"cik" )
      .withWatermark("event_time", "30 seconds")
      .groupBy(
        window($"event_time", "300 seconds"),
        $"cik"
      ).
      count().select("window.start","window.end","cik","count")

    val joinStaticAndStream = countsDF.
      join(staticSumsDF,
      (countsDF.col("cik") === staticSumsDF.col("cik") ) &&
      (date_format( countsDF.col("start"), "HH:mm:ss")  === staticSumsDF.col("start_time") ) &&
      (date_format( countsDF.col("end"), "HH:mm:ss")  === staticSumsDF.col("end_time") )
    ).select(
      countsDF.col("start").alias("stream_start"),
      countsDF.col("end").alias("stream_end"),
      countsDF.col("cik").alias("stream_cik"),
      countsDF.col("count").alias("stream_count"),
      staticSumsDF.col("start_time"),
      staticSumsDF.col("end_time"),
      staticSumsDF.col("cik"),
      staticSumsDF.col("avg_cnt"),
      staticSumsDF.col("avg_cnt").multiply(4).alias("big_cnt")
    ).filter( col("stream_count") > col("big_cnt") )

    val query = joinStaticAndStream.writeStream.
      foreachBatch{ (batchDF: DataFrame, batchId: Long) =>
        batchDF.write.mode("append").jdbc(url, table, db_props )
      }
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .start()

    query.awaitTermination()
  }
}
