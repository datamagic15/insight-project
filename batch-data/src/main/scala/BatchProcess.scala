import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BatchProcess {
  def main(args: Array[String]): Unit = {

    val spark_master_node = sys.env("SPARK_MASTER")
    val spark_path = "spark://" + spark_master_node + ":7077"
    val hdfs_input_files = "hdfs://" + spark_master_node + ":9000/user/edgar/batch_data/*.csv"
    val hdfs_output = "hdfs://" + spark_master_node + ":9000/user/edgar/batch_results"


    val spark = SparkSession.
      builder.
      master(spark_path).
      appName("Historical Batch Process").
      getOrCreate()

    import spark.implicits._

    //val kafkaLines = spark.read.textFile("hdfs://ec2-52-43-47-6.us-west-2.compute.amazonaws.com:9000/user/edgar/batch_data/*.csv")
    val kafkaLines = spark.read.textFile(hdfs_input_files)


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
        split($"value", ",")(5).alias("accession"),
        split($"value", ",")(6).alias("extention"),
        split($"value", ",")(7).cast("int").alias("code"),
        split($"value", ",")(8).cast("int").alias("size"),
        split($"value", ",")(9).cast("int").alias("idx"),
        split($"value", ",")(10).cast("int").alias("norefer"),
        split($"value", ",")(11).cast("int").alias("noagent"),
        split($"value", ",")(12).cast("int").alias("find"),
        split($"value", ",")(13).cast("int").alias("crawler")
      )


    val fixCikDF = logsDF.
      withColumn("cik_new", when(length($"cik") > 9, substring($"accession",4,7)).
        otherwise($"cik")).
      drop("cik").
      withColumnRenamed("cik_new","cik").withColumn("cik", col("cik").cast("Integer"))


    val countsDF = fixCikDF.select($"event_time", $"cik" ).
      groupBy(
        window($"event_time", "300 seconds"),
        $"cik"
      ).
      agg(count("*").
        alias("cnt")).
      select("window.start","window.end","cik","cnt")

    val windowCounts = countsDF.
      select(
        date_format(countsDF.col("start"),"HH:mm:ss").alias("start_time"),
        date_format(countsDF.col("end"), "HH:mm:ss").alias("end_time"),
        countsDF.col("cik"),
        countsDF.col("cnt")
      ).
      groupBy(
        $"start_time",$"end_time",$"cik"
      ).
      agg(avg($"cnt").alias("avg_cnt"))

//    windowCounts.write.parquet("hdfs://ec2-52-43-47-6.us-west-2.compute.amazonaws.com:9000/user/edgar/batch_results")
    windowCounts.write.parquet(hdfs_output)


  }
}
