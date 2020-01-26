package spark.batching

import org.apache.spark.sql.{DataFrame, SparkSession}

object BatchReader {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .appName("spark-batching-app")
      .master("yarn")
      .getOrCreate()

    val hotels_weather = ss.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "join-result-topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "1")
      .load()

    val expedia: DataFrame = ss
      .read.format("com.databricks.spark.avro")
      .load("/tmp/dataset/expedia")


    expedia.printSchema()
    hotels_weather.printSchema()
    ss.close()
  }
}