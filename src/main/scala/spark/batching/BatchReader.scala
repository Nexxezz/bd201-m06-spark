package spark.batching

import org.apache.spark.sql.SparkSession
object BatchReader {
  def main(args:Array[String]) = {
    val ss = SparkSession.builder()
      .appName("spark-batching-app")
      .master("local[*]")
      .getOrCreate()

    val hotels_weather = ss.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("kafka.value.deserializer", "spark.serdes.HotelWeatherDeserializer")
      .option("kafka.key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      .option("subscribe", "join-result-topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "1")
      .load()

    val expedia = ss.read.format("com.databricks.spark.avro").load("/tmp/dataset/expedia")

    expedia.printSchema()
    hotels_weather.printSchema()
  }
}