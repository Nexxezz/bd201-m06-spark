package spark.batching

import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.data.HotelWeather

object BatchReader {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .appName("spark-batching-app")
      .master("yarn")
      .getOrCreate()

    val df: DataFrame = ss.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("kafka.value.deserializer", "spark.serdes.HotelWeatherDeserializer")
      .option("kafka.key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      .option("subscribe", "join-result-topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "1")
      .load()

    import ss.sqlContext.implicits._
    val rdd = df.map(row => HotelWeather.of(row.getAs[Array[Byte]]("value")))

    val expedia: DataFrame = ss
      .read.format("com.databricks.spark.avro")
      .load("/tmp/dataset/expedia")

    expedia.printSchema()
    df.printSchema()

    ss.close()
  }
}