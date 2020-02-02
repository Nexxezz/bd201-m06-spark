package spark.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.data.HotelWeather

object StreamReader {

  def main(args: Array[String]) = {
    val ss = SparkSession.builder().appName("spark-streaming-app").master("yarn").getOrCreate()
    ss.sparkContext.setLogLevel("error")

    //Read Expedia data for 2016 year from HDFS as initial state DataFrame
    val expediaInitialStateDataframe: DataFrame = ss
      .read
      .format("com.databricks.spark.avro")
      .load("/tmp/expedia_batch_result/srch_ci=2016*")

    expediaInitialStateDataframe.printSchema()
    expediaInitialStateDataframe.show(5)

    //Read data for 2017 year as streaming data
    val expediaStream = ss
      .readStream
      .format("com.databricks.spark.avro")
      .schema(expediaInitialStateDataframe.schema)
      .load("/tmp/expedia_batch_result/srch_ci=2017*")

    val hotelsWeatherFromKafka: DataFrame = ss.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "join-result-topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    import ss.sqlContext.implicits._
    val hotelsWeather = hotelsWeatherFromKafka.map(row => HotelWeather.of(row.getAs[Array[Byte]]("value")))

    //Enrich both DataFrames with weather: add average day temperature at checkin (join with hotels+weaher data from Kafka topic)
    val joinResult = expediaStream.join(hotelsWeather, "srch_ci == weatherDate")

    //Filter incoming data by having average temperature more than 0 Celsius degrees
    val filtered = joinResult.filter("averageTemperatureCelsius <= 0")

    //Store final data in HDFS
    filtered.writeStream
      .format("com.databricks.spark.avro")
      .outputMode("append")
      .option("path", "/tmp/dataset/streaming_result_data")
      .start()
  }
}
