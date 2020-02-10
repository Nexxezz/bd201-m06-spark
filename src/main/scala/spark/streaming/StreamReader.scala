package spark.streaming

import org.apache.spark.sql.functions.{datediff, when, col}
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.data.HotelWeather

object StreamReader {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("spark-streaming-app").master("yarn").getOrCreate()
    ss.sparkContext.setLogLevel("error")

    //Read Expedia data for 2016 year from HDFS as initial state DataFrame
    val expediaInitialStateDataframe: DataFrame = ss
      .read
      .format("com.databricks.spark.avro")
      .load("/tmp/201bd/dataset/expedia_valid_data/srch_ci=2016*")


    //Read data for 2017 year as streaming data
    val expediaStream = ss
      .readStream
      .format("com.databricks.spark.avro")
      .schema(expediaInitialStateDataframe.schema)
      .load("/tmp/201bd/dataset/expedia_valid_data/")

    val hotelsWeatherFromKafka: DataFrame = ss.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "hotels-weather-topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("spark.es.port", "9200")
      .option("spark.es.nodes", "172.18.0.5")
      .load()

    import ss.sqlContext.implicits._
    val hotelsWeather = hotelsWeatherFromKafka.map(row => HotelWeather.of(row.getAs[Array[Byte]]("value")))

    //Enrich both DataFrames with weather: add average day temperature at checkin (join with hotels+weaher data from Kafka topic)
    val joinResult = expediaStream.join(hotelsWeather, "hotel_id")

    //Filter incoming data by having average temperature more than 0 Celsius degrees
    val filtered = joinResult.filter("avg_temp_c <= 0").withColumn("duration_of_stay", datediff(joinResult("srch_co"), joinResult("srch_ci")))

    val result = filtered.withColumn("stay_type",
      when(col("duration_of_stay") <= 0 || col("duration_of_stay") > 30, "erroneus_data")
        .when(col("duration_of_stay") === 1, "short_stay")
        .when(col("duration_of_stay") > 2 || col("duration_of_stay") <= 7, "standart_stay")
        .when(col("duration_of_stay") > 7 || col("duration_of_stay") <= 14, "standart_extended_stay")
        .when(col("duration_of_stay") > 14 || col("duration_of_stay") <= 28, "long_stay")
        .otherwise("null"))


    import org.elasticsearch.spark.sql._
    result.toDF().saveToEs(("weather-expedia-hotels/result"))


  }
}
