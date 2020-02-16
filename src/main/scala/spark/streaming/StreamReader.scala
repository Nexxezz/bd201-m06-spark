package spark.streaming

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.{col, datediff, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.data.HotelWeather

object StreamReader {

  private val LOG = Logger(getClass)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession
      .builder()
      .appName("spark-streaming")
      .master("yarn")
      .getOrCreate()


    val hotelsWeatherFromKafka: DataFrame = ss.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "hotels-weather-topic")
      .load()

    LOG.info("Readed hotels-weather data from Kafka")

    import ss.sqlContext.implicits._
    val hotelsWeather = hotelsWeatherFromKafka.map(row => HotelWeather.of(row.getAs[Array[Byte]]("value")))


    //Read Expedia data for 2016 year from HDFS as initial state DataFrame
    val expediaInitialStateDataframe: DataFrame = ss
      .read
      .format("com.databricks.spark.avro")
      .load("/tmp/201bd/dataset/expedia_valid_data/srch_ci=2016*")

    LOG.info("Readed expedia-2016 data from HDFS:" + "downloaded " + expediaInitialStateDataframe.count + " records")

    //Read data for 2017 year as streaming data
    val expediaStream = ss.readStream
      .format("com.databricks.spark.avro")
      .schema(expediaInitialStateDataframe.schema)
      .load("/tmp/201bd/dataset/expedia_valid_data/srch_ci=2017*")

    LOG.info("Readed expedia-2017 data from HDFS")


    //Enrich both DataFrames with weather: add average day temperature at checkin (join with hotels+weaher data from Kafka topic)
    val joinResult = expediaStream.join(hotelsWeather, "hotel_id")

    //Filter incoming data by having average temperature more than 0 Celsius degrees
    //Calculate customer's duration of stay as days between requested check-in and check-out date
    val filtered = joinResult.filter("avg_temp_c >= 0").withColumn("duration_of_stay", datediff(joinResult("srch_co"), joinResult("srch_ci")))


    //Create customer preferences of stay time based on next logic
    val result = filtered.withColumn("stay_type",
      when(col("duration_of_stay") <= 0 || col("duration_of_stay") > 30, "erroneus_data")
        .when(col("duration_of_stay") === 1, "short_stay")
        .when(col("duration_of_stay") > 2 || col("duration_of_stay") <= 7, "standart_stay")
        .when(col("duration_of_stay") > 7 || col("duration_of_stay") <= 14, "standart_extended_stay")
        .when(col("duration_of_stay") > 14 || col("duration_of_stay") <= 28, "long_stay")
        .otherwise("null"))

    //Store final data in HDFS
    result.writeStream.format("parquet")
      .option("checkpointLocation", "/tmp/check/15")
      .start("spark/weather-expedia-hotels")
  }
}
