package spark.batching

import com.typesafe.scalalogging._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.data.HotelWeather

object BatchReader {

  private val LOG = Logger(getClass)

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .appName("spark-batching-app")
      .master("yarn")
      .getOrCreate()
    ss.sparkContext.setLogLevel("error")

    LOG.info("Start reading hotels from Kafka topic")

    //Read hotels&weather data from Kafka with Spark application in a batch manner by specifying start offsets and batch limit in configuration file
    val hotelsWeatherFromKafka: DataFrame = ss.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "hotels-weather-topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    if (hotelsWeatherFromKafka.limit(1).collect().isEmpty)
      LOG.info("Kafka topic is empty")
    else
      LOG.info("HotelsWeather data from topic successfully read")

    import ss.sqlContext.implicits._
    val hotelsWeather = hotelsWeatherFromKafka.map(row => HotelWeather.of(row.getAs[Array[Byte]]("value")))

    // Read Expedia data from HDFS with Spark
    val expedia: DataFrame = ss
      .read.format("com.databricks.spark.avro")
      .load("/tmp/201bd/dataset/expedia")

    if (expedia.count() > 0)
      LOG.info("Successfully read expedia data from HDFS")

    // Calculate idle days (days between current and previous check in dates) for every hotel
    val expediaWithIdleDays = expedia.withColumn("idleDays", datediff(expedia("srch_co"), expedia("srch_ci")))



    // Remove all booking data for hotels with at least one "invalid" row (with idle days more than or equal to 2 and less than 30)
    val expediaFiltered = expedia.filter(datediff(expedia("srch_co"), expedia("srch_ci")) >= 2 || datediff(expedia("srch_co"), expedia("srch_ci")) <= 30)

    // Print hotels info (name, address, country etc) of "invalid" hotels and make a screenshot. Join expedia and hotel data for this purpose
    val hotelsWeatherExpedia = hotelsWeather.join(expedia, hotelsWeather("hotel_id") === expedia("hotel_id"), "inner")
    hotelsWeatherExpedia.show(5)

    // Store "valid" Expedia data in HDFS partitioned by year of "srch_ci"
    expediaFiltered.write
      .partitionBy("srch_ci")
      .format("com.databricks.spark.avro")
      .mode("overwrite")
      .save("hdfs://sandbox-hdp.hortonworks.com:8020/tmp/201bd/dataset/expedia_valid_data/")

   // Group the remaining data and print bookings counts: 1) by hotel country, 2) by hotel city. Make screenshots of the outputs
    val countryCount = expediaFiltered.groupBy("user_location_country").count()
    val cityCount = expediaFiltered.groupBy("user_location_city").count()

    LOG.info("BOOKINGS BY HOTEL COUNTRY:" + countryCount.count())
    LOG.info("BOOKINGS BY HOTEL CITY:" + cityCount.count())

    ss.close()
  }
}