package spark.batching

import com.typesafe.scalalogging._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import spark.data.HotelWeather

object BatchReader {

  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  def main(args: Array[String]): Unit = {


    val ss = SparkSession.builder()
      .appName("spark-batching-app")
      .master("yarn")
      .getOrCreate()
    ss.sparkContext.setLogLevel("info")

    logger.info("Start reading hotels from Kafka topic")

    val hotelsWeatherFromKafka: DataFrame = ss.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "join-result-topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    if (hotelsWeatherFromKafka.limit(1).collect().isEmpty)
      logger.debug("Kafka topic is empty")
    else
      logger.debug("HotelsWeather data from topic successfully read")

    import ss.sqlContext.implicits._

    val hotelsWeather = hotelsWeatherFromKafka.map(row => HotelWeather.of(row.getAs[Array[Byte]]("value")))
    val expedia: DataFrame = ss
      .read.format("com.databricks.spark.avro")
      .load("/tmp/dataset/expedia")
    if (expedia.count() > 0)
      logger.debug("Successfully read expedia data from HDFS")

    val expediaWithIdleDays = expedia.withColumn("idleDays", datediff(expedia("srch_co"), expedia("srch_ci")))

    val hotelsWeatherExpedia = hotelsWeather.join(expedia, hotelsWeather("hotelId") === expedia("hotel_id"), "inner")
    logger.debug("Invalid dataset:" + hotelsWeatherExpedia.head(5))
    val expediaFiltered = expedia.filter(datediff(expedia("srch_co"), expedia("srch_ci")) >= 2 || datediff(expedia("srch_co"), expedia("srch_ci")) <= 30)
    expediaFiltered.write
      .partitionBy("srch_ci")
      .format("com.databricks.spark.avro")
      .mode("overwrite")
      .save("hdfs://sandbox-hdp.hortonworks.com:8020/tmp/expedia_batch_result/")

    val countryCount = expediaFiltered.groupBy("user_location_country").count()
    val cityCount = expediaFiltered.groupBy("user_location_city").count()
    logger.debug("BOOKINGS BY HOTEL COUNTRY:" + countryCount.count())
    logger.debug("BOOKINGS BY HOTEL CITY:" + cityCount.count())

    ss.close()
  }
}