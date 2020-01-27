package spark.batching

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import spark.data.HotelWeather

object BatchReader {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .appName("spark-batching-app")
      .master("yarn")
      .getOrCreate()

    val hotelsWeatherFromKafka: DataFrame = ss.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "join-result-topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    import ss.sqlContext.implicits._

    val hotelsWeather = hotelsWeatherFromKafka.map(row => HotelWeather.of(row.getAs[Array[Byte]]("value")))

    val expedia: DataFrame = ss
      .read.format("com.databricks.spark.avro")
      .load("/tmp/dataset/expedia")

    val expediaWithIdleDays = expedia.withColumn("idleDays", datediff(expedia("srch_co"), expedia("srch_ci")))

    val hotelsWeatherExpedia = hotelsWeather.join(expedia, hotelsWeather("hotelId") === expedia("hotel_id"), "inner")

    val expediaFiltered = expedia.filter(datediff(expedia("srch_co"), expedia("srch_ci")) >= 2 || datediff(expedia("srch_co"), expedia("srch_ci")) <= 30)
    expediaFiltered.write
      .partitionBy("srch_ci")
      .format("com.databricks.spark.avro")
      .mode("overwrite")
      .save("hdfs://sandbox-hdp.hortonworks.com:8020/tmp/expedia_batch_result/")

    val countryCount = expediaFiltered.groupBy("user_location_country").count()
    val cityCount = expediaFiltered.groupBy("user_location_city").count()
    println("BOOKINGS BY HOTEL COUNTRY:" + countryCount.count())
    println("BOOKINGS BY HOTEL CITY:" + cityCount.count())

    ss.close()
  }
}