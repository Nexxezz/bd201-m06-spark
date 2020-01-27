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

    val df: DataFrame = ss.read.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "join-result-topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    import ss.sqlContext.implicits._

    val dataset = df.map(row => HotelWeather.of(row.getAs[Array[Byte]]("value")))

    val expedia: DataFrame = ss
      .read.format("com.databricks.spark.avro")
      .load("/tmp/dataset/expedia")

    val joinExpediaHotelsWeather = dataset.join(expedia, dataset("hotelId") === expedia("hotel_id"), "inner")

    val idleDays = expedia.filter(datediff(expedia("srch_co"), expedia("srch_ci")) >= 2 || datediff(expedia("srch_co"), expedia("srch_ci")) <= 30)
    expedia.printSchema()
    print(expedia.count())
    print(idleDays.count())
    df.printSchema()
    joinExpediaHotelsWeather.printSchema()
    idleDays.show(20)
    ss.close()
  }
}