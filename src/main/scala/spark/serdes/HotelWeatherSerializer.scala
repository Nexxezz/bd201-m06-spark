package spark.serdes

import org.apache.kafka.common.serialization.Serializer
import spark.data.HotelWeather

class HotelWeatherSerializer extends Serializer[HotelWeather] {

  /** *
   * Method for HotelWeather object serialization
   *
   * @param topic Kafka topic name
   * @param hw    HotelWeather object for serialization
   * @return byte array that represents HotelWeather object
   */
  override def serialize(topic: String, hw: HotelWeather): Array[Byte] = {
    if (hw.isEmpty) Array.empty else hw.toString.getBytes
  }
}
