package spark.serdes

import org.apache.kafka.common.serialization.Deserializer
import spark.data.HotelWeather;

class HotelWeatherDeserializer extends Deserializer[HotelWeather] {

  /** *
   * Method for HotelWeather object deserialization
   *
   * @param topic Kafka topic name
   * @param data  serialized HotelWeather class for deserialization
   * @return HotelWeather object
   */
  override def deserialize(topic: String, data: Array[Byte]): HotelWeather = {
    if (data == null || data.isEmpty)
      HotelWeather.empty
    else
      HotelWeather.of(new String(data))
  }
}
