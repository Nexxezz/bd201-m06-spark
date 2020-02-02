package spark.data

import spark.data.HotelWeather.COMMA

case class HotelWeather(hotel_id: Long,
                        hotelName: String,
                        averageTemperatureFahrenheit: Double,
                        averageTemperatureCelsius: Double,
                        weatherDate: String) extends Serializable {


  override def toString: String =
    hotel_id + COMMA +
      hotelName + COMMA +
      averageTemperatureFahrenheit + COMMA +
      averageTemperatureCelsius + COMMA +
      weatherDate

  def isEmpty: Boolean = hotel_id == 0L
}

object HotelWeather {

  val COMMA: String = ","
  val empty: HotelWeather = HotelWeather(0L, "", 0.0, 0.0, "")

  def of(str: String): HotelWeather = {
    val arr = str.split(COMMA)
    HotelWeather(arr(0).toLong, arr(1), arr(2).toDouble, arr(3).toDouble, arr(4))
  }

  def of(bytes: Array[Byte]): HotelWeather = of(new String(bytes))
}