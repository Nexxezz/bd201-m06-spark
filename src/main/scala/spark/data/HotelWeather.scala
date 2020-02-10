package spark.data

import spark.data.HotelWeather.COMMA

case class HotelWeather(hotel_id: Long,
                        hotel_name: String,
                        avg_temp_f: Double,
                        avg_temp_c: Double,
                        weather_date: String) extends Serializable {


  override def toString: String =
    hotel_id + COMMA +
      hotel_name + COMMA +
      avg_temp_f + COMMA +
      avg_temp_c + COMMA +
      weather_date

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