package spark.data

case class HotelWeather(hotelId: Long,
                        hotelName: String,
                        averageTemperatureFahrenheit: Double,
                        averageTemperatureCelsius: Double,
                        weatherDate: String) extends Serializable {


  override def toString: String =
    hotelId.toString + HotelWeather.COMMA +
      hotelName + HotelWeather.COMMA +
      averageTemperatureFahrenheit.toString + HotelWeather.COMMA +
      averageTemperatureCelsius.toString + HotelWeather.COMMA +
      weatherDate;

  def isEmpty: Boolean = hotelId == 0L
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