package spark.data;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class HotelWeather implements Serializable {

    private static String COMMA = ",";

    private Long hotelId;

    private String hotelName;

    private Double averageTemperatureFahrenheit;

    private Double averageTemperatureCelsius;

    private String weatherDate;

    @Override
    public String toString() {
        return hotelId.toString() + COMMA +
                hotelName + COMMA +
                averageTemperatureFahrenheit.toString() + COMMA +
                averageTemperatureCelsius.toString() + COMMA +
                weatherDate;
    }


    public static HotelWeather ofString(String str) {
        String[] arr = str.split(COMMA);
        return new HotelWeather(
                Long.parseLong(arr[0]),
                arr[1],
                Double.valueOf(arr[2]),
                Double.valueOf(arr[3]),
                arr[4]);
    }

    public static boolean nonNull(HotelWeather hotelWeather) {
        return hotelWeather.hotelId != null;
    }
}
