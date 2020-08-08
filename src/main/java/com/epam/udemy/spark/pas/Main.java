package com.epam.udemy.spark.pas;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final String SPARK_WHOUSE_DIR = "home/ilya/spark/warehouse";

    //private static final String HDFS_HOTELS_DIR = "./hotels/";
    //private static final String HDFS_WEATHER_DIR = "src/main/resources/weather/";

    private static final String HOTELS_FIELD_ID = "Id";
    private static final String HOTELS_FIELD_NAME = "Name";
    private static final String HOTELS_FIELD_COUNTRY = "Country";
    private static final String HOTELS_FIELD_CITY = "City";
    private static final String HOTELS_FIELD_ADDRESS = "Address";
    private static final String HOTELS_FIELD_LATITUDE = "Latitude";
    private static final String HOTELS_FIELD_LONGITUDE = "Longitude";

    private static final String WEATHER_FIELD_LONGITUDE = "lng";
    private static final String WEATHER_FIELD_LATITUDE = "lat";
    private static final String WEATHER_FIELD_AVG_TMPR_F = "avg_tmpr_f";
    private static final String WEATHER_FIELD_AVG_TMPR_C = "avg_tmpr_c";
    private static final String WEATHER_FIELD_DATE = "wthr_date";
    private static final String WEATHER_FIELD_PART_YEAR = "year";
    private static final String WEATHER_FIELD_PART_MONTH = "month";
    private static final String WEATHER_FIELD_PART_DAY = "day";

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession ss = SparkSession.builder()
                .appName("hotelsSQL").master("local[*]")
                .config("spark.sql.warehouse.dir", SPARK_WHOUSE_DIR)
                .getOrCreate();
        Dataset<Row> hotels = ss.read()
                .option("sep", ",")
                .option("header", true)
                .csv(args[0]);

        Dataset<Row> hotelsFiltered = hotels.filter((Row row) -> {
            String lat = row.getAs(HOTELS_FIELD_LATITUDE);
            String lng = row.getAs(HOTELS_FIELD_LONGITUDE);
            boolean hasNA = "NA".equals(lat) || "NA".equals(lng);
            boolean latEmpty = lat == null || lat.isEmpty();
            boolean lngEmpty = lng == null || lng.isEmpty();
            return !(hasNA || latEmpty || lngEmpty);
        });

        //System.out.println(hotelsFiltered.count());

        DecimalFormat df = new DecimalFormat("0.00");
        df.setRoundingMode(RoundingMode.HALF_UP);

        List<StructField> hotelFields = new ArrayList<>();
        hotelFields.add(DataTypes.createStructField(HOTELS_FIELD_ID, DataTypes.LongType, false));
        hotelFields.add(DataTypes.createStructField(HOTELS_FIELD_NAME, DataTypes.StringType, true));
        hotelFields.add(DataTypes.createStructField(HOTELS_FIELD_CITY, DataTypes.StringType, true));
        hotelFields.add(DataTypes.createStructField(HOTELS_FIELD_COUNTRY, DataTypes.StringType, true));
        hotelFields.add(DataTypes.createStructField(HOTELS_FIELD_ADDRESS, DataTypes.StringType, true));
        hotelFields.add(DataTypes.createStructField(HOTELS_FIELD_LATITUDE, DataTypes.DoubleType, false));
        hotelFields.add(DataTypes.createStructField(HOTELS_FIELD_LONGITUDE, DataTypes.DoubleType, false));
        StructType hotelsSchema = DataTypes.createStructType(hotelFields);

        Dataset<Row> hotelsDS = ss.createDataFrame(hotelsFiltered.toJavaRDD().map(row -> {
            Double lat = Double.parseDouble(row.getAs(HOTELS_FIELD_LATITUDE));
            Double lng = Double.parseDouble(row.getAs(HOTELS_FIELD_LONGITUDE));
            return RowFactory.create(Long.valueOf(row.getAs(HOTELS_FIELD_ID)), row.getAs(HOTELS_FIELD_NAME),
                    row.getAs(HOTELS_FIELD_CITY), row.getAs(HOTELS_FIELD_COUNTRY), row.getAs(HOTELS_FIELD_ADDRESS),
                    Double.valueOf(df.format(lat)), Double.valueOf(df.format(lng)));
        }), hotelsSchema);
        /*
        Dataset<Row> weather = ss.read().parquet(HDFS_WEATHER_DIR);
        Dataset<Row> weatherDS = ss.createDataFrame(weather.toJavaRDD().map(row -> {
            Double lat = row.getAs(WEATHER_FIELD_LATITUDE);
            Double lng = row.getAs(WEATHER_FIELD_LONGITUDE);
            return RowFactory.create(Double.valueOf(df.format(lng)), Double.valueOf(df.format(lat)),
                    row.getAs(WEATHER_FIELD_AVG_TMPR_F), row.getAs(WEATHER_FIELD_AVG_TMPR_C), row.getAs(WEATHER_FIELD_DATE),
                    row.getAs(WEATHER_FIELD_PART_YEAR), row.getAs(WEATHER_FIELD_PART_MONTH), row.getAs(WEATHER_FIELD_PART_DAY));
        }), weather.schema());

        Dataset<Row> joinedDS = weatherDS.join(hotelsDS,
                weatherDS.col(WEATHER_FIELD_LATITUDE)
                        .equalTo(hotelsDS.col(HOTELS_FIELD_LATITUDE))
                .and(weatherDS.col(WEATHER_FIELD_LONGITUDE)
                        .equalTo(hotelsDS.col(HOTELS_FIELD_LONGITUDE))),
                "inner")
                .select(hotelsDS.col(HOTELS_FIELD_ID),
                        hotelsDS.col(HOTELS_FIELD_NAME),
                        hotelsDS.col(HOTELS_FIELD_CITY),
                        hotelsDS.col(HOTELS_FIELD_COUNTRY),
                        hotelsDS.col(HOTELS_FIELD_ADDRESS),
                        weatherDS.col(WEATHER_FIELD_AVG_TMPR_F),
                        weatherDS.col(WEATHER_FIELD_AVG_TMPR_C),
                        weatherDS.col(WEATHER_FIELD_DATE));

        joinedDS.show();
        //joinedDS.printSchema();
        //System.out.println(joinedDS.count());
        */
        hotelsDS.write()
                .format("console")
                .save();

        ss.close();
    }
}
