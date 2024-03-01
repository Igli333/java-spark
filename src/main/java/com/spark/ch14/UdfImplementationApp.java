package com.spark.ch14;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class UdfImplementationApp {
    public static void main(String[] args) {
        UdfImplementationApp app = new UdfImplementationApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Custom UDF")
                .master("local[*]")
                .getOrCreate();

        spark.udf().register(
                "isOpen",
                new IsOpenUdf(),
                DataTypes.BooleanType
        );

        Dataset<Row> librariesDF = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .option("encoding", "cp1252")
                .load("data/sdlibraries.csv")
                .drop("Administrative_Authority")
                .drop("Address1")
                .drop("Address2")
                .drop("Town")
                .drop("Postcode")
                .drop("County")
                .drop("Phone")
                .drop("Email")
                .drop("Website")
                .drop("Image")
                .drop("WGS84_Latitude")
                .drop("WGS84_Longitude");

        librariesDF.show(false);
        librariesDF.printSchema();

        Dataset<Row> dateTimeDf = createDataframe(spark);
        dateTimeDf.show(false);
        dateTimeDf.printSchema();

        Dataset<Row> df = librariesDF.crossJoin(dateTimeDf);
        df.show(false);

        Dataset<Row> finalDf = df.withColumn(
                        "open",
                        callUDF("isOpen",
                                col("Opening_Hours_Monday"),
                                col("Opening_Hours_Tuesday"),
                                col("Opening_Hours_Wednesday"),
                                col("Opening_Hours_Thursday"),
                                col("Opening_Hours_Friday"),
                                col("Opening_Hours_Saturday"),
                                lit("Closed"),
                                col("date"))
                )
                .drop("Opening_Hours_Monday")
                .drop("Opening_Hours_Tuesday")
                .drop("Opening_Hours_Wednesday")
                .drop("Opening_Hours_Thursday")
                .drop("Opening_Hours_Friday")
                .drop("Opening_Hours_Saturday");

//        Dataset<Row> finalDfSql = spark.sql(
//                "SELECT Council_ID, Name, date, " +
//                        "isOpen( " +
//                        "Opening_Hours_Monday, Opening_Hours_Tuesday, " +
//                        "Opening_Hours_Wednesday, Opening_Hours_Thursday, " +
//                        "Opening_Hours_Friday, Opening_Hours_Saturday, " +
//                        "'closed', date) AS open FROM libraries "
//        );

        finalDf.show();
//        finalDfSql.show();
    }

    private static Dataset<Row> createDataframe(SparkSession spark) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("date_str", DataTypes.StringType, false)
        });

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("2019-03-11 14:30:00"));
        rows.add(RowFactory.create("2019-04-27 16:00:00"));
        rows.add(RowFactory.create("2020-01-26 05:00:00"));

        return spark.createDataFrame(rows, schema)
                .withColumn("date", to_timestamp(col("date_str")))
                .drop("date_str");
    }
}
