package com.spark.ch11;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SimpleSelectApp {
    public static void main(String[] args) {
        SimpleSelectApp app = new SimpleSelectApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Simple SELECT using SQL")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("geo", DataTypes.StringType, true),
                DataTypes.createStructField("yr1980", DataTypes.DoubleType, false)
        });

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .schema(schema)
                .load("data/populationbycountry19802010millions.csv");

        df.createOrReplaceTempView("geodata");
        df.printSchema();

        Dataset<Row> smallCountries = spark.sql("SELECT * FROM geodata WHERE yr1980 < 1 ORDER BY 2 LIMIT 5");

        smallCountries.show(10, false);

        df.createOrReplaceGlobalTempView("geodata");
        df.printSchema();

        Dataset<Row> smallCountriesDf = spark.sql("SELECT * FROM geodata WHERE yr1980 < 1 ORDER BY 2 LIMIT 5");

        smallCountriesDf.show(10, false);

        SparkSession spar2 = spark.newSession();
        Dataset<Row> slightlyBiggerCountries = spar2.sql("SELECT * FROM global_temp.geodata WHERE yr1980 > 1 ORDER BY 2 LIMIT 5");
        slightlyBiggerCountries.show(10, false);
    }
}
