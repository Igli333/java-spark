package com.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroToDataframeApp {
    public static void main(String[] args) {
        AvroToDataframeApp app = new AvroToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Avro to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("avro")
                .load("data/weather.avro");

        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows.");
    }
}
