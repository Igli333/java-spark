package com.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetToDataframeApp {
    public static void main(String[] args) {
        ParquetToDataframeApp app = new ParquetToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Parquet to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("parquet")
                .load("data/alltypes_plain.parquet");

        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows.");
    }
}
