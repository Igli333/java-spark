package com.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TextToDataframeApp {
    public static void main(String[] args) {
        TextToDataframeApp app = new TextToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Text to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("text")
                .load("data/romeo-juliet-pg1777.txt");

        df.show(10);
        df.printSchema();
    }
}
