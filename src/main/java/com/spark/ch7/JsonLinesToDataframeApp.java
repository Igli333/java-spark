package com.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonLinesToDataframeApp {
    public static void main(String[] args) {
        JsonLinesToDataframeApp app = new JsonLinesToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("JSON Lines to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("json").load("data/durham-nc-foreclosure-2006-2016.json");

        df.show(5, 13);
        df.printSchema();
    }
}
