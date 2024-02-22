package com.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MultilineJsonToDataframeApp {
    public static void main(String[] args) {
        MultilineJsonToDataframeApp app = new MultilineJsonToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Multiline JSON to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("json")
                .option("multiline", true)
                .load("data/countrytravelinfo.json");

        df.show(3);
        df.printSchema();
    }
}
