package com.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OrcToDataframeApp {
    public static void main(String[] args) {
        OrcToDataframeApp app = new OrcToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("ORC to Dataframe")
                .config("spark.sql.orc.impl", "native")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("orc")
                .load("data/demo-11-zlib.orc");

        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows.");
    }
}
