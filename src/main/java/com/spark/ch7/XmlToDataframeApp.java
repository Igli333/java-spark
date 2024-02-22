package com.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class XmlToDataframeApp {
    public static void main(String[] args) {
        XmlToDataframeApp app = new XmlToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("XML to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("xml")
                .option("rowTag", "row")
                .load("data/nasa-patents.xml");

        df.show(5);
        df.printSchema();
    }
}
