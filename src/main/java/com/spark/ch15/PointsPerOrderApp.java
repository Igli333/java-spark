package com.spark.ch15;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class PointsPerOrderApp {
    public static void main(String[] args) {
        PointsPerOrderApp app = new PointsPerOrderApp();
        app.start();

    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Orders loyalty point")
                .master("local[*]")
                .getOrCreate();

        spark.udf().register("pointAttribution", new PointAttributionUdaf());

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/orders.csv");

        Dataset<Row> pointDf = df
                .groupBy(col("firstName"), col("lastName"), col("state"))
                .agg(
                        sum("quantity"),
                        callUDF("pointAttribution", col("quantity")).as("point")
                );

        pointDf.show(20);
    }
}
