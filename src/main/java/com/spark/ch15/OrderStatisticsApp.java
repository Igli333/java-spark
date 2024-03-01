package com.spark.ch15;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class OrderStatisticsApp {
    public static void main(String[] args) {
        OrderStatisticsApp app = new OrderStatisticsApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("OrderAnalytics")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/orders.csv");

        Dataset<Row> apiDf = df
                .groupBy(col("firstName"), col("lastName"), col("state"))
                .agg(sum("quantity"), sum("revenue"), avg("revenue"));

        apiDf.show(20);

        df.createOrReplaceTempView("orders");
        String sqlStatement = "SELECT firstName, lastName, state, SUM(quantity), SUM(revenue), AVG(revenue) " +
                "FROM orders GROUP BY firstName, lastName, state";

        Dataset<Row> sqlDf = spark.sql(sqlStatement);
        sqlDf.show(20);
    }
}
