package com.spark.ch8.rdbms;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MySQLWithPartitioningApp {
    public static void main(String[] args) {
        MySQLWithPartitioningApp app = new MySQLWithPartitioningApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL with Partitioning")
                .master("local")
                .getOrCreate();

        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "password");
        props.put("useSSL", "false");
        props.put("allowPublicKeyRetrieval", "true");

        props.put("partitionColumn", "film_id");
        props.put("lowerBound", "1");
        props.put("upperBound", "1000");
        props.put("numPartitions", "10");

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila",
                "film",
                props);

        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df
                .count() + " record(s).");
        System.out.println("The dataframe is split over " + df.rdd()
                .getPartitions().length + " partition(s).");
    }
}
