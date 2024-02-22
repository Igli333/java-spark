package com.spark.ch3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class ArrayToDatasetApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Array to Dataset<String")
                .master("local")
                .getOrCreate();

        List<String> data = List.of("Jean", "Liz", "Pierre", "Lauric");

        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        ds.show();
        ds.printSchema();

        Dataset<Row> df = ds.toDF();
        df.show();
        df.printSchema();
    }
}
