package com.spark.ch2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class CsvToRelationalDatabaseApp {
    public static void main(String[] args) {
        CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/authors.csv");

        df = df.withColumn("name", concat(df.col("lname"), lit(", "), df.col("fname")));

        String dbConnectionUrl = "jdbc:postgresql://localhost:5432/spark_db";

        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "igli");
        prop.setProperty("password", "password");
        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "authors", prop);

        System.out.println("Process complete");
    }
}
