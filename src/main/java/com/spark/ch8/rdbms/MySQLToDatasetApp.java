package com.spark.ch8.rdbms;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MySQLToDatasetApp {
    public static void main(String[] args) {
        MySQLToDatasetApp app = new MySQLToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe through JDBC")
                .master("local")
                .getOrCreate();

        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "password");
        props.put("useSSL", "false");

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila?allowPublicKeyRetrieval=true&useSSL=false",
                "actor",
                props
        );

//        Using option
//        Dataset<Row> df = spark.read()
//                .option("url", "jdbc:mysql://localhost:3306/sakila")
//                .option("dbtable", "actor")
//                .option("user", "root")
//                .option("password", "Spark<3Java")
//                .option("useSSL", "false")
//                .option("serverTimezone", "EST")
//                .format("jdbc")
//                .load();

        df = df.orderBy(df.col("last_name"));

        df.show(5);
        df.printSchema();
        System.out.println("Dataframe contains: " + df.count());
    }
}
