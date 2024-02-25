package com.spark.ch8.rdbms;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MySQLWithWhereClauseToDatasetApp {
    public static void main(String[] args) {
        MySQLWithWhereClauseToDatasetApp app = new MySQLWithWhereClauseToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL with where clause")
                .master("local")
                .getOrCreate();

        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "password");
        props.put("useSSL", "false");
        props.put("allowPublicKeyRetrieval", "true");

        String sql = "select * from film where "
                + "(title like \"%ALIEN%\" or title like \"%victory%\" "
                + "or title like \"%agent%\" or description like \"%action%\") "
                + "and rental_rate>1 "
                + "and (rating=\"G\" or rating=\"PG\")";

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila",
                "(" + sql + ")" + "actor",
                props
        );

        df.show(5);
        df.printSchema();

        System.out.println("The dataframe contains " + df
                .count() + " record(s).");
    }
}
