package com.spark.ch8.rdbms;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MySQLWithJoinToDatasetApp {
    public static void main(String[] args) {
        MySQLWithJoinToDatasetApp app = new MySQLWithJoinToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL with Join Operation")
                .master("local")
                .getOrCreate();

        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "password");
        props.put("useSSL", "false");
        props.put("allowPublicKeyRetrieval", "true");

        String sqlQuery =
                "select actor.first_name, actor.last_name, film.title, "
                        + "film.description "
                        + "from actor, film_actor, film "
                        + "where actor.actor_id = film_actor.actor_id "
                        + "and film_actor.film_id = film.film_id";

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila",
                "(" + sqlQuery + ") actor_film_alias",
                props);

        df.show(5);
        df.printSchema();
        System.out.printf("Count: " + df.count() + "\n");
    }
}
