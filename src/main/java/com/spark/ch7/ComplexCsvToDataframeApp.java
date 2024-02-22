package com.spark.ch7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ComplexCsvToDataframeApp {
    public static void main(String[] args) {
        ComplexCsvToDataframeApp app = new ComplexCsvToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV to Dataframe")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "id",
                        DataTypes.IntegerType,
                        false
                ),
                DataTypes.createStructField(
                        "authorId",
                        DataTypes.IntegerType,
                        true
                ),
                DataTypes.createStructField(
                        "bookTitle",
                        DataTypes.StringType,
                        false
                ),
                DataTypes.createStructField(
                        "releaseDate",
                        DataTypes.DateType,
                        true
                ),
                DataTypes.createStructField(
                        "url",
                        DataTypes.StringType,
                        false
                )
        });

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "*")
                .option("dateFormat", "MM/dd/yyyy")
                .option("inferSchema", true)
                .schema(schema)
                .load("data/books-updated.csv");

        System.out.println("Excerpt of the dataframe content: ");
        df.show(7, 90);
        System.out.println("Dataframe's schema: ");
        df.printSchema();
    }
}
