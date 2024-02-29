package com.spark.ch11;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DeleteApp {
    public static void main(String[] args) {
        DeleteApp app = new DeleteApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("DELETE SQL")
                .master("local")
                .getOrCreate();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("geo", DataTypes.StringType, true),
                DataTypes.createStructField("yr1980", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr1981", DataTypes.DoubleType, false),
                DataTypes.createStructField("yr2010", DataTypes.DoubleType, false)
        });

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .schema(schema)
                .load("data/populationbycountry19802010millions.csv");

        df.createOrReplaceTempView("geodata");

        Dataset<Row> cleanedDf = spark.sql("SELECT * FROM geodata WHERE geo IS NOT NULL " +
                "AND geo != 'Africa' " +
                "AND geo != 'North America' " +
                "AND geo != 'World' " +
                "AND geo != 'Asia & Oceania' " +
                "AND geo != 'Central & South America' " +
                "AND geo != 'Europe' " +
                "AND geo != 'Eurasia' " +
                "AND geo != 'Middle East' " +
                "ORDER BY yr2010 DESC");

        cleanedDf.show(20, false);
    }
}
