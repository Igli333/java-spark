package com.spark.ch11;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SqlAndApiApp {
    public static void main(String[] args) {
        SqlAndApiApp app = new SqlAndApiApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Simple SQL")
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
//        for (int i = 1981; i < 2010; i++) {
            df = df.drop(df.col("yr1981"));
//        }

        df = df.withColumn("evolution", functions.expr("round((yr2010 - yr1980) * 1000000)"));
        df.createOrReplaceTempView("geodata");

        Dataset<Row> negativeEvolutionDf = spark.sql("SELECT * FROM geodata " +
                "WHERE geo IS NOT NULL AND evolution <= 0 " +
                "ORDER BY evolution " +
                "LIMIT 25");

        negativeEvolutionDf.show(15, false);

        Dataset<Row> moreThanAMillionDf = spark.sql("SELECT * FROM geodata WHERE geo IS NOT NULL AND evolution > 999999 " +
                "ORDER BY evolution DESC LIMIT 25");

        moreThanAMillionDf.show(15, false);

    }
}
