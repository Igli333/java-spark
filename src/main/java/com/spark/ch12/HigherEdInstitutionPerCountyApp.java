package com.spark.ch12;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class HigherEdInstitutionPerCountyApp {
    public static void main(String[] args) {
        HigherEdInstitutionPerCountyApp app = new HigherEdInstitutionPerCountyApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Join")
                .master("local")
                .getOrCreate();

        Dataset<Row> censusDf = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "cp1252")
                .load("data/PEP_2017_PEPANNRES.csv");

        censusDf = censusDf
                .drop("GEO.id")
                .drop("rescen42010")
                .drop("resbase42010")
                .drop("respop72010")
                .drop("respop72010")
                .drop("respop72011")
                .drop("respop72012")
                .drop("respop72013")
                .drop("respop72014")
                .drop("respop72015")
                .drop("respop72016")
                .withColumnRenamed("respop72017", "pop2017")
                .withColumnRenamed("GEO.id2", "countyId")
                .withColumnRenamed("GEO.display-label", "county");

        censusDf.sample(0.1).show(3, false);

        Dataset<Row> higherEdDf = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/InstitutionCampus.csv");

        higherEdDf = higherEdDf
                .filter("LocationType = 'Institution'")
                .withColumn("addressElements", split(higherEdDf.col("Address"), " "));
        higherEdDf = higherEdDf
                .withColumn("addressElementCount", size(higherEdDf.col("addressElements")));

        higherEdDf = higherEdDf
                .withColumn("zip9", element_at(
                        higherEdDf.col("addressElements"),
                        higherEdDf.col("addressElementCount")
                ));

        higherEdDf = higherEdDf.withColumn("splitZipCode", split(higherEdDf.col("zip9"), "-"));

        higherEdDf = higherEdDf
                .withColumn("zip", higherEdDf.col("splitZipCode").getItem(0))
                .withColumnRenamed("LocationName", "location")
                .drop("DapipId")
                .drop("zip9")
                .drop("addressElements")
                .drop("addressElementCount")
                .drop("splitZipCode");

        higherEdDf.show(5);

        Dataset<Row> countryZipDf = spark.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/COUNTY_ZIP_092018.csv");

        countryZipDf = countryZipDf
                .drop("red_ratio")
                .drop("bus_ratio")
                .drop("oth_ratio")
                .drop("tot_ratio");

        countryZipDf.show(5);

        Dataset<Row> institPerCountyDf = higherEdDf.join(countryZipDf,
                higherEdDf.col("zip").equalTo(countryZipDf.col("zip")),"inner");

        institPerCountyDf.show(5);
    }
}
