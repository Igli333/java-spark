package com.spark.ch12;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class AqeHigherEdInstitutionApp {
    public static void main(String[] args) {
        AqeHigherEdInstitutionApp app = new AqeHigherEdInstitutionApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Join Using AQE")
                .master("local")
                .config("spark.sql.adaptive.enabled", true)
                .getOrCreate();

        Dataset<Row> censusDf = spark
                .read()
                .format("csv")
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
                .drop("OpeId")
                .drop("ParentName")
                .drop("ParentDapipId")
                .drop("LocationType")
                .drop("Address")
                .drop("GeneralPhone")
                .drop("AdminName")
                .drop("AdminPhone")
                .drop("AdminEmail")
                .drop("Fax")
                .drop("UpdateDate")
                .drop("zip9")
                .drop("addressElements")
                .drop("addressElementCount")
                .drop("splitZipCode");

        Dataset<Row> countyZipDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/COUNTY_ZIP_092018.csv");
        countyZipDf = countyZipDf
                .drop("res_ratio")
                .drop("bus_ratio")
                .drop("oth_ratio")
                .drop("tot_ratio");

        Dataset<Row> institutePerCountyDf = higherEdDf.join(
                countyZipDf,
                higherEdDf.col("zip").equalTo(countyZipDf.col("zip")),
                "inner"
        );

        institutePerCountyDf = institutePerCountyDf.join(
                censusDf,
                institutePerCountyDf.col("county").equalTo(censusDf.col("countyId")),
                "left"
        );

        institutePerCountyDf = institutePerCountyDf
                .drop(higherEdDf.col("zip"))
                .drop(countyZipDf.col("county"))
                .drop("countyId")
                .distinct();

        institutePerCountyDf = institutePerCountyDf.cache();
        institutePerCountyDf.collect();

        institutePerCountyDf.show(10);

        spark.stop();
    }
}
