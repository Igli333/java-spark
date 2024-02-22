package com.spark.ch3;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class IngestionSchemaManipulationApp {

    private SparkSession spark;

    public IngestionSchemaManipulationApp() {
        this.spark = SparkSession.builder()
                .appName("Restaurants in NC")
                .master("local")
                .getOrCreate();
    }

    public static void main(String[] args) {
        IngestionSchemaManipulationApp app = new IngestionSchemaManipulationApp();
        app.start();
    }

    private void start() {
        Dataset<Row> wakeRestaurantsDf = startWakeCounty(spark);
        Dataset<Row> durhamRestaurantsDf = startDurhamCounty(spark);

        combineDataframes(wakeRestaurantsDf, durhamRestaurantsDf);
    }

    private void combineDataframes(Dataset<Row> df1, Dataset<Row> df2) {
        Dataset<Row> df = df1.unionByName(df2);
        df.show(5);

        printSchemaAndInfo(df);
    }

    private Dataset<Row> startWakeCounty(SparkSession spark) {
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/Restaurants_in_Wake_County_NC.csv");

        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumn("dateEnd", lit(null))
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");

        df = df.withColumn("id",
                concat(df.col("state"),
                        lit("_"),
                        df.col("county"),
                        lit("_"),
                        df.col("datasetId")));

        return df;
    }

    private Dataset<Row> startDurhamCounty(SparkSession spark) {
        Dataset<Row> df = spark.read().format("json")
                .load("data/Restaurants_in_Durham_County_NC.json");

        df = df.withColumn("county", lit("Durham"))
                .withColumn("datasetId", df.col("fields.id"))
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", df.col("fields.opening_date"))
                .withColumn("dateEnd", df.col("fields.closing_date"))
                .withColumn("type", split(df.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df.col("fields.geolocation").getItem(1))
                .drop(df.col("fields"))
                .drop(df.col("geometry"))
                .drop(df.col("record_timestamp"))
                .drop(df.col("recordid"));

        df = df.withColumn("id",
                concat(df.col("state"), lit("_"),
                        df.col("county"), lit("-"),
                        df.col("datasetId")));

        return df;
    }

    private void printSchemaAndInfo(Dataset<Row> df) {
        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " +
                partitionCount);

        // As a tree
        StructType schema = df.schema();

        System.out.println("Schema as a tree");
        schema.printTreeString();

        // As a string
        System.out.println("Schema as a string: \n" + schema.mkString());

        // As json

        System.out.println("Schema as a string: \n" + schema.prettyJson());
    }
}
