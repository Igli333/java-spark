package com.spark.ch16;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class BrazilStatisticsApp {
    enum Mode {NO_CACHE_NO_CHECKPOINT, CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER}

    private SparkSession spark;

    public static void main(String[] args) {
        BrazilStatisticsApp app = new BrazilStatisticsApp();
        app.start();
    }

    private void start() {
        spark = SparkSession.builder()
                .appName("Brazil Economy")
                .master("local[*]")
                .getOrCreate();
        SparkContext context = spark.sparkContext();
        context.setCheckpointDir("/tmp");

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("sep", ";")
                .option("enforceSchema", true)
                .option("nullValue", "null")
                .option("inferSchema", true)
                .load("data/BRAZIL_CITIES.csv");

        df.show(100);
        df.printSchema();

        long t0 = process(df, Mode.NO_CACHE_NO_CHECKPOINT);
        long t1 = process(df, Mode.CACHE);
        long t2 = process(df, Mode.CHECKPOINT);
        long t3 = process(df, Mode.CHECKPOINT_NON_EAGER);

    }

    private long process(Dataset<Row> df, Mode mode) {
        long t0 = System.currentTimeMillis();

        df = df.orderBy(col("CAPITAL").desc())
                .withColumn("WAL-MART", when(col("WAL-MART").isNull(), 0).otherwise(col("WAL-MART")))
                .withColumn("MAC", when(col("MAC").isNull(), 0).otherwise(col("MAC")))
                .withColumn("GDP", regexp_replace(col("GDP"), ",", "."))
                .withColumn("GDP", col("GDP").cast("float"))
                .withColumn("area", regexp_replace(col("area"), ",", "."))
                .withColumn("area", col("area").cast("float"))
                .groupBy("STATE")
                .agg(
                        first("CITY").alias("capital"),
                        sum("IBGE_RES_POP_BRAS").alias("pop_brazil"),
                        sum("IBGE_RES_POP_ESTR").alias("pop_foreign"),
                        sum("POP_GDP").alias("pop_2016"),
                        sum("GDP").alias("gdp_2016"),
                        sum("POST_OFFICES").alias("post_offices_ct"),
                        sum("WAL-MART").alias("wal_mart_ct"),
                        sum("MAC").alias("mc_donalds_ct"),
                        sum("Cars").alias("cars_ct"),
                        sum("Motorcycles").alias("moto_ct"),
                        sum("AREA").alias("area"),
                        sum("IBGE_PLANTED_AREA").alias("agr_area"),
                        sum("IBGE_CROP_PRODUCTION_$").alias("agr_prod"),
                        sum("HOTELS").alias("hotels_ct"),
                        sum("BEDS").alias("beds_ct")
                )
                .withColumn("agr_area", expr("agr_area / 100"))
                .orderBy(col("STATE"))
                .withColumn("gdp_capita", expr("gdp_2016 / pop_2016 * 1000"));

        switch (mode) {
            case CACHE -> df = df.cache();
            case CHECKPOINT -> df = df.checkpoint();
            case CHECKPOINT_NON_EAGER -> df = df.checkpoint(false);
        }

        df.show(5);

        Dataset<Row> popDf = df.drop("area", "pop_brazil", "pop_foreign", "post_offices_ct",
                        "cars_ct", "moto_ct", "mc_donalds_ct", "agr_area", "agr_prod",
                        "wal_mart_ct", "hotels_ct", "beds_ct", "gdp_capita", "agr_area",
                        "gdp_2016")
                .orderBy(col("pop_2016").desc());

        popDf.show(30);

        Dataset<Row> walmartPopDf = df
                .withColumn("walmart_lm_inh", expr("int(wal_mart_ct / pop_2016 * 100000000) / 100"))
                .drop("pop_brazil", "pop_foreign", "post_offices_ct", "cars_ct",
                        "moto_ct", "area", "agr_area", "agr_prod", "mc_donalds_ct",
                        "hotels_ct", "beds_ct", "gdp_capita", "agr_area", "gdp_2016")
                .orderBy(col("walmart_lm_inh").desc());

        walmartPopDf.show(5);

        Dataset<Row> postOfficeDf = df
                .withColumn("post_office_1m_inh",
                        expr("int(post_offices_ct / pop_2016 * 100000000) / 100"))
                .withColumn("post_office_100k_km2",
                        expr("int(post_offices_ct / area * 10000000) / 100"))
                .drop(
                        "gdp_capita", "pop_foreign", "gdp_2016", "gdp_capita",
                        "cars_ct", "moto_ct", "agr_area", "agr_prod", "mc_donalds_ct",
                        "hotels_ct", "beds_ct", "wal_mart_ct", "agr_area", "pop_brazil")
                .orderBy(col("post_office_1m_inh").desc());

        switch (mode) {
            case CACHE -> postOfficeDf = postOfficeDf.cache();
            case CHECKPOINT -> postOfficeDf = postOfficeDf.checkpoint();
            case CHECKPOINT_NON_EAGER -> postOfficeDf = postOfficeDf.checkpoint(false);
        }

        Dataset<Row> postOfficePopDf = postOfficeDf
                .drop("post_office_100k_km2", "area")
                .orderBy(col("post_office_1m_inh").desc());
        postOfficePopDf.show(5);

        Dataset<Row> postOfficeArea = postOfficeDf
                .drop("post_office_1m_inh", "pop_2016")
                .orderBy(col("post_office_100k_km2").desc());
        postOfficeArea.show(5);

        long t1 = System.currentTimeMillis();

        return t1 - t0;
    }
}
