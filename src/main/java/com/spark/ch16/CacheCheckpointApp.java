package com.spark.ch16;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.col;

public class CacheCheckpointApp {
    enum Mode {NO_CACHE_NO_CHECKPOINT, CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER}

    private SparkSession spark;

    public static void main(String[] args) {
        CacheCheckpointApp app = new CacheCheckpointApp();
        app.start();
    }

    private void start() {
        spark = SparkSession.builder()
                .appName("Cache and Checkpoint")
                .master("local[*]")
                .config("spark.executor.memory", "70g")
                .config("spark.driver.memory", "50")
                .config("spark.memory.offHeap.enabled", true)
                .config("spark.memory.offHeap.size", "16g")
                .getOrCreate();

        SparkContext sc = spark.sparkContext();
        sc.setCheckpointDir("/tmp");

        int recordCount = 10000;
        long t0 = processDataframe(recordCount, Mode.NO_CACHE_NO_CHECKPOINT);
        long t1 = processDataframe(recordCount, Mode.CACHE);
        long t2 = processDataframe(recordCount, Mode.CHECKPOINT);
        long t3 = processDataframe(recordCount, Mode.CHECKPOINT_NON_EAGER);

        System.out.println("\nProcessing times");
        System.out.println("Without cache ............... " + t0 + " ms");
        System.out.println("With cache .................. " + t1 + " ms");
        System.out.println("With checkpoint ............. " + t2 + " ms");
        System.out.println("With non-eager checkpoint ... " + t3 + " ms");
    }

    private long processDataframe(int recordCount, Mode mode) {
        Dataset<Row> df = RecordGeneratorUtils.createDataframe(this.spark, recordCount);

        long t0 = System.currentTimeMillis();
        Dataset<Row> topDf = df.filter(col("rating").equalTo(5));

        switch (mode) {
            case CACHE -> topDf = topDf.cache();
            case CHECKPOINT -> topDf = topDf.checkpoint();
        }

        List<Row> langDf = topDf.groupBy("lang").count().orderBy("lang").collectAsList();
        List<Row> yearDf = topDf.groupBy("year").count().orderBy(col("year").desc()).collectAsList();

        long t1 = System.currentTimeMillis();

        System.out.println("Processing took " + (t1 - t0) + " ms.");

        for (Row r : langDf)
            System.out.println(r.getString(0) + "...." + r.getLong(1));


        for (Row r : yearDf)
            System.out.println(r.getString(0) + "...." + r.getLong(1));

        return t1 - t0;
    }
}
