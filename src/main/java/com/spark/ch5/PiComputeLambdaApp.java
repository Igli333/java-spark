package com.spark.ch5;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class PiComputeLambdaApp {
    private static long counter = 0;

    public static void main(String[] args) {
        PiComputeLambdaApp app = new PiComputeLambdaApp();
        int slices = 1;
        if (args.length != 0) {
            slices = Integer.parseInt(args[0]);
        }
        app.start(slices);
    }

    private void start(int slices) {
        int numberOfThrows = 100000 * slices;

        System.out.println("About to throw " + numberOfThrows
                + " darts, ready? Stay away from the target!");

        long t0 = System.currentTimeMillis();

        SparkSession spark = SparkSession.builder()
                .appName("Spark Pi")
                .master("local")
                .getOrCreate();

        long t1 = System.currentTimeMillis();

        System.out.println("Session initialized in " + (t1 - t0) + " ms");

        List<Integer> listOfThrows = new ArrayList<>(numberOfThrows);
        for (int i = 0; i < numberOfThrows; i++) {
            listOfThrows.add(i);
        }

        Dataset<Row> incrementalDf = spark.createDataset(listOfThrows, Encoders.INT()).toDF();

        long t2 = System.currentTimeMillis();
        System.out.println("Initial dataframe built in " + (t2 - t1) + " ms");

        Dataset<Integer> dotsDs = incrementalDf.map(
                (MapFunction<Row, Integer>) status -> {
                    double x = Math.random() * 2 - 1;
                    double y = Math.random() * 2 - 1;

                    counter++;

                    if (counter % 100000 == 0) {
                        System.out.println(counter + " darts thrown so far");
                    }
                    return (((x * x) + (y * y)) <= 1) ? 1 : 0;
                }, Encoders.INT()
        );

        long t3 = System.currentTimeMillis();
        System.out.println("Throwing darts done in " + (t3 - t2) + " ms");

        int dartsInCircle = dotsDs.reduce((ReduceFunction<Integer>) Integer::sum);

        long t4 = System.currentTimeMillis();
        System.out.println("Analyzing result in " + (t4 - t3) + " ms");

        System.out.println("Analyzing result in " + (t4 - t3) + " ms");
        System.out.println("Pi is roughly " + 4.0 * dartsInCircle /
                numberOfThrows);
    }
}
