package com.spark.ch15;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class NewYorkSchoolStatisticsApp {

    private SparkSession spark = null;

    public static void main(String[] args) {
        NewYorkSchoolStatisticsApp app = new NewYorkSchoolStatisticsApp();
        app.start();
    }

    private void start() {
        spark = SparkSession.builder()
                .appName("NYC schools analytics")
                .master("local[*]")
                .getOrCreate();

        String enrolled = "enrolled";
        String schoolId = "schoolId";
        String present = "present";
        String absent = "absent";
        String schoolYear = "schoolYear";


        Dataset<Row> masterDf = loadDataUsing2018Format("data/nyc/2018*.csv");

        masterDf = masterDf.unionByName(loadDataUsing2015Format("data/nyc/2015*.csv"));

        masterDf = masterDf.unionByName(loadDataUsing2006Format("data/nyc/200*.csv", "data/nyc/2012*.csv"));

        Dataset<Row> averageEnrollmentDf = masterDf
                .groupBy(col(schoolId), col(schoolYear))
                .avg(enrolled, present, absent)
                .orderBy(schoolId, schoolYear);

        averageEnrollmentDf.show(20);


        Dataset<Row> studentCountPerYearDf = averageEnrollmentDf
                .withColumnRenamed("avg(" + enrolled + ")", enrolled)
                .groupBy(col(schoolYear))
                .agg(sum(enrolled).as(enrolled))
                .withColumn(enrolled, floor(enrolled).cast(DataTypes.LongType))
                .orderBy(schoolYear);

        studentCountPerYearDf.show(20);

        Row maxStudentRow = studentCountPerYearDf.orderBy(col(enrolled).desc()).first();

        String year = maxStudentRow.getString(0);
        long max = maxStudentRow.getLong(1);
        System.out.println("The year with the most students was " + year + " and the max was " + max);

        Dataset<Row> relativeStudentCountPerYearDf = studentCountPerYearDf
                .withColumn("max", lit(max))
                .withColumn("delta", expr("max - " + enrolled))
                .drop("max")
                .orderBy(schoolYear);

        relativeStudentCountPerYearDf.show(20);

        Dataset<Row> maxEnrollmentPerSchoolDf = masterDf
                .groupBy(col(schoolId), col(schoolYear))
                .max(enrolled)
                .orderBy(schoolId, schoolYear);

        maxEnrollmentPerSchoolDf.show();

        Dataset<Row> minAbsenteeDf = masterDf
                .groupBy(col(schoolId), col(schoolYear))
                .min(absent)
                .orderBy(schoolId, schoolYear);

        minAbsenteeDf.show(20);

        Dataset<Row> absenteeRatioDf = masterDf
                .groupBy(col(schoolId), col(schoolYear))
                .agg(max(enrolled).alias(enrolled), avg(absent).as(absent));

        absenteeRatioDf = absenteeRatioDf.groupBy(col(schoolId))
                .agg(avg(enrolled).as("avg_" + enrolled), avg(absent).as("avg_" + absent))
                .withColumn("%", expr("avg_" + absent + " / avg_" + enrolled + " * 100"))
                .filter(col("avg_" + enrolled).$greater(10))
                .orderBy("%");

        absenteeRatioDf.show(5);

        absenteeRatioDf.orderBy(col("%").desc()).show(5);

    }

    private Dataset<Row> loadDataUsing2018Format(String... fileNames) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "schoolId",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "date",
                        DataTypes.DateType,
                        false),
                DataTypes.createStructField(
                        "enrolled",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "present",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "absent",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "released",
                        DataTypes.IntegerType,
                        false)});
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("dateFormat", "yyyyMMdd")
                .schema(schema)
                .load(fileNames);
        return df.withColumn("schoolYear", lit(2018));
    }

    private Dataset<Row> loadDataUsing2006Format(String... fileNames) {
        return loadData(fileNames, "yyyyMMdd");
    }

    private Dataset<Row> loadDataUsing2015Format(String... fileNames) {
        return loadData(fileNames, "MM/dd/yyyy");
    }

    private Dataset<Row> loadData(String[] fileNames, String dateFormat) {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("schoolId", DataTypes.StringType, false),
                DataTypes.createStructField("date", DataTypes.DateType, false),
                DataTypes.createStructField("schoolYear", DataTypes.StringType, false),
                DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
                DataTypes.createStructField("present", DataTypes.IntegerType, false),
                DataTypes.createStructField("absent", DataTypes.IntegerType, false),
                DataTypes.createStructField("released", DataTypes.IntegerType, false)
        });

        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("dateFormat", dateFormat)
                .schema(schema)
                .load(fileNames);

        return df.withColumn("schoolYear", substring(col("schoolYear"), 1, 4));
    }

}
