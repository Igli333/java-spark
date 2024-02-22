package com.spark.ch3.pojodataset;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.spark.sql.functions.*;

public class CsvToDatasetBookToDataframeApp implements Serializable {
    private static final long serialVersionUID = -1L;

    public static void main(String[] args) {
        CsvToDatasetBookToDataframeApp app = new CsvToDatasetBookToDataframeApp();
        app.start();
    }

    public void start() {
        SparkSession spark = SparkSession.builder().appName("CSV to dataframe to Dataset<Book> and back").master("local").getOrCreate();

        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

        String filename = "data/books.csv";
        Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true").option("header", "true").load(filename);

        df.show();
        df.printSchema();

        Dataset<Book> bookDs = df.map(new BookMapper(), Encoders.bean(Book.class));

        bookDs.show(5, 17);
        bookDs.printSchema();

        Dataset<Row> df2 = bookDs.toDF();

        df2 = df2.withColumn(
                "releaseDateAsString",
                concat(expr("releaseDate.year + 1900"), lit("-"),
                        expr("releaseDate.month + 1"), lit("-"),
                        df2.col("releaseDate.date")
                )
        );

        df2 = df2.withColumn(
                        "releaseDateAsDate",
                        to_date(df2.col("releaseDateAsString"), "yyyy-MM-dd")
                )
                .drop("releaseDateAsString");

        df2.show(5);
        df2.printSchema();
    }

    static class BookMapper implements MapFunction<Row, Book> {

        private static final long serialVersionUID = -2L;

        @Override
        public Book call(Row row) throws Exception {
            Book b = new Book();

            b.setId(Integer.getInteger(row.getAs("id")));
            b.setAuthorId(Integer.getInteger(row.getAs("authorId")));
            b.setTitle(String.valueOf(row.getAs("title")));

            String dateAsString = row.getAs("releaseDate");
            if (dateAsString != null) {
                SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");
                Date date = parser.parse(dateAsString);
                if (date != null) {
                    b.setReleaseDate(date);
                }
            }

            b.setLink(row.getAs("link"));

            return b;
        }
    }
}
