package com.innovativeintelli.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class LogExecutionTest {

    public static void main(String args[]) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("LogInfo")
                .master("local[*]")
                .getOrCreate();

       List<Row> inMemory = new ArrayList<>();

       inMemory.add(RowFactory.create("WARN", "2018-12-01"));
       inMemory.add(RowFactory.create("FATAL", "2018-11-16"));
       inMemory.add(RowFactory.create("WARN", "2018-10-13"));
       inMemory.add(RowFactory.create("INFO", "2018-10-12"));
       inMemory.add(RowFactory.create("FATAL", "2018-09-08"));

       StructType schema = new StructType(new StructField[]{
               new StructField("level", DataTypes.StringType, false, Metadata.empty()),
               new StructField("Date", DataTypes.StringType, false, Metadata.empty())
       });
       Dataset<Row> dataSet = sparkSession.createDataFrame(inMemory, schema);

        dataSet.createOrReplaceTempView("logging_table");

        sparkSession.sql("select level, count(Date) from logging_table group by level ").explain();

        Scanner scanner = new Scanner(System.in);
        scanner.hasNext();
        sparkSession.close();
    }
}
