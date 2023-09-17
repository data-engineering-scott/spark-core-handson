package com.innovativeintelli.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class BigLogFileProcessing {

    public static void main(String args[]) throws InterruptedException {

        System.out.println(" Available cores " + Runtime.getRuntime().availableProcessors());

        Thread.sleep(3000);

        SparkSession sparkSession = SparkSession.builder()
                .appName("big-log-file-processing")
                .config("spark.master", "local")
                .config("spark.executor.cores", 4)
                 //.config("spark.sql.shuffle.partitions", 55)
                .getOrCreate();



        Dataset<Row> initialDataset = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");

        /*
        SQL version start here - Taking 9s with 2 cores
         */
       /* initialDataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = sparkSession.sql("select level, date_format(datetime, 'MMM') as month, count(1) as total, " +
        " first(date_format(datetime, 'M')) as monthnum from logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as int), level");

        results.show(100);
        results.explain();
*/

          /*
        SQL version with performance improvement start here - The problem in the previous sql version is
       date_format(datetime, 'MMM') as month will generate a key of type String, Spark SQl optimization use
        Hash aggregation algorithm if the monthnum is mutable types such as Integer, decimal float etc...see UnSafeRow of spark
        I converted the date_format(datetime, 'MMM') as month to integer by using cast, which will make monthnum eligible for hashAggregartion and make to run faster
         */
        initialDataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = sparkSession.sql("select level, date_format(datetime, 'MMM') as month, count(1) as total, " +
        " first(cast (date_format(datetime, 'M') as int)) as monthnum from logging_table group by level, month order by monthnum, level");

        results.drop("monthnum");
        results.show(100);
        results.explain();

        /*
        Java Version start here - Taking 3s with 2 cores
         */
        /*Dataset<Row> formattedInitialDateSet = initialDataset.select(col("level"),
                date_format(col("datetime"), "MMM").alias("month"),
                date_format(col("datetime"), "M").alias("mon-num").cast(DataTypes.IntegerType));


        formattedInitialDateSet = formattedInitialDateSet.groupBy("level", "month", "mon-num").count().as("total").orderBy("mon-num");

        formattedInitialDateSet = formattedInitialDateSet.drop("mon-num");

        formattedInitialDateSet.show(100);*/
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        sparkSession.close();





    }
}
