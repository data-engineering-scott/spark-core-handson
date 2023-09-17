package com.innovativeintelli.analysis;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;

/*
 *
 *The class will consume the data from input file and filter the boring words
 * and get top 5 mostly repeated words in input.txt file I used DataSets instead of RDD;
 * Not sure may be it is because of smaller dataset this program is taking more time compared to rdd;
 * This has only one Stage
 */
public class RankNonBoringWordsWithDatasets {

    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .appName("Spark-Handson")
                .master("local[*]")
                .getOrCreate();

        // Register the UDF isNotBoring from the Util class
        UserDefinedFunction isNotBoringUDF = functions.udf((String word) -> Util.isNotBoring(word), DataTypes.BooleanType);
        spark.udf().register("isNotBoring", isNotBoringUDF);

        // Load the text file into a Dataset of String
        Dataset<String> inputDS = spark.read().textFile("src/main/resources/subtitles/input.txt").as(Encoders.STRING());

        // Explode and split the text into words
        Dataset<String> wordsDS = inputDS
                .flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.toLowerCase().replaceAll("[^a-zA-Z\\s]", "").split(" ")).iterator(), Encoders.STRING())
                .filter(functions.expr("isNotBoring(value)"));

        // Rank the non-boring words
        // Improvement 1: we should avoid using groupBy; instead try ranking using WindowSpec

        Dataset<Row> rankedWordsDF = wordsDS
                .groupBy("value")
                .agg(functions.count("*").as("count"))
                .orderBy(functions.col("count").desc());

        // Show the top 5 non-boring words
        rankedWordsDF.show(5);

        // The below Thread.sleep is logical way to stop the spark program completion. So that we can access spark UI
        Thread.sleep(300000);

        spark.close();
    }
}
