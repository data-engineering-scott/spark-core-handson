package com.innovativeintelli.analysis;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Scanner;

/*
*
*The class will consume the data from input file and filter the boring words
* and get top 5 mostly repeated words in input.txt file
 */
public class RankNonBoringWords {

    public static void main(String args[]) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("Spark-Handson").setMaster("local[*]")
                .set("spark.ui.port", "8080");

        JavaSparkContext context = new JavaSparkContext(conf);

        // clean the input file, remove numerics and empty strings

        // Improvement 1: Since boring words are constants and small data set; we can try broadcast the dataset

        JavaRDD<String> cleanedInputRDD =  context.textFile("src/main/resources/subtitles/input.txt")
                .map(sent -> sent.replaceAll("[^a-zA-Z\\s]", "")).filter(line -> !line.trim().equals(""))
                .flatMap(eachLine -> Arrays.asList(eachLine.split(" ")).iterator())
                .filter(Util::isNotBoring);

        // Improvement 2: See how many partitions the data is split into; see if you can repartition by cleanedInputRDD.repartition(4);

        // Rank each non-boring work
        JavaPairRDD<String, Long> rankedNonBoringWords =  cleanedInputRDD.mapToPair(notBoringWord -> new Tuple2<>(notBoringWord, 1L))
                .reduceByKey((v1, v2) -> v1+v2);

        // Improvement 3: If the data fits in memory, consider aggregateByKey to minimize the data shuffling where as reduceByKey and sortByKey need data shuffling

        // Reverse the Key value pair as we need to find the highest ranked non boring word and sort in descending order
        JavaPairRDD sortedValues = rankedNonBoringWords.mapToPair(eachRank -> new Tuple2(eachRank._2, eachRank._1))
                .sortByKey(false);

        // Take top 5 non-boring words
        sortedValues.take(5).forEach(System.out::println);

        // The below Thread.sleep is logical way to stop the spark program completion. So that we can access spark UI

        Scanner scanner = new Scanner(System.in);
        if (scanner.hasNext()) {
            System.out.println("Hello");
        }

        context.close();

    }

}
