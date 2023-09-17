package com.innovativeintelli;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class LogData implements Serializable {
    private String logLevel;
    private String date;

    public LogData(String logLevel, String date) {
        this.logLevel = logLevel;
        this.date = date;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public String getDate() {
        return date;
    }

    @Override
    public String toString() {
        return "Log Data: Log Level "+ logLevel + " Log Date " + date;
    }
}

public class Main {

    Logger logger = Logger.getLogger(Main.class);
    public static void main(String args[]){


        String randowString = "Example of a DAG in Spark. In this example, the DAG diagram consists of five stages: Text RDD, Filter RDD, Map RDD, Reduce RDD, and Output RDD. The arrows indicate the dependencies between the stages, and each stage is made up of multiple tasks that can be executed in parallel.";

        List<LogData> inputData = Arrays.asList(new LogData("WARN", "2023-01-01")
        , new LogData("INFO", "2023-01-01"), new LogData("WARN", "2023-02-01"),
                new LogData("FATAL", "2023-03-01"), new LogData("WARN", "2023-04-01"), new LogData("INFO", "2023-05-01")
        , new LogData("WARN", "2023-06-01"), new LogData("ERROR", "2023-07-01"));

        SparkConf conf = new SparkConf()
                .setAppName("Spark-Handson").setMaster("local[*]");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<LogData> inputRDD = context.parallelize(inputData);

        inputRDD.mapToPair(logData -> new Tuple2<>(logData.getLogLevel(), 1L)).reduceByKey((val1, val2) -> val1+ val2)
                .foreach(tup -> System.out.println(tup._1+ " has" + tup._2));

        JavaRDD<String> randomStringInput = context.parallelize(Arrays.asList(randowString, "Hellow word"));

        JavaRDD<String> valuesSplited = randomStringInput.flatMap(randomStringInpValue -> {
                    String[] arr = randomStringInpValue.split(" ");
                    return Arrays.asList(arr).stream().filter(ele -> ele.length() > 4).collect(Collectors.toList()).iterator();
                });

        valuesSplited.collect().forEach(System.out::println);

        JavaRDD<String> subtitlesInput =  context.textFile("src/main/resources/subtitles/input.txt");
        subtitlesInput.flatMap(va -> Arrays.asList(va.split(" ")).iterator()).collect()
                .forEach(System.out::println);

        context.close();



    }
}
