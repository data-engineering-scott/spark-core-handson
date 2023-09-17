package com.innovativeintelli.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class StudentInfoExecutionMoreScenarios {

    public static void main(String args[]) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Student-Profile")
                .master("local[*]")
                .getOrCreate();

        sparkSession.udf().register("passedWith", (String grade) -> {
            if (grade.equals("A+")) {
                return "Distinction";
            } else if (grade.equals("A")) {
                return "First class";
            } else if (grade.equals("E")) {
                return "Failed";
            } else {
                return "Passed";
            }
        }, DataTypes.StringType);
        Dataset<Row> dataset = sparkSession.
                read().option("header", true).csv("src/main/resources/exams/students.csv");


        dataset=  dataset.withColumn("pass", callUDF("passedWith", col("grade")));

        dataset.show();


        sparkSession.close();
    }
}
