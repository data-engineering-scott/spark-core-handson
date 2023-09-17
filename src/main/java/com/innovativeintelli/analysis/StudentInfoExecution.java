package com.innovativeintelli.analysis;

import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.Scanner;

public class StudentInfoExecution {

    public static void main(String args[]) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Student-Profile")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.
                read().option("header", true).csv("src/main/resources/exams/students.csv");

        Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year > 2007");

        modernArtResults.createOrReplaceTempView("students_info_views");

        Dataset<Row> resutls = sparkSession.sql("select max(score), min(score), avg(score) from students_info_views");
        modernArtResults.show();
        resutls.show();

        Scanner scanner = new Scanner(System.in);
        scanner.hasNext();
        sparkSession.close();
    }
}
