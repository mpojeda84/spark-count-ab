package com.mpojeda84.mapr.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

public class Application {

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        String logFile = "/user/mapr/Answers/Lab2.3a.txt"; // Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        String output = "Lines with a: " + numAs + ", lines with b: " + numBs;

        System.out.println(output);

        spark.stop();

    }
}
