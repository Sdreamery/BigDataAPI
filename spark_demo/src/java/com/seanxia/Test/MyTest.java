package com.seanxia.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class MyTest {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("broadcast");

        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = context.parallelize(Arrays.asList(
                "1",
                "2",
                "3"
        ));


        rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return true;
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("=========");
            }
        });


    }
}
