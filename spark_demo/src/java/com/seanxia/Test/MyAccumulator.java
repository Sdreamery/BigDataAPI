package com.seanxia.Test;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.deploy.master.Master;
import org.apache.spark.deploy.worker.Worker;

public class MyAccumulator {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("accumulator");

        JavaSparkContext sc = new JavaSparkContext(conf);


        final Accumulator<Integer> accumulator = sc.accumulator(0);

        JavaRDD<String> lineRDD = sc.textFile("data/word.txt", 2);

        lineRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {

                accumulator.add(1);
                System.out.println(accumulator  + "  - ---- -- - - ---");
            }
        });

        System.out.println(accumulator.value());

    }
}
