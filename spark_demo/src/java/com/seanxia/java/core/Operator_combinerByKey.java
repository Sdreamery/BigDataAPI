package com.seanxia.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class Operator_combinerByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("countByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 1)

        ),2);

//        rdd1.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                System.out.println(" v1 :" + v1 + " v2 : " + v2);
//                return v1 + v2;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

        rdd1.combineByKey(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return null;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return null;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return null;
            }
        });

    }
}
