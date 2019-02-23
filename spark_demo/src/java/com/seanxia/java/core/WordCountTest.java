package com.seanxia.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {
    public static void main(String[] args){

        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("wc");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> lineRDD = context.textFile("./data/wc.txt");

//        long count = javaRDD.count();
//        List<String> collect = javaRDD.collect();
//        List<String> take = javaRDD.take(5);
//        String first = javaRDD.first();

        JavaRDD<String> wordRDD = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1361366081449028006L;

            @Override
            public Iterable<String> call(String line) throws Exception {
                String[] split = line.split(" ");
                List<String> list = Arrays.asList(split);
                return list;
            }
        });

//        wordRDD.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        JavaPairRDD<String, Integer> pairRDD = wordRDD.mapToPair(
                new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -6202938131576200568L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2(word, 1);
            }
        });

//        pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

        JavaPairRDD<String, Integer> resultRDD = pairRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -2281075788486668774L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> reverseRDD = resultRDD.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            private static final long serialVersionUID = -2383474885055140770L;

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<>(tuple2._2, tuple2._1);
            }
        });

        JavaPairRDD<Integer, String> sortByKey = reverseRDD.sortByKey(false);

        JavaPairRDD<String, Integer> result = sortByKey.mapToPair(
                new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            private static final long serialVersionUID = -2517407873769342743L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return new Tuple2<>(tuple2._2, tuple2._1);
            }
        });

        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -7028608762710200957L;

            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });


    }

}

