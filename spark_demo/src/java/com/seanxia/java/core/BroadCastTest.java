package com.seanxia.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.deploy.master.Master;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BroadCastTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("broadcast");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final List<String> list = Arrays.asList("hello","bjsxt","shsxt");

        JavaRDD<String> lineRDD = sc.textFile("data/word.txt");


//
//        lineRDD.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterable<String> call(String s) throws Exception {
//                return Arrays.asList(s.split(" "));
//            }
//        }).filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String word) throws Exception {
//
//                return list.contains(word);
//            }
//        }).foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        //广播变量
        final Broadcast<List<String>> broadcast = sc.broadcast(list);

        lineRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String word) throws Exception {
                List<String> value = broadcast.value();
                return value.contains(word);
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


    }
}
