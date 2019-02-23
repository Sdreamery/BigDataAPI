package com.seanxia.spark.java.core;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.deploy.master.Master;

import org.apache.spark.scheduler.DAGScheduler$;
import scala.Tuple2;

public class Lcs_PV {
    public static void main(String[] args){

        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("pv");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lineRDD = context.textFile("./data/pvuvdata");

        JavaPairRDD<String, Integer> pairRDD = lineRDD.mapToPair(
                new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -8915323203600188997L;

            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] split = line.split("\t");
                return new Tuple2<>(split[5], 1);
            }
        });

        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -4896754900945213805L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
//
//        reduceRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

        JavaPairRDD<Integer, String> reverseRDD = reduceRDD.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            private static final long serialVersionUID = 714560770900806480L;

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<>(tuple2._2, tuple2._1);
            }
        });

//        reverseRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
//            @Override
//            public void call(Tuple2<Integer, String> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

        // 按浏览量倒序排序
        JavaPairRDD<Integer, String> sortByKey = reverseRDD.sortByKey(false);

//        sortByKey.foreach(new VoidFunction<Tuple2<Integer, String>>() {
//            @Override
//            public void call(Tuple2<Integer, String> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

        JavaPairRDD<String, Integer> result = sortByKey.mapToPair(
                new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            private static final long serialVersionUID = -2760030620964758688L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return new Tuple2<>(tuple2._2, tuple2._1);
            }
        });

        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -8357829329304668102L;

            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(tuple2);
            }
        });


    }

}
