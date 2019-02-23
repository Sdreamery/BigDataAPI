package com.seanxia.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

public class TopN {
    public static void main(String[] args){

        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("TopN");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lineRDD = sc.textFile("./data/scores.txt", 3);

        JavaPairRDD<String, Integer> pairRDD = lineRDD.mapToPair(
                new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 8784919217657399353L;

                    @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");
                String clazzName = split[0];
                Integer score = Integer.valueOf(split[1]);
                return new Tuple2<>(clazzName, score);
            }
        });

//        pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });


        JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = pairRDD.groupByKey();

        groupByKeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = -1191147232196335269L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                String clazzName = tuple2._1;
                Iterator<Integer> iterator = tuple2._2.iterator();
                System.out.println("===>" + tuple2);

                Integer[] top3 = new Integer[3];
                while (iterator.hasNext()){
                    Integer score = iterator.next();
                    for (int i = 0; i < top3.length; i++) {
                        if (top3[i] == null){
                            top3[i] = score;
                            break;
                        } else if (top3[i] < score){
                            for (int j = 2; j > i; j--) {
                                top3[j] = top3[j-1];
                            }
                            top3[i] = score;
                            break;
                        }
                    }
                }
                System.out.println("clazzName: " + clazzName);
                for (Integer sscore : top3) {
                    System.out.println(sscore);
                }
            }
        });

    }

}
