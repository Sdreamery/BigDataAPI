package com.sxt.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.deploy.master.Master;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;

public class lcs_PV {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local").setAppName("pv");
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD<String> lineRDD = context.textFile("./data/pvuvdata");




        /**
         * 每个页面 PV
         */

//        lineRDD.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String line) throws Exception {
//                String[] str = line.split("\t");
//
//
//                return new Tuple2<>(str[5],1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                    return v1 + v2;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

//        JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String line) throws Exception {
//                String[] str = line.split("\t");
//
//
//                return new Tuple2<>(str[5], 1);
//            }
//        }).groupByKey();
//
//        groupByKeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
//            @Override
//            public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
//
//
//                int count = 0;
//                Iterator<Integer> iterator = tuple2._2.iterator();
//                while(iterator.hasNext()){
//                    count++;
//                }
//                System.out.println("url : " + tuple2._1 + " value: " + count);
//            }
//        });

//        Map<String, Object> map = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String line) throws Exception {
//                String[] str = line.split("\t");
//
//                // url,1
//                return new Tuple2<>(str[5], 1);
//            }
//        }).countByKey();
//
//        for (String key :map.keySet()){
//            System.out.println("key : " + key  + "   value :" + map.get(key) );
//        }





    }
}
