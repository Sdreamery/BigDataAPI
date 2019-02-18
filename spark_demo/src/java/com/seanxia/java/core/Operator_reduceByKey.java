package com.seanxia.java.core;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

/**
 * countByKey
 * 
 * 作用到K,V格式的RDD上，根据Key计数相同Key的数据集元素。返回一个Map<K,Object>
 * @author root
 *
 */
public class Operator_reduceByKey {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("countByKey");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaPairRDD<String, Integer> parallelizePairs = sc.parallelizePairs(Arrays.asList(
				new Tuple2<>("a",1),
                new Tuple2<>("a",1),
				new Tuple2<>("a",1),
				new Tuple2<>("a",1),
                new Tuple2<>("a",1),
                new Tuple2<>("a",1),
				new Tuple2<>("a",1)
		),2);

//        parallelizePairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                System.out.println("v1: " +v1 + " v2: " + v2);
//                return v1 + v2;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

//        JavaPairRDD<String, Integer> pairRDD = parallelizePairs.aggregateByKey(80, new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                System.out.println("map .....  v1: " + v1 + " v2: " + v2);
//                return v1 + v2;
//            }
//        }, new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                System.out.println("reudce .... v1: " + v1 + " v2: " + v2);
//                return v1 + v2;
//
//            }
//        });
////
//        pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple2) throws Exception {
//                System.out.println(tuple2);
//            }
//        });

////
        JavaPairRDD<String, Integer> pairRDD = parallelizePairs.combineByKey(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                System.out.println("---初始化-----" + v1);

                return v1 * 10 ;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("map.... v1: " + v1 + " v2: " + v2);
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("reduce..   v1: " + v1 + " v2: " + v2);
                return v1 + v2;
            }
        });
//
        pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });
////



    }
}
